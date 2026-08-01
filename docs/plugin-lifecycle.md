# Plugin lifecycle: startup, shutdown, crash/restart, and hot-reload

A plugin instance's life has one beginning and four possible endings: a
graceful `Host.Stop`, a crash the restart policy retries, a crash the
restart policy gives up on, and a `Host.Reload` that replaces it with a
successor. This guide covers the mechanics of each — what actually happens
to the child process and to in-flight calls — complementing
[docs/supervisor-events.md](supervisor-events.md), which covers how these
same transitions surface on `Host.Events()`.

```
  spawn ──► starting ──► serving
                            │
              ┌─────────────┼──────────────────┐
              │             │                  │
         Host.Stop         crash            Host.Reload
              │             │                  │
              ▼             ▼                  ▼
            gone      restart or gave up   succeeds  → serving (successor)
                     (see "Crash and       rolls back → serving (same instance)
                      restart" below)      (see "Hot-reload" below)
```

## Startup

`Host.Start` spawns every configured `PluginSpec`: a private control
socketpair, a sanitized environment, then a handshake (protocol version,
features, service list, and — if `PluginSpec.BinarySHA256` is set — binary
identity) before any shared-memory region exists. Once negotiation settles
on the shared-memory transport, the host creates and seals that region and
the plugin attaches it; only then does the plugin report itself ready and
heartbeat monitoring begin.

On the plugin side, `Serve` calls `InstallDeathSignal` before anything
else: it arms `PR_SET_PDEATHSIG(SIGKILL)` so the kernel kills the process
if its parent dies, then independently re-checks its parent PID against
the one captured at process start. That second check exists because the
signal alone can't cover a parent that already died in the gap between
fork and the signal being armed — a plugin that finds itself reparented
exits immediately (`os.Exit(1)`) rather than run headless.

`Start` blocks per plugin only until that plugin's first attempt reaches
`Ready` or gives up; ongoing monitoring and restarts continue in the
background and are reported via `Events`. One plugin failing to start
doesn't abort the others — `Start`'s returned error is every failure
joined together (`errors.Join`), not the first one.

## Graceful shutdown — `Host.Stop`

`Host.Stop(ctx)` tears every plugin down. The one thing worth internalizing
up front: **`ctx` bounds how long `Stop` waits for that teardown to join
— it does not bound the teardown itself or give in-flight calls extra time
to finish.** A plugin's teardown runs to completion on its own goroutines
regardless of `ctx`; if `ctx` expires first, `Stop` returns your `ctx`'s
error early while that teardown keeps running detached (see
[Stop that doesn't join in time](#stop-that-doesnt-join-in-time) below).

Tearing an instance down — for a graceful `Stop`, a crash, hot-reload's
retired predecessor, or a poisoned region — is always the same six steps,
whichever caller triggered it:

1. **Stop admission** and detach routing, so a new call fails immediately
   rather than being handed to a dying instance.
2. **Fail every in-flight call, immediately** — not after waiting for them
   to finish. A call already admitted but not yet handed to the transport
   fails with the retryable `ErrPluginUnavailable`; a call whose request
   frame already reached the plugin fails with the non-retryable
   `ErrOutcomeUnknown`, because whether the plugin executed it before
   dying is unknowable. **This is the detail most likely to surprise
   someone porting a "graceful shutdown drains in-flight requests" mental
   model from another framework: `Host.Stop` does not wait for outstanding
   calls to complete.** There is no drain phase here — that behavior
   exists only for `Reload` (see below), which trades a live successor for
   the wait; a `Stop` has no successor to trade for, so nothing is gained
   by waiting.
3. **Join the goroutines** that still touch the transport mapping, bounded
   by an internal 5-second timeout so a wedged one can never block the
   next step forever. If that join times out, teardown proceeds anyway —
   this internal timeout is not surfaced on `Stop`'s return value or on
   `Events()`; it exists purely so a stuck goroutine can't wedge shutdown.
4. **Unmap** the region.
5. **Terminate the process**: send it a real `Shutdown` control message
   carrying a deadline, wait up to a fixed 5 seconds for it to exit on its
   own, then `SIGKILL` its entire process group (reaching any child
   processes the plugin itself spawned, not just its own PID) and
   `waitpid`-reap it. This step always runs, even if an earlier step
   panicked or timed out — a plugin is never left un-reaped.
6. **Close every local fd**, deliberately last so step 5's exchange still
   has a working control socket.

That fixed 5-second graceful window (and the goroutine-join bound in step
3) are internal constants today — neither is a `PluginSpec` or `HostConfig`
field yet.

### Stop that doesn't join in time

If a plugin's supervisor does not finish tearing down before `Stop`'s `ctx`
expires, `Stop` returns that plugin's deadline error but keeps its runtime
alive in the background: the name stays marked "stopping," so a concurrent
`Start` or `Reload` for that same name fails with `ErrPluginStopping`
rather than racing a second supervisor onto it. The retained runtime
finishes tearing down on its own — via a detached watcher once the
supervisor's `Run` actually exits, or the next time `Stop` is called and
retries it — and the name clears once that completes. There's nothing to
call to hurry this along beyond giving `Stop` a longer `ctx` (or
`context.Background()`) in the first place.

## Crash and restart

A crash — the process exiting, the control connection being lost, or a
handshake/attach failure — runs through the identical six-step teardown
above, then the restart policy decides whether to try again:

```
         serving
            │
   process exits, connection lost, or a
   heartbeat wedge persists past the wedge window
            │
            ▼
      EventCrashed
            │
    ┌───────┴────────────────┐
    │                        │
restarts remaining      budget exhausted, or
(Restart.Max)           handshake incompatible
    │                        │
    ▼                        ▼
EventRestarting          EventGaveUp (terminal —
    │                    Plugin(name) now fails
    ▼                    every call, indefinitely)
starting (spawn again)
```

See [docs/supervisor-events.md](supervisor-events.md#the-six-event-kinds) for
what fires on `Events()` and the sharp edge in the default
`RestartPolicy{}` (`Max: 0` gives up on the very first crash, with no
restart attempt at all).

## Hot-reload — `Host.Reload`

`Reload(ctx, name)` swaps a running instance for a freshly spawned
successor without restarting supervision or losing in-flight work. Unlike
`Stop`, it genuinely does wait — new calls are cut off, and the plugin
itself waits for every call it had already accepted to finish, before
anything is torn down. It runs as five ordered phases, and everything
through the fourth can still roll back to the original, still-serving
instance:

```
serving (old instance)
      │
      ▼
1 Cutoff → 2 Drain → 3 Snapshot → 4 Restore+validate → 5 Promote
   │          │           │              │                 │
   │          │           │              │                 ▼
   │          │           │              │        serving (new instance)
   │          │           │              │             EventReady
   └──────────┴───────────┴──────────────┘
                     │
          any phase 1-4 fails or times out
                     ▼
      rollback: resume old instance, reopen admission
        (old instance never stopped serving)
                     │
        old instance doesn't answer Resume
                     ▼
       EventCrashed → normal crash/restart path
```

1. **Cutoff** — closes admission and waits (bounded by the `DrainAck`
   deadline below) for every caller that was admitted just before the
   cutoff to finish publishing its request. Purely host-local; nothing has
   been frozen and no successor exists yet, so aborting here is trivial.
2. **Drain** — sends the plugin a `Drain` message. The plugin freezes every
   registered `Mutator` (see below) in registration order, waits for every
   call it accepted before cutoff to finish, and only then acknowledges.
3. **Snapshot** — the plugin calls its registered `StateSaver.SaveState`
   (or sends an empty payload if none is registered — every reload sends a
   snapshot, even a stateless one) and seals it into a `memfd`. The host
   independently re-verifies the seal, declared length, and checksum
   rather than trusting the plugin's claims.
4. **Restore and validate** — a fresh successor process is spawned and
   handed the verified snapshot; its registered `StateRestorer.RestoreState`
   applies it before the successor reports itself ready.
5. **Promote** — routing atomically swaps to the successor and admission
   reopens immediately. The host then waits until it has read every answer
   the predecessor already produced, and only then tears the predecessor
   down (through the same six-step teardown described above, which is why
   the predecessor's control loop sees a real `Shutdown` rather than a bare
   disconnect).

That wait in step 5 is what makes the reload's completion guarantee whole.
Step 2 gets the plugin's promise that every accepted call has been answered
onto the transport; step 5 makes the host collect those answers before it
destroys the calls waiting for them. Skipping it would report calls that
completed successfully as `ErrOutcomeUnknown` — the one error class
`IsRetryable` refuses to retry — precisely when the host is busiest, since a
loaded reader trails the plugin by the whole in-flight set.

**What this means for you as a caller:** a call the plugin accepted before
the cutoff runs to completion and you get its real outcome. A call refused
at the cutoff fails with `ErrDrained` and is safe to retry. `Reload` can
therefore take up to a second longer than the wire phases alone, and the
wait is not cancellable — cancelling your `Reload` context after the
promote cannot un-promote the successor, so honoring it could only throw
away answers the host is already holding.

The wait is bounded at one second, so a plugin whose responses somehow never
arrive cannot stall a reload. Calls still unanswered when that bound expires
are at risk of failing with `ErrOutcomeUnknown`, and that many are counted on
`styx.reload.dropped.count` (labeled with the plugin name). The counter is an
upper bound on what was actually lost, never an under-count: the connection's
reader keeps running through the first teardown steps and may still resolve
some of those calls. The counter should sit at zero; a non-zero value means a
reload put work at risk that a caller cannot safely retry, and is worth an
alert.

The three per-phase deadlines are fixed defaults today, not `PluginSpec`
fields: 30 seconds for drain, 10 seconds for the snapshot exchange, and 30
seconds for restore-and-validate. The step-5 response join has its own fixed
one-second bound, separate from all three.

### Registering a plugin's reload participation

A stateless plugin needs no registration at all — a stateless snapshot is
still exchanged, just empty. A plugin that needs to carry state or pause a
background component across reload registers on its `PluginServer`:

- **`RegisterMutator(m)`** for anything that mutates its own state on a
  schedule the reload can't otherwise see — a background flusher, a lease
  renewer, a reconnect loop, a cache evictor. `Freeze` must return only
  once the component has fully settled; register dependent mutators in
  dependency order (a cache that draws on a connection pool registers
  after that pool), since `Freeze` and the rollback path's `Resume` both
  run in that same registration order.
- **`RegisterStateSaver` / `RegisterStateRestorer`** as a matched pair —
  `SaveState` runs on the predecessor once it's already frozen and
  quiescent, `RestoreState` runs on the successor before it reports ready.
  See [`examples/hot-reload/plugin/main.go`](../examples/hot-reload/plugin/main.go)
  for a complete pair that carries a running counter across reload,
  validating its own format-version stamp on restore.

### What "hot-reload fails" actually means

There isn't one failure shape — `Reload`'s returned error means three
different things depending on which phase it came from, and one of them
is not really a failure at all:

- **A rollback before promotion (phases 1–4).** The old instance is
  resumed (mutators restarted, admission reopened) and never stopped
  serving. `Reload` returns the reason the transaction aborted — a timeout
  waiting for `DrainAck`, a rejected snapshot, a `StateRestorer` that
  refused the payload, and so on. No successor is left running and no
  event fires on `Events()`; the synchronous error is the complete
  picture. This is the common case — a slow or misbehaving mutator, a
  transient state-save error — and the plugin your callers were already
  talking to is exactly the one still serving them.
- **A rollback that can't complete.** If the old instance stopped
  answering while frozen (it crashed mid-reload), there's no live peer
  left to send `Resume` to. Admission stays closed and this plugin is
  handed to the ordinary crash/restart path instead — the same path a
  plain crash takes, subject to the same `RestartPolicy`. `Reload` returns
  an error here, **and** — because this funnels into the crash path — an
  `EventCrashed` (and then `EventRestarting`/`EventGaveUp` per policy)
  follows shortly after on `Events()`. This is the one case where a
  `Reload` failure and a `Events()` notification describe the same
  underlying fault.
- **A post-promote teardown fault.** Once phase 5 promotes the successor,
  there is nothing left to roll back — the new instance is already live
  and serving, and admission has already reopened. If tearing down the
  *retired predecessor* afterward hits an error, `Reload` still returns
  that error, even though the reload itself succeeded. **A non-nil
  `Reload` error does not always mean the reload failed** — `Events()`
  will have already reported the successor's `EventReady` in this case.
  Don't treat every `Reload` error as "the plugin is still on the old
  instance" without checking for that.

`Reload` itself distinguishes a handful of its own conditions with public
sentinels: `ErrPluginUnavailable` if the named plugin isn't running,
`ErrPluginStopping` if its prior instance is still tearing down from a
`Stop` that hasn't finished, and your `ctx`'s own error if it's done before
the transaction completes. Anything else is wrapped with the phase detail
described above.

## Further reading

- [docs/supervisor-events.md](supervisor-events.md) — what each transition
  in this document looks like on `Host.Events()`,
  how to check the same retained state synchronously with `Host.Health`
  instead of subscribing,
  and what's worth building alerting around.
- [docs/migration-from-go-plugin.md](migration-from-go-plugin.md#lifecycle-liveness-shutdown-and-kill) —
  how this compares to `hashicorp/go-plugin`'s `Kill()`/reattach model.
- [`examples/hot-reload/`](../examples/hot-reload/) — a runnable host and
  plugin pair exercising a full reload with state carried across it.
