# `arloliu/go-plugin` fork deltas vs. `hashicorp/go-plugin`

Research investigating every commit unique to the
`arloliu/go-plugin` fork between its divergence point and `v1.9.0` (the
version eqp-hub pins), cross-referenced against eqp-hub's actual usage, and
turned into explicit requirements (or explicit non-requirements) for Styx's
public API and the surrounding framework implementation work.

## Summary

The fork forked from `hashicorp/go-plugin` at commit `96d18ee` (18 commits
past upstream's `v1.7.0` tag, an ancestor of upstream's `v1.8.0`) and diverged
into 34 fork-only commits, tagged as its own `v1.8.0`, `v1.8.1`, and `v1.9.0`
(eqp-hub's pinned version, confirmed identical to fork `HEAD`). The fork is a
single-maintainer hardening pass over go-plugin's gRPC transport for a
fleet-of-long-running-daemons use case (eqp-hub's device gateway), not a
feature fork: roughly two-thirds of the commits are bug fixes for resource
leaks, hangs, and host-crashing panics that only matter when a host runs many
plugins for a long time and cannot tolerate one wedged/misbehaving plugin
taking the host down; the rest are new bounded-timeout knobs on
`ClientConfig`, a module rename, and repo hygiene (lint, docs, Makefile).
Concretely, eqp-hub's own code exercises exactly two of the fork's additive
`ClientConfig` fields (`PingTimeout`, `ShutdownTimeout`) and depends
pervasively on `Ping()`/`Kill()` semantics; the fork's TLS-conflict rejection,
reattach hardening, and gRPC dependency bump are not exercised by eqp-hub at
all because Styx's design (same-host, same-user, UDS/SHM transport, no
reattach-to-existing-process concept) makes the underlying go-plugin
mechanisms these fixes patch moot. Cross-referencing against
`docs/specs/2026-07-16-styx-design.md` and
`docs/plans/2026-07-16-m1-framework-uds.md` shows the spec already covers
the pain points that matter (bounded heartbeat/ping, bounded shutdown grace,
non-reorderable teardown with SIGKILL fallback and forced reap, panic
isolation of observability hooks, structured logging with no vendor deps,
error-taxonomy retryability) — this report's main value is confirming that
coverage commit-by-commit and flagging two real gaps: process-group-wide
kill for plugins that themselves fork subprocesses, and an explicit
"malformed/adversarial data from a plugin must not panic the host" test
requirement that the design document states as a principle but the
lifecycle/teardown and control-plane implementation work don't yet
call out as an explicit test obligation.

## Baseline determination

`arloliu/go-plugin`'s GitHub fork-parent metadata is empty, so the baseline
was established directly from git history:

1. Cloned both repos into the scratch dir, added `upstream` as a remote of
   `fork`, and fetched.
2. `git tag` comparison: fork and upstream share identical tag→commit
   mappings for every tag from `v1.0.0` through `v1.7.0` (verified `v1.0.0`,
   `v1.5.0`, `v1.7.0` by SHA — all three match byte-for-byte between the two
   remotes; `git fetch upstream --tags` on the fork clone only rejects
   `v1.8.0` as "would clobber," meaning every other overlapping tag name was
   already identical). From `v1.8.0` onward the fork's tags point to fork-only
   commits — the fork continued its own `v1.8.0`/`v1.8.1`/`v1.9.0` releases
   independently of upstream's own (different-content) `v1.8.0`.
3. `git merge-base HEAD upstream/main` → `96d18ee73579514cd44c3426890f4f97dc6ae8f0`
   ("`[chore]`: Bump actions/upload-artifact ... (#376)", 2026-04-13).
   `git merge-base --is-ancestor 96d18ee upstream/v1.8.0` → true, and
   `git describe --tags 96d18ee` → `v1.7.0-18-g96d18ee`: the fork branched 18
   commits after upstream's `v1.7.0` tag, at a point still 14 commits before
   upstream cut its own `v1.8.0`. `git rev-list --count 96d18ee..HEAD` = 34
   (the fork's unique commits); `git rev-list --count 96d18ee..upstream/main`
   = 14 (upstream commits the fork never merged forward — not reviewed here,
   out of scope per the brief: this report catalogs fork deltas, not
   upstream's independent evolution).
4. `git rev-parse HEAD` on the fork clone = `ff4304ec05ba17cb4fa9b3898b3e5188852ebfd0`
   = `git rev-parse v1.9.0` on the same clone — confirms fork `HEAD` at
   clone time is exactly the `v1.9.0` release eqp-hub's `go.mod` pins.

**Baseline: commit `96d18ee73579514cd44c3426890f4f97dc6ae8f0`
(`hashicorp/go-plugin`, 18 commits past its `v1.7.0` tag). Target:
`ff4304ec05ba17cb4fa9b3898b3e5188852ebfd0` (`arloliu/go-plugin` `v1.9.0`).
34 commits in between, all fork-authored, none upstream-cherry-picked.**

## Commit-by-commit

Ordered oldest → newest (`git log --reverse 96d18ee..v1.9.0`). Each fork
commit's own message is detailed (single-maintainer fork, no linked PRs for
these 34 — `git log ... | grep -io '#[0-9]\+'` over this range returns
nothing, unlike the upstream-inherited pre-fork history which does reference
PRs); "why" below is drawn from those messages plus the CHANGELOG.md the fork
maintains.

1. **`96cd083` — plugin: stop panicking the host on malformed plugin
   output.** `copyStream` panicked on a nil reader/writer; `parseJSON` did
   unchecked type assertions on hclog-reserved JSON keys, so a plugin
   emitting a non-string `@timestamp`/`@level`/`@message` crashed the
   stderr log pump and took the host down. **Bug fix** (host-crash from
   plugin-controlled input).
2. **`82f4a38` — plugin: bound `Ping()` with a timeout.** Both `RPCClient`
   and `GRPCClient` `Ping()` had no deadline; a wedged plugin hung the
   caller forever. Adds `defaultPingTimeout` (10s var) and, for net/rpc,
   dispatches via `rpc.Go` + a timer select since net/rpc has no native
   cancellation; adds `ErrPingTimeout`. **Bug fix** (unbounded hang).
3. **`d3a0755` — plugin: recover panics in stderr log pump and stdout
   scanner goroutines.** No panic recovery on the two client-side
   stream-processing goroutines; a panicking `io.Writer` (e.g. a
   user-supplied `ClientConfig.Stderr`) took the host down. **Bug fix.**
4. **`8bf442e` — cmdrunner: kill the plugin's whole process group on
   POSIX.** `CmdRunner.Kill` only signaled the plugin PID; any subprocess
   the plugin itself forked (ssh, socat, external drivers) was orphaned to
   PID 1. Sets `Setpgid` at spawn (skipped when `Setsid` is already set,
   since a session leader can't `setpgid` itself) and sends SIGKILL to
   `-pgid`. Windows unchanged (single-PID). **New capability /
   behavior change** (broader kill blast radius, intentional).
5. **`2425a45` — plugin: persist `getGRPCMuxer` init error across
   calls.** `sync.Once` ran the closure once; the captured error was a
   local, so every subsequent caller silently saw `(nil, nil)` even after
   an initial failure, silently falling back to non-muxed mode for a user
   who had explicitly requested `GRPCBrokerMultiplex`. Stores the error on
   the `Client` struct. **Bug fix** (correctness/silent fallback).
6. **`593e46c` — plugin: bound `GRPCClient.Close`'s Shutdown RPC with a
   timeout.** Used `c.doneCtx` (only cancelled at process exit); a
   wedged-but-alive plugin hung `Close()`, which hangs `Client.Kill`.
   Switches to a fresh bounded context (`defaultPingTimeout`). **Bug
   fix.**
7. **`45a9033` — plugin: prefer `GracefulStop` over `Stop` in the gRPC
   controller's Shutdown handler.** `Stop()` cut every in-flight RPC
   immediately; for equipment-command RPCs this truncated in-flight
   responses even during a clean shutdown. `GracefulStop` dispatched in a
   goroutine (synchronous call would deadlock, since `GracefulStop` waits
   for the handler that's calling it), with a bounded fallback to `Stop`
   after `gracefulStopTimeout` (100ms default, sized to fit inside
   `Client.Kill`'s 2s grace window). **Behavior change** (drain semantics
   on shutdown).
8. **`4ed59dc` — plugin: remove managed clients from the global slice on
   `Kill`.** `managedClients` grew unboundedly across the life of a host
   that creates/kills many `Managed` clients — a slow memory leak at
   fleet scale. **Bug fix** (resource leak).
9. **`9883393` — plugin: integration test for managed-client Kill cleanup;
   documents a `Ping()` usage hazard.** Test-only, plus a doc note: on
   `ErrPingTimeout` the background `rpc.Go` goroutine only unblocks when
   the transport closes (normally via `Kill`) — a caller that retries
   `Ping` without killing accumulates goroutines. **Test coverage +
   documentation**, no behavior change.
10. **`ed7ada5` — plugin: `ClientConfig.DisableProcessGroupKill` opt-out.**
    The process-group kill from commit 4 (`8bf442e`) can move the plugin
    out of the terminal's foreground process group, causing TTY-interactive
    children (e.g. one that `exec`s a pager or `ssh` reading `/dev/tty`) to
    get `SIGTTIN` and stall. Adds an opt-out flag threaded to
    `CmdRunner.SetDisableProcessGroup`; Windows ignores it (already
    single-PID). **New capability** (escape hatch for a behavior change).
11. **`34e835e` — plugin: `ClientConfig.PingTimeout`.** Promotes the
    package-level `defaultPingTimeout` (commit 2) to a per-`ClientConfig`
    field so different plugins can have different health-probe budgets;
    zero retains the old default. **New capability.**
12. **`e2ad8cc` — plugin: `ClientConfig.ShutdownTimeout` (Kill grace
    window).** The 2s grace window in `Client.Kill` was hard-coded; for
    plugins doing slow I/O at shutdown (flush a command queue, close a
    serial port) 2s is often too short and the plugin is force-killed
    mid-flush. Additive field, zero retains the old 2s default. **New
    capability.**
13. **`fc3913e` — plugin: `BrokerTimeout` var for broker
    accept/dial/knock.** The 5s broker timeouts in `mux_broker.go`,
    `grpc_broker.go`, and the internal grpcmux session-init path were
    hard-coded; a loaded host under GC/scheduler pressure could plausibly
    exceed 5s and get an opaque error with no way to widen the budget.
    Collapses them into one exported `plugin.BrokerTimeout` var. **New
    capability.**
14. **`4b91d64` — plugin: route library-internal `log.Printf` through
    hclog.** Broker/RPCServer/stream-copy diagnostics went through stdlib
    `log.Printf` (stderr-only, unstructured, ad-hoc prefix); the host's
    hclog stderr pump re-ingested and misclassified it as Debug. Adds
    `SetInternalLogger(hclog.Logger)`. **Behavior change / new capability**
    (structured internal logging, opt-in override).
15. **`75cbe3d` — test: fill coverage gaps for the 14 fork commits landed
    so far.** Test-only, except one supporting refactor:
    `gracefulStopTimeout` moves to an atomic-backed getter/setter so tests
    can mutate it under `-race`. **Test coverage**, no user-facing
    behavior change.
16. **`599169a` — test: tighten the `ShutdownTimeout` regression bracket.**
    Narrows a `< 2s` upper-bound assertion to `[100ms, 1s]` so a
    regression to a hardcoded 500ms, or a regression that force-kills
    immediately, are both caught. **Test coverage only.**
17. **`3a4f527` — build: add a root Makefile mirroring CI gates.** Repo
    tooling (`make test`/`lint`/`fmt-check`/`vet`/`build`/`cover`/`check`).
    **Repo hygiene, no library behavior change.**
18. **`150f58f` — chore: rename module path `hashicorp` →
    `arloliu`.** Mechanical rename across `go.mod`, docs, examples.
    **Fork-identity housekeeping, no behavior change.**
19. **`cdc4564` — build: `update-pkg-cache` Makefile target.** Repo
    tooling. **No behavior change.**
20. **`71af8dc` — docs: `v1.8.0` CHANGELOG entry.** Docs only.
21. **`22159a2` — build: missing `LATEST_GIT_TAG` Makefile variable.**
    Repo tooling fix. **No behavior change.**
22. **`851bb8e` — chore: `make fmt` and `go fix`.** Mechanical formatting.
    **No behavior change.**
23. **`81ac83c` — fix: comment typos** (`suppressed`, `Process`,
    `accommodate`). **Docs only, no code behavior change.**
24. **`ad93661` — chore: add golangci-lint v2 config.** Repo tooling.
    **No behavior change.**
25. **`7de0cca` — docs: rewrite `CLAUDE.md`/`AGENTS.md`/`.agents/` for
    go-plugin.** Agent-facing repo docs. **No behavior change.**
26. **`c178ec2` — refactor: address lint findings for stricter checks**
    (errorlint, perfsprint, dupword, whitespace, unconvert, thelper,
    nakedret). **Mechanical refactor, no behavior change** (explicit intent
    of the commit).
27. **`b6963bc` — refactor: rename underscore/`Id`-style identifiers to Go
    conventions** (`nextId`→`nextID`, etc.), scoped to unexported symbols
    and test helpers; public `MuxBroker.NextId`/`GRPCBroker.NextId` left
    intact. **Mechanical refactor, no public API change.**
28. **`6bc7022` — chore(lint): enable additional golangci-lint checks.**
    Repo tooling/config. **No behavior change.**
29. **`f7c8fa4` — fix: harden `Client.Kill` concurrency, stdio panic,
    reattach version, broker timeout errors.** Four fixes bundled: (a)
    `Client.Kill` gated by `sync.Once`, runs its body at most once even
    under racing callers; (b) `grpcStdioClient.Run` recovers panics from
    host-provided `SyncStdout`/`SyncStderr` writers; (c) non-test reattach
    adopts `ReattachConfig.ProtocolVersion` as the negotiated version
    (previously `Dispense` ran at protocol version 0 after reattach); (d)
    new exported `ErrBrokerTimeout` sentinel, wrapped via `%w` from
    mux/grpc broker accept/dial/knock timeouts. **Bug fixes + new
    capability** (the sentinel).
30. **`36c5747` — test: pin the AutoMTLS-vs-TLSProvider failure mode.**
    Regression test asserting the *existing* (bad) failure mode — a late,
    generic TLS certificate error — ahead of fixing it in the next commit.
    **Test coverage only** (documents the bug before the fix).
31. **`bb529ad` — fix(server): reject AutoMTLS + TLSProvider with a clear
    error.** Previously the combination silently took the `TLSProvider`
    branch, never echoed a server cert, and failed late at first RPC with
    a generic x509 error. `Serve` now detects the conflict before
    listening and exits with a specific stderr message. **Bug fix**
    (error-quality / fail-fast).
32. **`9e68e32` — fix: reattach validation, broker stream reap,
    error-format polish.** (a) `Client.Start` validates `ReattachConfig`
    up front (clear error instead of a panic reachable via `os.FindProcess(0)`
    on a nil `Addr`); (b) version-mismatch error formats the client
    version list with `%v` instead of `%d`; (c) `GRPCBroker.Dial` deletes
    its pending `clientStreams` entry when `BrokerTimeout` fires before a
    `ConnInfo` arrives (previously only reaped by a goroutine that never
    ran on this path — a map leak for long-running hosts); `knock` gets
    the same fix. **Bug fixes** (map leak + panic-avoidance + error
    clarity).
33. **`84571e7` — fix: reattach plumbing, Kill race, broker overflow
    handling.** (a) tightens `ReattachConfig` validation (`Addr` always
    required, `Pid` required when `ReattachFunc` unset — prevents reaching
    `os.FindProcess(0)`, which silently succeeds on Unix and can target
    the caller's own process group on `Kill`); (b) `ReattachConfig()`
    round-trips the negotiated `ProtocolVersion`, and the reattach path
    installs `VersionedPlugins[negVer]` into `config.Plugins` mirroring
    normal `Start` (previously `Dispense` failed with "unknown plugin
    type" on a reattach client configured with `VersionedPlugins` only);
    (c) `Kill`'s `sync.Once` gate is no longer consumed when there is no
    runner, so a `Kill` racing ahead of the first `Start` doesn't
    permanently disable later cleanup; (d) `MuxBroker.Run` closes the
    extra yamux stream on a duplicate-id-with-full-slot instead of
    silently abandoning it; spawns `timeoutWait` only when a message was
    actually queued; `MuxBroker.timeoutWait`'s cleanup branch no longer
    blocks forever when the pending stream was already drained by a
    racing `Accept`; overflow drops now log a `Warn` line with the
    service id; (e) `GRPCServer.Stop`/`GracefulStop` serialize broker
    close through an internal mutex (the Shutdown controller RPC could
    invoke both concurrently, double-closing the broker and racing on
    `s.broker = nil`). **Bug fixes** (reattach correctness, kill-race,
    broker robustness).
34. **`ff4304e` — deps: bump grpc to v1.80, migrate to `grpc.NewClient`,
    drop `golang/protobuf`.** Upgrades `google.golang.org/grpc`
    v1.61.0→v1.80.0, `google.golang.org/protobuf` v1.36.6→v1.36.11;
    regenerates `.pb.go` sources; drops the legacy `UnsafeEnabled` dual
    path and the deprecated `golang/protobuf` import; migrates
    `dialGRPCConn` from `grpc.Dial` to `grpc.NewClient` with the
    passthrough resolver (avoids DNS lookup on the "unused" dial target)
    and an explicit `conn.Connect()` call to preserve prior dial-timing
    behavior under `grpc.NewClient`'s lazy-connect semantics. **Dependency
    upgrade / behavior-preserving migration**, no exposed API change.

All 34 fork-unique commits between the baseline and `v1.9.0` are accounted
for above; none excluded.

## Requirements for Styx

Each bullet names where Styx's design/plan already covers the underlying
pain point, or flags a gap. Bullets marked **HARD REQUIREMENT** are backed by
confirmed eqp-hub usage (grep evidence below); the rest are hardening
lessons the fork learned the hard way that Styx's spec already anticipates,
listed to confirm the coverage rather than to introduce something new.

- **HARD REQUIREMENT — bounded per-plugin health check with a configurable
  timeout** (fork commits 2, 11: `Ping()` bound + `ClientConfig.PingTimeout`).
  eqp-hub sets `PingTimeout: 5 * time.Second` on `goplugin.ClientConfig`
  (`internal/device/plugin/plugin.go:133`) and calls `host.Ping()`
  pervasively in its test suite to assert liveness/deadness
  (`tests/devplugin/devplugin_test.go`, `internal/device/plugin/plugin_test.go`).
  Covered by the design document's heartbeat model (the supervisor work's `HealthConfig` —
  `HeartbeatInterval`/`MissedHeartbeats`/`WedgeWindow`, defaults 1s/3/5s) —
  Styx's heartbeat is stronger than go-plugin's `Ping()` (a progress
  contract distinguishing transport-wedged/dispatch-wedged/overloaded, not
  a bare liveness RPC), so this is more than satisfied, not just matched.
- **HARD REQUIREMENT — bounded graceful-shutdown grace window before force
  kill** (fork commits 7, 12: `GracefulStop` preference + `ClientConfig.ShutdownTimeout`).
  eqp-hub sets `ShutdownTimeout: 10 * time.Second` on `goplugin.ClientConfig`
  (`internal/device/plugin/plugin.go:134`). Covered by the design document's
  normative teardown step 5 ("graceful `Shutdown` with deadline → `SIGKILL`
  fallback → `waitpid` reap, always") and the lifecycle/teardown work's
  `Teardown.ShutdownDeadline` field —
  the deadline is a per-teardown-run parameter, matching the fork's
  per-`ClientConfig` granularity.
- **HARD REQUIREMENT — `Kill()`/process-termination as a primitive the host
  calls freely and repeatedly.** eqp-hub calls `client.Kill()` in test
  teardown paths, `defer`s, and error paths throughout
  (`internal/device/plugin/plugin.go:214` and 20+ call sites across its
  test suite). Styx does not expose an equivalent public `Kill()` primitive
  callable by arbitrary user code with the same concurrency hazard the fork
  patched in commits 29/33 (`sync.Once`-gated `Kill`, race with `Start`) —
  in Styx's design, process termination is internal (`internal/lifecycle.Teardown.Run`),
  driven solely by the supervisor or `Host.Stop`, never
  exposed as a user-callable concurrent-kill primitive. This is Styx's
  architecture *avoiding* the class of bug rather than needing the same
  fix — noted here so the public API design does not accidentally expose
  a bare `Kill()` on `ClientConn`/`Host` without the same idempotency
  guarantee the fork needed.
- **Host must never crash from plugin-controlled/malformed data** (fork
  commits 1, 3: panic recovery in stream-copy/log-pump goroutines and
  `parseJSON`'s type-assertion hardening). The design document states the
  principle ("a compromised plugin must not be able to crash the host through
  the protocol") and requires stdout/stderr capture goroutines with
  bounded buffers, but neither the design document nor the control-plane,
  lifecycle/teardown, and supervisor implementation work's step
  lists currently call out an explicit test obligation of the form "feed
  the control/log-capture path adversarial or malformed bytes and assert
  the host does not panic/crash." **Gap** — recommend the control-plane work
  (`internal/control`) and the supervisor work (`capture.go`'s stdout/stderr capture)
  each get an explicit fuzz/malformed-input regression test asserting no
  panic, mirroring fork commits 1 and 3, before that work is considered
  done.
- **Process-group-wide kill for plugins that themselves fork
  subprocesses** (fork commits 4, 10: `Setpgid` + `SIGKILL -pgid` on
  POSIX, with a `DisableProcessGroupKill` opt-out for TTY hosts). Not
  currently exercised by eqp-hub's own device-plugin binaries (grepped
  `devices/*` for `exec.Command`/`exec.CommandContext` at runtime — none
  found; the only `exec.Command` call sites in eqp-hub are build/test
  tooling, not the device plugins themselves), so this is not a
  confirmed hard requirement today. It is a real gap against the
  lifecycle/teardown work's current design, though: `internal/lifecycle.Spawn` does not set
  `Setpgid`, and `Teardown`'s step-5 `SIGKILL` fallback targets the single
  spawned PID, not a process group — so a future device plugin that
  shells out to an equipment-specific helper (ssh, a vendor CLI, a serial
  bridge) would orphan it exactly as upstream go-plugin did before this
  fork existed. **Open question** (not resolved by this report): should
  the lifecycle/teardown work add `Setpgid`-at-spawn and group-wide `SIGKILL` now, speculatively,
  or wait until a concrete device plugin needs subprocess-spawning and add
  it then? Resolving this needs either (a) a decision from whoever owns
  the framework implementation plan on whether to fold it in now, or (b) confirmation
  from eqp-hub's device-plugin roadmap that no planned device driver will
  ever shell out to a helper process.
- **Structured, vendor-agnostic internal logging, panic-isolated
  observability** (fork commit 14: `SetInternalLogger(hclog.Logger)`
  routing internal `log.Printf` sites through the host's structured
  logger). Covered by the design document's `observe` package (`Logger`
  interface, no vendored stacks) and its requirement that "Observability hooks
  run on non-hot-path goroutines, panic-isolated" — Styx's design
  makes structured logging the only path from the start (no legacy
  `log.Printf` call sites to migrate away from), so this is architecturally
  subsumed rather than needing a discrete fix.
- **Bounded internal control-plane operation timeouts** (fork commits 13,
  29's `ErrBrokerTimeout`: exported `BrokerTimeout` var bounding broker
  accept/dial/knock, with a distinguishable sentinel error). Covered by
  the control-plane work's `internal/control.ReplyDeadlines` (per-message-type
  reply deadline map) — Styx's control protocol already gives every
  message type its own bounded deadline by construction, a stronger
  guarantee than the fork's single `BrokerTimeout` knob covering three
  call sites.
- **Resource-leak-free client/process bookkeeping across restarts** (fork
  commits 5, 8, 32's map-leak fix: `getGRPCMuxer` error persistence,
  `managedClients` slice pruning on `Kill`, `GRPCBroker`'s `clientStreams`
  map reaped on timeout). No equivalent global/package-level mutable
  registry exists in Styx's design (no `managedClients`-style slice, no
  package-level broker singleton) — each `Host` owns its own supervisor
  state and each plugin's `Teardown` is scoped to that
  one plugin instance, so this class of leak has no direct analog to
  reproduce. Recommend the lifecycle/teardown and supervisor work's
  fd-leak-across-restart tests
  (already planned: "ownership tables are asserted in tests (no fd leaks
  across restart, verified by counting)") be read as covering the
  same lesson for Styx's actual stateful structures (request table,
  supervisor's per-plugin state) — no gap, just noting the lesson is
  already generalized in the design document's test-strategy language rather than
  needing a fork-shaped fix.
- **Fail-fast, specific error messages for host/plugin
  misconfiguration** (fork commits 30, 31: AutoMTLS+TLSProvider conflict
  detected before listening, not left to fail late and generically at
  first RPC). The design document's error taxonomy (`ErrIncompatible` at handshake)
  and the handshake negotiation work already fail fast and specifically
  at handshake time for version/feature mismatches — the general
  principle (configuration conflicts must surface at Start, not at first
  RPC) is already Styx's design default via the handshake phase; no gap.

## Non-requirements

Fork changes that do not apply to Styx's design, with the reason each is
moot:

- **AutoMTLS / TLSProvider TLS mutual-auth machinery** (fork commits 30,
  31, and the underlying feature they patch). go-plugin's gRPC transport
  can run over a network-capable listener and its TLS story exists to
  authenticate that link. Styx's trust model is explicit:
  host and plugins run as the same user on the same machine, communicating
  over a private UDS socketpair and anonymous memfd regions never exposed
  on a filesystem or network — there is no networked link for a TLS
  handshake to secure. Moot by transport model, not merely unused.
- **Reattach-to-an-already-running-plugin-process** (fork commits 29, 32,
  33's `ReattachConfig` fixes: `Addr`/`Pid` validation, `ProtocolVersion`
  round-trip, `VersionedPlugins` installation on reattach). Styx's design
  has no reattach concept at all — grepped the design document and the
  framework implementation plan for
  "reattach": zero matches. Every plugin instance is spawned fresh by
  `internal/lifecycle.Spawn`; hot-reload replaces a running
  instance transactionally rather than the host detaching/reattaching to
  a surviving child across host restarts. Moot by design, not a feature
  gap.
- **`grpc.Dial`→`grpc.NewClient` migration, grpc-go v1.61→v1.80 bump,
  `golang/protobuf` removal** (fork commit 34). This is upstream gRPC-go's
  own API evolution, relevant only because go-plugin's transport *is*
  gRPC. Styx's RPC runtime (`internal/rpcruntime`) is a
  custom dispatcher over descriptor rings / framed UDS, not grpc-go —
  there is no `grpc.Dial` call site to migrate and no grpc-go dependency
  version to track for this reason. (Styx does depend on
  `google.golang.org/protobuf` for the codec, per the design document, but that's an
  unrelated, already-current dependency choice, not an inherited fork
  concern.)
- **Module path rename `hashicorp`→`arloliu`** (fork commit 18) and its
  CHANGELOG/README mechanical follow-through (commits 20, 71af8dc's
  changelog subset). Pure fork-identity housekeeping; Styx is a fresh
  module (`github.com/arloliu/styx`, org TBD — see the design document's
  open-questions section)
  with no rename to perform.
- **Repo tooling: Makefile targets, golangci-lint v2 config, additional
  lint-rule enablement, mechanical lint/fmt refactors, comment typo
  fixes, agent-doc rewrites** (fork commits 17, 19, 21, 22, 23, 24, 25,
  26, 27, 28). These are the fork maintainer bringing an *inherited*
  upstream codebase up to their own tooling/style bar. Styx is written
  fresh against its own `.agents/rules/` from day one (already in place
  per `AGENTS.md`/`.agents/rules/200-coding-style.md` and friends) — there
  is no legacy naming, no un-configured linter, and no pre-existing
  Makefile gap to retrofit. (Styx's `AGENTS.md` already notes the repo
  predates that tooling work's own Makefile addition; that's an independent, already-known
  item, not something this fork comparison surfaces.)
- **Test-only commits validating the above** (fork commits 9, 15, 16, 30 —
  `9883393`, `75cbe3d`, `599169a`, `36c5747`). These add regression
  coverage for fixes already listed under Requirements or Non-requirements
  above; no independent requirement beyond "the corresponding fix/feature
  needs a test," which Styx's TDD-first workflow (`.agents/rules/` testing
  conventions, and every task's plan in the framework implementation work
  already writing the failing test
  first) already guarantees structurally.

## Self-check against the brief's gate

- Every fork commit (34 total, `96d18ee..ff4304e`/`v1.9.0`) is accounted
  for: all 34 appear in Commit-by-commit with a one-line what/why/
  classification; the ones that don't generate a Styx requirement are
  cross-referenced into Non-requirements with a named reason (transport
  model, no reattach concept, different RPC stack, fresh module/tooling,
  or "validates an already-listed fix").
- Every eqp-hub-used fork API appears in Requirements for Styx: confirmed
  via `grep -rn "arloliu/go-plugin\|goplugin\.\|ShutdownTimeout\|PingTimeout\|DisableProcessGroupKill\|SetInternalLogger\|ErrPingTimeout\|ErrBrokerTimeout" /home/arlo/projects/eqp-hub`.
  eqp-hub uses: the import itself (`internal/device/plugin/{server,device,plugin}.go`,
  aliased `goplugin` in `plugin.go`/tests, unaliased `plugin` in
  `server.go`/`device.go`), `goplugin.NewClient`/`ClientConfig`
  (`HandshakeConfig`, `Plugins`, `Cmd`, `AllowedProtocols`, `Logger`, and
  the two fork-only fields `PingTimeout`/`ShutdownTimeout`), `Client.Kill`,
  `Client.Ping` (via its own `ClientProtocol` wrapper interface), `GRPCPlugin`/
  `NetRPCUnsupportedPlugin`, `GRPCBroker`, `Serve`/`ServeConfig`. Of the
  fork-specific (not upstream-inherited) surface, only `PingTimeout` and
  `ShutdownTimeout` are directly set by eqp-hub — both appear as **HARD
  REQUIREMENT** bullets above with file:line evidence.
  `DisableProcessGroupKill`, `SetInternalLogger`, `ErrPingTimeout`,
  `ErrBrokerTimeout`, `BrokerTimeout`, `AutoMTLS`, `TLSConfig`,
  `TLSProvider`, `Reattach*`, `Managed` all returned zero hits in eqp-hub —
  confirmed unused, so correctly excluded from "hard requirement" status
  and instead classified as hardening lessons (already covered) or
  non-requirements (moot by design) above.
  Separately: the `Init`/`Start`/`Stop`/`HotReload`/`SaveRuntimeState`/
  `CollectMetrics`/`Ping` "lifecycle contract" the task brief asks about is
  **eqp-hub's own gRPC service** (`internal/device/plugin/pb`, a protobuf
  service riding *over* the go-plugin-negotiated connection), not a
  go-plugin fork API — go-plugin (fork or upstream) has no opinion on
  those method names. That contract is already captured as an ordinary
  user-defined service requirement in the design document's opening
  description of service definitions ("Styx
  must support that contract as an ordinary user-defined service") and
  isn't part of this fork-delta analysis; noted here so the two are not
  conflated.
- No "TBD"/"investigate further" placeholders: the one genuinely
  unresolved item (process-group-wide kill, fork commits 4/10) is stated
  as an explicit open question in Requirements for Styx, naming exactly
  what would resolve it — a decision to fold `Setpgid`/group-kill into
  the lifecycle/teardown work speculatively, or confirmation from eqp-hub's device-plugin
  roadmap that no planned driver will ever shell out to a subprocess.

## Open questions

- Should `internal/lifecycle.Spawn` set `Setpgid` and have
  `Teardown`'s SIGKILL fallback target the process group rather than the
  single PID, given the fork's `8bf442e`/`ed7ada5` precedent, even though
  no current eqp-hub device plugin forks subprocesses at runtime? Resolves
  via either an explicit decision when that work is implemented, or a check
  of eqp-hub's device-plugin roadmap/backlog for any planned driver that
  shells out to a helper process (e.g. a serial-bridge or vendor CLI
  wrapper).
