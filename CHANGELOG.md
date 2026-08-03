# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-08-03

Observability and lifecycle-reporting work: a host can now say which plugin
a failure came from, order the transitions it observes, account for what it
drops, and size its own shared memory. Plus the teardown and crash-reporting
fixes that writing those answers surfaced.

### Added

- **`Event.Revision`** and **`HealthSnapshot.Revision`**: a dense, per-plugin
  position in that plugin's transition history, taken from the same retained
  record both views already read. An event that advanced the record carries the
  position it advanced to; one the record discarded as superseded carries 0 and
  is still delivered. A consumer can now seed a view from `Health` and fold
  `Events()` with one comparison — ignore anything whose `Revision` is not
  strictly greater than what it has already applied — instead of reconstructing
  an order from `Kind` and `Time`, which cannot work: the bus delivers a
  critical `EventCrashed` ahead of an informational `EventStarting` published
  before it, and two events in the same clock tick carry equal `Time`. The
  numbering is dense where it is assigned but not where it is delivered, so a
  gap between two accepted revisions means transitions were dropped, a
  lower-numbered one is still in flight behind the event that overtook it, or
  both — a prompt to re-read `Health`, not a count of what was lost. Nothing
  reports how many events `Events()` dropped. Revisions count one plugin's
  transitions, so they are comparable only within a single `Event.Plugin`;
  `MissedHeartbeats` is written by the heartbeat path, records no transition,
  and is deliberately outside the numbering. Adding a field is source-compatible
  for every consumer that does not build a `styx.Event` or `styx.HealthSnapshot`
  by positional composite literal.
- **`observe.MetricStdioDropped`** (`styx.stdio.dropped.count`): counts
  stdout/stderr lines a plugin's stdio capture discarded because a
  `PluginSpec.Stdio` sink fell behind, labelled by plugin and by stream.
  Reported as a per-interval delta rather than one event per dropped line,
  so a plugin spraying output cannot turn this counter into its own flood,
  with a final delta when the instance ends so the interval a crash or a
  shutdown cuts short is reported too — the interval a crash flood lands
  in, and one nothing later could account for, since a restarted plugin's
  counts start from its own capture's zero.
- **`observe.MetricObserveDropped`** (`styx.observe.dropped.count`): counts
  metric updates or log entries a `Metrics` or `Logger` dispatcher
  discarded under backpressure or after the producer cutoff a shutdown
  begins with, labelled by which dispatcher (`"metrics"` or `"log"`) lost
  them. Delivered by the dispatcher calling the sink
  directly on its own schedule rather than through the queue it reports on,
  so the counter can never itself be part of the loss it describes, with a
  final report published after that cutoff so the interval a shutdown ends
  is reported rather than lost with the goroutine that reports it. A
  nonzero value means every other `Metrics`-sourced counter for that
  window, including `styx.heartbeat.miss.count` and `styx.timeout.count`,
  is a lower bound rather than an exact count. Reported by both the host
  and a plugin process for their own dispatcher(s); a plugin has no log
  dispatcher, so its dispatcher label is always `"metrics"`.
- **`observe.MetricStdioSinkPanic`** (`styx.stdio.sink.panic.count`): counts
  times a plugin's stdio capture recovered a panic from a caller-supplied
  `StdioSink`'s `WriteLine`, labelled by plugin and by stream. Reported as
  a per-interval delta the same way `MetricStdioDropped` is, final delta
  included, so a panicking sink cannot turn this counter into its own
  flood. Together
  with `MetricStdioDropped`, it tells apart the three reasons a captured
  line can go missing: the plugin printed nothing, the sink fell behind,
  or the sink panicked outright.
- **`styx.RegisterIdentityName(id uint64, name string)`**: records the name a
  generated FNV-1a-64 routing ID hashes from. Intended for generated code —
  `protoc-gen-go-styx` calls it once per service and per method from each
  generated file's own `init` function — so a `*PluginPanicError` raised
  through a precomputed-ID call can report the real name. Safe to call by
  hand; a duplicate `(id, name)` registration is a no-op, and an `id` claimed
  by two different names leaves that `id` permanently unresolved rather than
  guessing which name is real.
- **`ShmGeometry.RegionBytes() (uint64, error)`**: reports the exact
  shared-memory region size a geometry would cost — the same `region_size`
  `CreateRegion` mmaps for it, not an estimate, since it is derived through
  the same `internal/shm` computation `CreateRegion` itself calls. Regions
  are lazily faulted but never reclaimed for a plugin instance's life, and
  the pages they touch are charged to the host's memory cgroup, so this is
  what a capacity plan must budget per plugin. Returns an error wrapping
  `ErrInvalidConfig` (as a `*ConfigError` naming the field `"ShmGeometry"`)
  for a structurally invalid geometry, rather than a silently wrong number.
  See the new capacity-planning section of
  [`docs/configuration.md`](docs/configuration.md#capacity-planning-budgeting-host-memory-across-plugins).

### Changed

- **`PluginPanicError`**: `Plugin`, `Service`, and `Method` are now filled in
  from the host's own call context on every panicking unary call and stream
  terminal, instead of staying empty — previously only `Value` was ever
  populated, and `Error()` rendered the empty join as `"handler .panicked"`.
  A call made through the precomputed-ID API (`InvokeID`, `InvokeIDFactory`,
  `OpenStreamID`, `OpenServerStreamID` — every generated client stub) has no
  string name to give at the call site, so `protoc-gen-go-styx` now also
  emits a `styx.RegisterIdentityName` call per service/method at package
  init, letting the host resolve the ID back to the real name; only an ID
  nothing ever registered (a hand-called precomputed-ID API, or a plugin
  built with an older generator) still falls back to the routing hash
  rendered as hex. The unused `Stack []byte` field, never populated on any
  path, is removed.
- **`Host.Stop`**: the context now bounds how long the teardown waits,
  never whether it happens. A `Stop` handed an already-canceled or expired
  context previously returned immediately having torn nothing down, leaving
  the host's background workers running and — the real cost — every plugin
  process alive with nothing left to reap it. It now stops every plugin,
  which tears each instance down through the same graceful-`Shutdown`,
  `SIGKILL`, `waitpid` sequence any other `Stop` uses, and returns each
  plugin's context error instead of waiting for the join. Recovering from a
  reused `signal.NotifyContext` no longer requires a second `Stop`; giving
  `Stop` a real budget now only decides whether the teardown has finished by
  the time it returns.
- **`Host.Stop`'s fixed teardown tails** — joining the observability
  dispatchers, and waiting for an `Events()` consumer to take what the
  shutdown published — are now capped by the caller's context as well as by
  their own internal bounds, instead of being spent on top of it. A `Stop`
  given a one-second budget can no longer run for roughly 2.25 seconds, so
  sizing a shutdown deadline no longer means reading styx's constants out of
  its source.
- **`Host.Start` after a `Stop` has begun** now reports `ErrHostStopped`,
  where previously only a `Stop` that had already finished its teardown did.
  A `Host` is single-use, and the teardown decides which plugins it owns when
  it takes its snapshot, so a plugin admitted after that point would be
  supervised by a teardown that will never stop it. A name that `Stop` is
  still tearing down keeps reporting the more specific `ErrPluginStopping`.
  A `Start` already inside a plugin spawn when a `Stop` begins abandons that
  attempt and reports `ErrHostStopped` for it, leaving no process behind,
  rather than making the teardown wait the spawn out.

### Fixed

- **`Host.Stop` with concurrent callers** reported teardowns that had not
  happened. Only one caller's snapshot held the runtimes, so a second
  concurrent `Stop` found an empty host and returned `nil` while plugins
  were still being torn down; it could also reach the worker release ahead
  of the caller that owned the teardown and, with a spent context, end the
  `Events()` subscription immediately — cutting short a drain the first
  caller still had budget for. Concurrent `Stop`s are now serialized: one
  owns the teardown and the others wait for it, so every caller returns only
  once a teardown it observed has finished or its own context has expired,
  and the bounded teardown waits are always charged against the owner's
  budget. A waiting caller with budget left runs its own pass afterward,
  which is how it completes a join an earlier caller ran out of time for.

- **`PluginCrashError.StderrTail`** could come back empty for a plugin that
  had in fact written to stderr before dying, taking the stderr suffix
  embedded in `Reason` with it. The tail a crash reason is built from was
  reachable only through the same bounded queue that feeds a
  `PluginSpec.Stdio` sink, so the queue could discard it twice over: a sink
  falling behind dropped lines once the queue filled, and the cancellation
  that precedes reporting a crash ended delivery with lines still queued.
  A plugin that sprayed output before dying was then indistinguishable from
  one that printed nothing — the exact case the tail exists for. The tail is
  now written by the goroutine that reads the pipe, before the queue, so it
  sees neither the drops nor the cancellation; the queue still isolates the
  caller-supplied sink, which is the only part that needs it.

  One narrower window remains: the stdio pipes are closed during process
  teardown while the reader may still be draining them, so a plugin that
  writes to stderr and exits in the same instant can still lose its tail.
  Where stderr is the whole diagnosis, configure a `PluginSpec.Stdio` sink
  and log from there as well — that path does not share this window.

## [0.2.0] - 2026-08-01

Public-API additions from the first external integration of the
shared-memory transport, plus the fixes that integration surfaced.

### Added

- **`ErrPayloadTooLarge`**: an oversize payload is rejected before any byte
  is published, and the rejection is now matchable with `errors.Is` at the
  public API — on unary `Invoke`, stream opens, and stream sends — instead
  of surfacing only as a wrapped internal sentinel.
- **Live stdio observation**: `PluginSpec.Stdio` accepts a `StdioSink`
  receiving every stdout/stderr line a plugin writes, live; a slow or
  panicking sink never blocks or crashes the plugin. `PluginCrashError`
  additionally carries the crash-reason stderr tail as a structured
  `StderrTail []string`, so log pipelines no longer parse it out of
  `Reason`.
- **`Host.Health(name)`**: a pull-based, level-triggered health snapshot —
  most recent lifecycle state, last transition time, last error, and the
  current consecutive missed-heartbeat count — for synchronous probes that
  previously had to rebuild retained state from the edge-triggered
  `Host.Events()` channel. Unknown names answer the new
  `ErrUnknownPlugin`.
- **Protobuf editions support** in `protoc-gen-go-styx`: `edition = "2023"`
  contract files now generate (editions 2 through 2024, mirroring the
  pinned protobuf runtime), with golden and response-contract tests.
- **`NoRestart`**: a named supervision policy for hosts whose embedding
  supervisor owns restart decisions, plus a documented host-owned-restart
  pattern (fresh `Host` per recreation) in `docs/plugin-lifecycle.md`.
- **`IncompatibleError.Kind`**: distinguishes a binary-identity pin
  mismatch (`IncompatibleBinaryIdentity`) from a genuine handshake
  incompatibility (`IncompatibleHandshake`) without parsing `Reason`.
- **`ErrPluginAlreadyStarted`**: `Start` now refuses a name whose plugin
  this `Host` already started, instead of silently attaching a second
  supervisor to it.

### Fixed

- An oversize `STREAM_OPEN` surfaced wrapped in the retryable
  `ErrPluginUnavailable`, mislabeling a deterministic rejection as
  transient; it now maps to the non-retryable `ErrPayloadTooLarge`.
- The shared-memory writer's arena oversize backstop reported an error
  outside the never-published classification, which would have
  misclassified a provably-unpublished send as outcome-unknown.
- `make lint` never linted the failpoint-tagged files that
  `make test-failpoint` tests; the `ci` gate now runs both passes.

## [0.1.0] - 2026-08-01

Initial release.

> **Stability:** this is a pre-1.0 release. The public Go API may still move
> between minor versions; the on-the-wire contracts (`docs/specs/shm-abi.md`,
> `docs/specs/stream-protocol.md`) are frozen and change only by explicit,
> versioned amendment.

### Added

- **Shared-memory data plane**: sealed memfd region with descriptor rings, a
  slab payload arena, and eventfd wakeups. Unary round trip p50 of 2.4 µs at a
  64-byte payload — against 7.7 µs over Unix domain sockets and 15.9 µs over
  gRPC-over-UDS. Faster than `hashicorp/go-plugin` in all 24 cells of the
  comparison matrix (1.65×–4.72× on throughput, 64 B–1 MiB payloads,
  concurrency 1–64).
- **Unix-domain-socket transport** as a fallback data plane, and
  `TransportAuto` negotiation between the two.
- **Protobuf IDL with gRPC-style generated stubs** via `protoc-gen-go-styx`:
  unary and all three streaming shapes; callers never see shared-memory
  details.
- **Supervised plugin lifecycle**: process isolation with crash boundaries,
  restart policies, health from heartbeat progress counters,
  subscription-based supervisor events (`host.Events()`), and
  state-preserving hot reload.
- **Default arena geometry**: a seven-rung ladder from 256 B to 1 MiB with
  headroom-aligned slab sizes, plus a worked custom-geometry sizing guide in
  `docs/configuration.md`.
- **Typed error surface**: handler errors as `*styx.Status`; panic and crash
  isolation as `*styx.PluginPanicError` / `*styx.PluginCrashError`; explicit
  retryability semantics (`ErrBackpressure`, `ErrRequestDeclined`) and honest
  ambiguity (`ErrOutcomeUnknown` only when bytes were accepted with no reply).
- **Frozen wire contracts**: `docs/specs/shm-abi.md` and
  `docs/specs/stream-protocol.md`, with stable, never-renumbered section
  anchors.
- **Validation suites**, all CI-gated: a differential suite against the UDS
  oracle, a fault-injection (chaos) suite, a long-running leak soak, and a
  failpoint suite.
- **Runnable examples**: echo, streaming (all three shapes), state-preserving
  hot reload, a backpressuring slow handler, and a real consumer's
  device-plugin lifecycle contract (`examples/device-gateway/`).
- **Migration guide** from `hashicorp/go-plugin`
  (`docs/migration-from-go-plugin.md`).

[0.3.0]: https://github.com/arloliu/styx/releases/tag/v0.3.0
[0.2.0]: https://github.com/arloliu/styx/releases/tag/v0.2.0
[0.1.0]: https://github.com/arloliu/styx/releases/tag/v0.1.0
