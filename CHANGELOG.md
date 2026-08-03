# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  so a plugin spraying output cannot turn this counter into its own flood.
- **`observe.MetricObserveDropped`** (`styx.observe.dropped.count`): counts
  metric updates or log entries a `Metrics` or `Logger` dispatcher
  discarded under backpressure, labelled by which dispatcher (`"metrics"`
  or `"log"`) lost them. Delivered by the dispatcher calling the sink
  directly on its own schedule rather than through the queue it reports on,
  so the counter can never itself be part of the loss it describes. A
  nonzero value means every other `Metrics`-sourced counter for that
  window, including `styx.heartbeat.miss.count` and `styx.timeout.count`,
  is a lower bound rather than an exact count.

### Changed

- **`PluginPanicError`**: `Plugin`, `Service`, and `Method` are now filled in
  from the host's own call context on every panicking unary call and stream
  terminal, instead of staying empty — previously only `Value` was ever
  populated, and `Error()` rendered the empty join as `"handler .panicked"`.
  A call made through the precomputed-ID API (`InvokeID`, `InvokeIDFactory`,
  `OpenStreamID`, `OpenServerStreamID` — every generated client stub) has no
  string name to give, so `Service`/`Method` carry the routing hash rendered
  as hex instead. The unused `Stack []byte` field, never populated on any
  path, is removed.

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

[0.2.0]: https://github.com/arloliu/styx/releases/tag/v0.2.0
[0.1.0]: https://github.com/arloliu/styx/releases/tag/v0.1.0
