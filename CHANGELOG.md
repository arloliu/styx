# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/arloliu/styx/releases/tag/v0.1.0
