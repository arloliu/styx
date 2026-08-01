# Styx — High-Level Design

**Date:** 2026-07-16
**Status:** Draft for review
**Reference spec:** `tmp/kickoff.md`
**Reference consumer:** a device gateway — a host application that manages a
fleet of out-of-process device-protocol driver plugins for connected
hardware

---

## 1. Executive Summary

Styx is a new Go plugin framework for local, same-machine, process-isolated plugin
communication. It preserves the isolation model of hashicorp/go-plugin — plugins are
separate executables with their own runtime, heap, GC, and crash boundary — but replaces
gRPC-over-Unix-domain-socket with a shared-memory data plane: descriptor rings plus a
payload arena, eventfd-based wakeups, and protobuf payloads.

Target: unary RPC round-trips in the low single-digit microseconds (vs. tens of
microseconds for gRPC over UDS), with allocation-light steady state, while remaining
robust enough for enterprise-critical inline industrial environments (24/7
operation, crash isolation, supervised recovery).

Users define services in standard protobuf `service` blocks and get gRPC-style generated
clients and servers. They never see shared memory, ring indices, or eventfds.

## 2. Project Philosophy

1. **Correctness before speed.** The unsafe cross-process core stays as small as
   possible; everything clever must be benchmarked, bounded, and test-saturated.
2. **Never trust the other side of the wall.** Every value read from shared memory is
   potentially corrupt. Validate, bound-check, and poison-and-restart rather than repair.
3. **Boring control plane, fast data plane.** Robustness features (handshake, liveness,
   teardown) run over a conventional socket; only the RPC hot path rides shared memory.
4. **Boring public API.** Idiomatic Go, gRPC-shaped stubs, zero IPC concepts leaked.
5. **Benchmark-driven.** The performance premise is proven in a spike before
   framework code is written, and guarded by CI regression benchmarks afterward.

## 3. Comparison with HashiCorp go-plugin

| Aspect | go-plugin | Styx |
|---|---|---|
| Isolation | subprocess | subprocess (same) |
| Transport | gRPC or net/rpc over UDS/TCP | shared-memory rings; UDS fallback |
| IDL | protobuf (via gRPC) | protobuf (same `service` syntax) |
| Unary latency | ~30–80 µs typical | target ≤3 µs SHM, spike-verified |
| Handshake | env vars + stdout line | socketpair + framed protobuf control protocol |
| Versioning | single protocol integer | protocol range + plugin semver + per-service versions + feature flags |
| Health | gRPC health service + broker | control-plane heartbeat + SIGCHLD + supervisor |
| Streaming | yes (gRPC/HTTP2 streams) | yes — gRPC-shaped stubs over plain ring messages (no stream-aware transport) |
| Cross-language | yes | layout documented, not committed in v1 |
| Restart/supervision | left to host (host loops) | built-in supervisor with policy |
| Hot-reload + state handoff | not built-in | built-in (device-gateway requirement) |

What Styx deliberately keeps from go-plugin: plugin-as-executable, version-negotiated
handshake, typed interfaces, host-side lifecycle ownership, crash isolation.

## 4. Non-Goals

- Not a distributed/network RPC framework; no TCP transport for application traffic.
- Not a service mesh, HTTP framework, or gRPC replacement for remote services.
- Not a `.so`/cgo plugin loader.
- Not (in v1) a non-Go plugin platform or a Windows/macOS production target (tests may
  run with the UDS transport on macOS for dev convenience).
- Not a stream-multiplexing transport: streaming exists as a high-level API, but the
  transport layer never grows HTTP/2-style stream states, windows, or priorities.
- Not a plugin-package distributor/fetcher: `PluginSpec` takes a local binary path and
  an optional SHA-256 hash. Downloading, caching, or resolving plugin packages at
  runtime (the device gateway's own package store/fetcher) is the host application's
  job, not Styx's (§27, Open Question 4).

## 5. Requirements from the Reference Consumer (a device gateway)

The reference consumer is a device gateway: a host application that manages a fleet of
out-of-process device-protocol driver plugins, each speaking a specific wire protocol
to a piece of connected hardware. It already runs those plugins via `arloliu/go-plugin`
with a lifecycle contract (`Init/Start/Stop/HotReload/SaveRuntimeState/LoadRuntimeState/CollectMetrics`).
Styx must support that contract as an ordinary user-defined service, and natively provide
what the device gateway currently builds around go-plugin:

- health checks / heartbeat with restart policy and exponential backoff
- hot-reload: graceful drain, state save on the old process, state restore on the new one
- stdout/stderr capture and structured routing (the device gateway ships plugin stderr
  to a structured log sink)
- versioned plugin binaries, identity verified at handshake (the device gateway fetches
  plugin packages at runtime)
- an error taxonomy that distinguishes fatal from transient failures — the device
  gateway fast-shuts-down on critical device faults, so misclassification is an outage
- Prometheus-friendly metrics hooks; structured-logging-friendly hooks (no forced deps)
- container/Kubernetes deployment: host and plugins share one container; no reliance on
  named files in `/dev/shm` surviving restarts
- **process-group-scoped kill, not single-PID.** `arloliu/go-plugin` originally
  SIGKILL'd only the plugin's own PID, orphaning any grandchild it forked (drivers,
  subprocess tooling); fixed upstream-of-Styx by killing the plugin's whole process
  group, with a configurable opt-out for TTY-interactive dev use (§27 Open Question 2
  catalog). Styx's teardown machine (§9) must do the same from day one.
- **panic-isolated internal goroutines.** The same fork catalog found host-crashing
  panics in log-pump/stdout-capture goroutines triggered by malformed plugin output or
  a panicking user-supplied sink. Styx's capture and observability goroutines (§18)
  must recover from panics in anything touching plugin-controlled or user-supplied
  data — this is not optional hardening, it's a repeat of a real incident class.

## 6. Architecture

```
Host process                                Plugin process
─────────────                               ──────────────
Application code                            Service implementations
Generated client stubs                      Generated server stubs
Styx RPC runtime  ◄── codec (protobuf) ──►  Styx RPC runtime
Transport interface                         Transport interface
  ├─ shm transport ══ memfd rings/arena ══  shm transport
  └─ uds transport ── framed protobuf ────  uds transport
        │                                        │
        └───── control plane (socketpair): ──────┘
               handshake, fd passing, heartbeat, shutdown
```

**Control plane.** At spawn, the host creates a `socketpair(AF_UNIX, SOCK_SEQPACKET)`
and passes one end to the child as an inherited fd. All control traffic is small
length-delimited protobuf messages: `Hello`, `HelloAck` (version/feature negotiation),
`AttachRegion` (memfd fds via `SCM_RIGHTS`), `Heartbeat`, `Drain`, `SaveState`,
`Shutdown`, `Poisoned`. Low rate, easy to evolve, easy to reason about.

Control-protocol contract: every message type has a maximum encoded size and a reply
deadline; one seqpacket datagram carries exactly one message; `MSG_TRUNC`/`MSG_CTRUNC`
is a protocol violation (poison). Messages carry a correlation ID and the current
region generation; fd-bearing messages (`AttachRegion`) state the exact fd count, and a
mismatch is a violation. A per-lifecycle-state table defines which messages are legal;
anything else is a violation. Large payloads (hot-reload state snapshots) never travel
inline: they are passed as a separate sealed memfd with declared length, format version,
and checksum, so the control channel that carries heartbeats can never be blocked by a
bulk transfer.

**Data plane.** One shared-memory region per plugin created with `memfd_create` and
sealed (`F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL`), passed as an fd over the control
socket. No filesystem name → the kernel reclaims the region when the last mapping dies;
there is no stale-segment cleanup problem after crashes.

**Transport abstraction.** `shm` is the production transport. `uds` (framed protobuf
over a second socketpair) implements the same internal transport interface and serves
three purposes: degradation path when SHM is unavailable, correctness oracle for
differential testing, and the vehicle for building the full framework before the
SHM transport exists. Transport selection is negotiated at handshake and
configurable per plugin.

## 7. Package Layout

Module `github.com/arloliu/styx` (final org is Open Question 1).

```
styx/                      // public: Host, HostConfig, PluginSpec, PluginServer,
                           //         ClientConn, errors, options
  codec/                   // Codec interface; protobuf impl (default)
  supervisor/              // restart policy types (public config surface)
  observe/                 // metrics/log/trace hook interfaces (no vendor deps)
  internal/control/        // control-plane protocol (framed protobuf, fd passing)
  internal/transport/      // transport interface + uds + shm implementations
  internal/ring/           // SPSC descriptor ring (the unsafe core)
  internal/arena/          // payload arena allocator
  internal/shm/            // memfd, mmap, sealing, region layout
  internal/event/          // eventfd + spin-park waiter
  internal/rpcruntime/     // request table, deadlines, cancellation, dispatch
  cmd/protoc-gen-go-styx/  // code generator
  examples/
  bench/
```

Top-level `styx` package is the single import for both host and plugin authors —
mirrors go-plugin's ergonomics. Everything sharp lives under `internal/`.

## 8. Public API (target shape)

Host:

```go
host := styx.NewHost(styx.HostConfig{
    Plugins: []styx.PluginSpec{{
        Name:    "image",
        Path:    "./plugins/image-plugin",
        Restart: styx.RestartPolicy{Max: 5, Backoff: styx.ExpBackoff(time.Second, time.Minute)},
    }},
})
if err := host.Start(ctx); err != nil { ... }

client := imagepb.NewImageProcessorClient(host.Plugin("image"))
resp, err := client.Resize(ctx, &imagepb.ResizeRequest{Width: 800, Height: 600})
```

Plugin:

```go
func main() {
    srv := styx.NewPluginServer()
    imagepb.RegisterImageProcessorServer(srv, &ImageProcessor{})
    if err := srv.Serve(); err != nil { os.Exit(1) }
}
```

`host.Plugin(name)` returns a `*styx.ClientConn` that generated constructors accept —
analogous to `grpc.ClientConnInterface`. Supervisor events are exposed as a subscription
(`host.Events()`), never as callbacks invoked on internal goroutines holding locks.

## 9. Host and Plugin Lifecycle

**Host:** declare plugins → spawn each child with control socketpair + sanitized env →
handshake (versions, features, service list, binary identity) → create/seal/pass SHM
region → plugin signals ready → supervisor begins heartbeat monitoring. On failure:
teardown (below), restart per policy with fresh region and incremented generation.
Shutdown: `Drain` (stop accepting, finish in-flight with deadline) → `Shutdown` →
wait with timeout → SIGKILL fallback.

**Teardown state machine (normative).** Teardown — for crash, poison, restart, or
shutdown alike — follows a fixed order: (1) atomically stop admission and detach the
routing target; (2) fail every in-flight call and open stream with the appropriate typed
error and wake all waiters (including parked eventfd waiters, via a dedicated shutdown
signal); (3) join every goroutine that can touch the mapping; (4) `munmap`;
(5) **when teardown is terminating a child process** (old hot-reload instance, poisoned
or wedged plugin, shutdown), graceful `Shutdown` with a deadline (configurable per
plugin, not hard-coded — a fork pain point, §27 Open Question 2) over the still-open
control socket → `SIGKILL` fallback **sent to the plugin's process group, not just its
PID** (configurable opt-out for TTY-interactive dev use — the same fork pain point) →
`waitpid` reap, always; (6) close all local fds exactly
once — fd closure is deliberately last so step 5's `Shutdown` exchange still has its
control socket. Teardown is not complete until the reap. After a crash or
health-triggered restart, a successor instance must not be promoted `Ready` while the
predecessor PID is alive or unreaped. Hot-reload promotion is the one deliberate
exception (hot-reload phase 5): the predecessor is provably drained and frozen —
incapable of touching hardware or state — when the successor is promoted, and its
teardown-with-reap must still complete for the reload transaction to be done. No step
may be reordered; use-after-unmap is impossible by construction because nothing that
can touch the region survives step 3. fd discipline:
every fd is `CLOEXEC` except the two intentionally inherited bootstrap fds; fds received
via `SCM_RIGHTS` are counted against the message's declared count and owned by the
receiver from that point; ownership tables are asserted in tests (no fd leaks across
restart, verified by counting).

**Plugin:** `Serve()` reads the inherited control fd → handshake → receives region fd,
maps it → registers services → serving loop. On host disconnect (control socket EOF —
the host crashed): stop serving, run registered cleanup hooks, exit. A plugin never
outlives its host: `PR_SET_PDEATHSIG` is installed as a backstop, and because the parent
may die before installation, the child re-checks `getppid()` immediately after
installing it and exits if reparented.

**Hot-reload (device-gateway-critical)** is a transactional state machine, not a command
sequence. Phases, each with an explicit acknowledgement and deadline:

1. **Cutoff** — host atomically stops admitting new calls to the plugin (they queue or
   fail per config, bounded).
2. **Drain-ack** — the plugin acknowledges drain only after all accepted calls have
   finished AND mutable state is frozen (background mutators stopped).
3. **Snapshot** — plugin produces a bounded, versioned, checksummed state snapshot,
   transferred via a snapshot memfd that is **fully sealed before transfer**
   (`F_SEAL_WRITE|F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL` — unlike the live SHM region,
   a snapshot is immutable by contract); the host verifies the seal set via
   `F_GET_SEALS`, the declared length, and the checksum, and maps it read-only, before
   acting on it. A snapshot the producer could still mutate is a protocol violation.
4. **Restore & validate** — host spawns the new binary, completes handshake, delivers
   the snapshot; the new plugin acknowledges successful restore and readiness.
5. **Promote** — host atomically swaps the stable `ClientConn` routing target to the
   new instance (single linearization point; no call ever spans the snapshot boundary),
   then joins the answers the old instance already produced, and only then terminates
   it via the normal teardown machine (including the step-5 reap). The join is the
   host's half of the phase-2 guarantee: drain-ack proves the plugin wrote every
   accepted call's response to the transport, and this waits until the host's own
   reader has consumed them and no call is still awaiting an outcome. Without it,
   teardown fails calls that had in fact completed, reporting a non-retryable unknown
   outcome for answers already sitting unread. The wait is bounded at one second — every
   answer is already in local memory, so this covers a scheduling hiccup, not a network
   — and it deliberately ignores the reload caller's cancellation, since by this point
   the reload is committed and cancelling could only discard answers already in hand.
   Anything still unanswered when the bound expires is at risk of being failed as
   outcome-unknown, and that many are counted on `styx.reload.dropped.count` — an
   upper bound on the loss rather than an exact count, since the reader keeps running
   through the first teardown steps and may still resolve some of them.

**Rollback is defined from every pre-promotion phase, and it reverses the freeze.**
An abort during phase 1 (cutoff) is trivial: nothing is frozen and no successor exists,
so the host simply reopens admission. Failure or deadline-miss in phases 2–4 aborts the
reload: the new process (if spawned)
is torn down, then the host sends `Resume` to the old instance, which restarts its
background mutators and acknowledges the unfreeze — **only after that ack does the host
reopen admission**. Calls can therefore never enter a half-frozen plugin. If the old
instance dies during rollback, the supervisor reports the failed reload as a
crash-equivalent event and the normal restart path (with the snapshot, if phase 3
completed) takes over.

In-flight requests therefore either complete on the old instance before drain-ack or
were never admitted; nothing is silently dropped, and nothing executes on both sides of
the snapshot.

## 10. Handshake and Versioning

Carried over the control socket as protobuf (not env vars — structured, evolvable, and
supports fd passing in the same channel). go-plugin's single protocol integer is
explicitly rejected as too coarse; Styx negotiates on three independent axes:

1. **Styx protocol version** — a supported *range* (`min..max`), not a single value.
   Current protocol is v1; both sides intersect ranges and speak the highest common
   version. Within a protocol version, optional capabilities are negotiated via
   **feature flags** (named booleans), so most evolution never bumps the version at all.
   A protocol bump is reserved for changes that cannot be expressed as an additive flag.
2. **SHM layout version** — versions the region/ring/descriptor memory layout
   independently of the message protocol (a layout change must not force a protocol
   change, and vice versa).
3. **Plugin identity and version** — plugin name, **plugin semver** (the plugin author's
   own release version, surfaced to the host for logging/metrics/compatibility policy),
   binary identity (SHA-256, checked when pinned), and advertised services each with a
   per-service version.

Also exchanged: codec support, transport support (shm/uds), and a per-launch nonce the
child must echo (guards against attaching to a stale/foreign process). Negotiation
failures produce a typed `ErrIncompatible` carrying both sides' offers, reported through
supervisor events — never a silent fallback.

Negotiation selects one complete **compatibility tuple** — (protocol version,
transport, layout version if SHM, feature set, codec) — from a documented support
matrix; the tuple is acknowledged explicitly by both sides before any region is
attached, so an untested combination of individually-valid versions can never run.
Transport is part of the tuple, not a side negotiation: which layouts, features, and
codecs are valid depends on the transport chosen. Feature flags have stable
string identifiers and declared dependencies; flags are classified *required* or
*optional* per side, and an unknown or unsupported *required* flag fails the handshake
(fail-closed) rather than being ignored. The host additionally declares a required
version range per service it intends to call (from generated-code metadata, which
embeds the generator/runtime ABI version); a plugin that cannot satisfy them fails
handshake with the offending service named in the error.

## 11. Shared Memory Layout

One region per plugin (v1), fixed layout, versioned by `layout_version`:

```
[ layout page    ] IMMUTABLE: magic, layout version, generation, ring geometry,
                   arena geometry — written once by the host before fd passing
[ sync page      ] MUTABLE: ring head/tail indices, park/wake state words,
                   poison flag, progress counters
[ ring H→P       ] descriptor ring, host→plugin (requests)
[ ring P→H       ] descriptor ring, plugin→host (responses)
[ arena H→P      ] payload slabs, owned/allocated by host (request payloads)
[ arena P→H      ] payload slabs, owned/allocated by plugin (response payloads)
```

- Immutable layout metadata lives on its own page, is validated once at attach (against
  the sealed region size, with overflow-safe arithmetic), then **cached locally and
  never re-read** — a peer that later scribbles on the layout page cannot redirect the
  other side's memory accesses. Where practical the layout page is remapped read-only
  after validation. Mutable synchronization state never shares a page with geometry.
- `generation` increments on every restart; descriptors carry it so late writes from a
  dying process are detected and discarded.
- A `poison` flag set by either side marks the region unrecoverable; the supervisor
  tears down and restarts with a fresh region. There is no in-place repair path — by
  design.

## 12. Ring and Arena Design

- **SPSC rings, one per direction, one writer goroutine per ring — on both sides.**
  Single-producer/single-consumer keeps the memory-ordering story tractable (one writer
  per index; the tail and park-state words are seq_cst per the framework's notification
  ground rule, and no weaker ordering appears anywhere in the protocol), but SPSC only holds if each ring
  has exactly
  one writing task. Normatively: on each side, a single writer goroutine owns its
  outbound ring and outbound arena; concurrent callers (host) and concurrent handlers /
  streaming `Send`s (plugin) submit completed, immutable send-intents through a bounded
  in-process queue to that writer, which alone allocates payload slabs, writes
  descriptors, publishes the tail, and signals. **The writer serves two intent lanes:**
  a dedicated bounded lifecycle lane and the data lane. The lifecycle lane carries
  exactly the two descriptor-only data-plane frame kinds that must make progress
  regardless of data traffic: `CANCEL` (strict priority, always) and `STREAM_ACK`
  (reserved capacity but **bounded** priority — ACKs are cumulative per stream, so the
  writer coalesces them and alternates with the data lane under a bounded burst rule
  defined in `stream-protocol.md`; credit return must never be starved by data, and
  data must never be starved by a hot stream's ACK traffic — both directions get
  bounded progress). Drain and shutdown are control-plane messages, not ring
  frames, and never compete here. Lifecycle frames take no arena slab, so the writer
  can always emit them into the reserved descriptor budget — and it is forbidden
  to block on data-lane work (arena space, a full data descriptor window) while
  lifecycle intents are pending. Per-CPU sharded rings are an explicitly reserved v2
  optimization (geometry field exists in the layout page).
- **Fixed-size descriptors** (one cache line, 64 B): call ID, service ID, method ID,
  kind/flags, generation, allocation sequence, payload offset/length, deadline budget,
  trace context. Head/tail indices on separate cache lines to avoid false sharing.
- **Payload arena, not in-ring payloads.** Variable-size data in the ring destroys index
  arithmetic simplicity and cache behavior; slab arenas (size classes) give payload
  reuse and bounded memory.
- **Arena ownership protocol (normative).** Each direction has its own arena, allocated
  from and freed to only by that direction's producer (its single writer goroutine) —
  the free lists are never touched by two processes. Consumption is acknowledged by
  ring-head advancement: the producer may reclaim a slab only after the consumer's head
  has passed the descriptor referencing it, and the head advance is itself the proof
  that the payload has been consumed — consumers finish reading the slab, by copying the
  bytes out or decoding them in place, before advancing, and read nothing from the slab
  afterwards (shm-abi.md §9 states the rule normatively). Every
  slab handle carries (generation, allocation sequence); a stale pair is a protocol
  violation. Cancellation/timeout never reclaims a slab early — the slot is released
  only via normal head advancement or region teardown, so use-after-free and ABA are
  impossible rather than unlikely. Crash reclaims everything at once: the region is
  discarded whole.
- **Zero-copy is opt-in, never implicit.** Generated protobuf messages are always fully
  owned copies — the runtime cannot know whether a handler retains a message, a bytes
  field, or a sub-object, so it never gambles: decode copies out of the arena, then the
  slab is releasable. A separate, explicitly borrowed API (scoped `styx.Borrowed[T]`
  with a mandatory release, usable only inside the handler frame) is reserved for v1.x
  for callers who prove they need the copy back; it cannot leak borrowed bytes into
  ordinary messages.
- Arena exhaustion and ring-full are backpressure signals, never grow events.
- **Go's official `arena` package: considered and rejected.** It is experimental
  (`GOEXPERIMENT=arenas`), its proposal (golang/go#51317) is on indefinite hold with the
  API subject to incompatible change or removal, and — decisively — it allocates
  GC-managed Go objects from the runtime's own heap. It cannot allocate at fixed offsets
  inside an `mmap`'d shared region, which is the whole job here. Styx's arena is a small
  hand-rolled slab allocator over the memfd region.

## 13. Message Frame / Descriptor Format

The descriptor (above) is the only frame type. A `kind` field (part of `flags`)
distinguishes `UNARY_REQ`, `UNARY_RESP`, `STREAM_OPEN`, `STREAM_MSG`, `STREAM_ACK`
(credit return), `STREAM_CLOSE`, `STREAM_ERR`, `CANCEL`; the call ID field is
shared by unary calls and streams.
Remaining flag bits reserve compression and trace-context presence. Optional payload
checksum (CRC32C, negotiated feature flag) for paranoid deployments; off by default
since the kernel guarantees SHM integrity — the checksum defends against buggy plugins,
not the kernel. Error responses carry a status payload (protobuf `Status`-like: code,
message, details) instead of a normal payload.

The prose above is deliberately not implementable on its own. A **byte-exact,
endian-defined ABI document** (`docs/specs/shm-abi.md`) is a gating deliverable of the
SHM-transport work:
exact field offsets/widths/alignment of the descriptor and sync page, the atomic
primitives used per field, executable enqueue/dequeue pseudocode with the normative
ordering (payload write → descriptor write → tail seq_cst-store; tail seq_cst-load →
descriptor read → payload read — the tail and park-state words are seq_cst per the
framework's notification ground rule, which also orders the descriptor/payload accesses around them; no weaker
ordering appears anywhere in the protocol), unsigned wraparound arithmetic for
full/empty tests,
initialization values, and compile-time layout assertions per supported architecture.
No SHM transport code merges before that document exists and the implementation
cross-references it.

## 14. RPC Runtime

- **Per-call state machine with exactly one terminal transition.** Request table keyed
  by call ID (monotonic within a generation, never reused within it). States:
  `SUBMITTED → {REJECTED | CANCELED | DEADLINE}` (terminal before publication: admission
  failure, queue failure, cancel/deadline while still local) and
  `SUBMITTED → PUBLISHED → {COMPLETED | FAILED | CANCELED | DEADLINE | OUTCOME_UNKNOWN |
  REJECTED}`. The post-publication `REJECTED` terminal belongs to a send that encodes
  its payload directly into the transport's own buffer (a fill send) rather than into a
  wire buffer first: when that encode runs, after the publication CAS, and fails, the
  transport discards the frame and releases its buffer without ever emitting a
  descriptor. That is a provable non-dispatch — the same guarantee a pre-publication
  `REJECTED` carries — so the call terminates the same way even though publication
  already happened locally.
  Transitions are arbitrated by CAS on the call state, which resolves the
  publication/cancellation race atomically: a cancel that wins before `PUBLISHED` means
  the descriptor is never written and nothing crosses the ring; after `PUBLISHED`, a
  `CANCEL` descriptor is emitted. The first terminal transition wins; late frames for a
  terminal call are discarded and their payload slots released through normal head
  advancement. **No tombstones are needed and none are kept** (they would grow without
  bound): because call IDs are never reused within a generation, any frame whose ID is
  absent from the request table is by construction late-or-unknown and is discarded the
  same way.
- **Cancellation is a data-plane `CANCEL` descriptor on the same ring** — same path,
  same ordering as the request it cancels, so it can never overtake it. There is no
  control-plane cancellation fallback (an unordered second path is how cancellations
  get lost); if the ring is wedged, the deadline or supervisor path terminates the call
  anyway.
- **Deadlines travel as remaining budget** (relative duration at send time), not
  wall-clock timestamps, and are re-anchored to the receiver's monotonic clock —
  wall-clock adjustments cannot expire or extend calls. Both sides enforce: the plugin
  checks budget before dispatch and after handler return; abandoned requests are reaped
  by deadline.
- **Delivery semantics are explicit: at-most-once dispatch per call ID, and Styx never
  lies about what it knows.** A failure before the request descriptor is consumed is
  reported as not-dispatched (safely retryable — the handler provably never ran). Any
  failure after the plugin may have begun the handler — crash mid-call, poisoned
  region, lost response — is `ErrOutcomeUnknown`, NOT retryable by default: for
  device-facing calls, "did it happen?" must reach the application. Methods may opt
  into automatic retry by declaring idempotency (generated-code option); a retry is a
  NEW call ID and **may execute the handler more than once** — that is what the
  idempotency declaration asserts is safe. Styx transports an application-supplied
  deduplication key with each attempt but does not itself provide an effect-once
  guarantee or a durable dedup store; deduplication, if needed, is owned by the
  application/handler. This is stated bluntly because pretending otherwise is how
  devices get double-actuated.
- **Handler panic taints the process (default policy).** The plugin runtime returns a
  `PluginPanicError` status for the panicking call when it can, then initiates
  controlled termination; the supervisor restarts per policy. A recovered panic can
  leave plugin state inconsistent — continuing to serve from that state is not
  acceptable in the default enterprise profile. `ContinueAfterPanic` exists as an
  explicit opt-in for handlers that guarantee isolation. A panic in the Styx runtime
  itself always exits (crash path).

### Streaming: gRPC-shaped API, message-oriented transport

Styx offers the full gRPC streaming surface (server-streaming, client-streaming, bidi —
`Send`/`Recv`/`CloseSend`, stream-scoped `ctx`) as a **pure RPC-runtime concept**. The
transport stays message-oriented and stream-unaware: a stream is just a sequence of
ordinary descriptors sharing a call ID (`STREAM_OPEN` … `STREAM_MSG`* … `STREAM_CLOSE`).

This is the deliberate simplification the ring's latency buys: gRPC needs HTTP/2 stream
states, per-stream windows, and priority trees because streams share a high-latency,
head-of-line-blocking byte pipe. On a microsecond-latency ring none of that pays for
itself. Flow control degenerates to two bounded mechanisms:

- the ring/arena backpressure that already exists (a sender blocks or errors exactly
  like a unary caller), and
- a per-stream credit counter (default N outstanding `STREAM_MSG`s, negotiated at
  `STREAM_OPEN`) so one hot stream cannot monopolize the shared ring, plus a global
  per-connection cap on open streams.

No windows, no priorities, no transport stream table — but "simple" does not mean
"unspecified". Streaming ships only after a complete stream-protocol spec
(`docs/specs/stream-protocol.md`, a gating deliverable of the enterprise-features
work) defines: per-message
sequence numbers within a stream; the credit-return rule (`STREAM_ACK` frames
replenishing sender credit as the receiver consumes, so both sides can never wait on
each other forever); the half-close state machine (client `CloseSend`, server
completion, and their legal interleavings); arbitration when cancel/error/close race
(one terminal outcome per stream, first-wins, same late-frame discard rule as unary
calls); and
duplicate/late/out-of-order frame disposal with payload release. Stream teardown
follows the unary rules: deadline/cancel maps to `CANCEL` + `STREAM_ERR`; a crashed
peer fails all open streams with the same typed crash errors as unary calls.

## 15. Notification

Hybrid spin-then-park, per direction, with an explicitly race-free arming protocol —
the naive "check a parked flag, maybe signal" design loses wakeups (producer reads
`awake` an instant before the consumer parks → consumer sleeps on a non-empty ring).
Normative protocol:

- **Memory-ordering ground rule:** the tail word and the park-state word are both
  accessed ONLY with sequentially-consistent atomics (Go's `sync/atomic`, which is
  seq_cst — Styx deliberately does not attempt weaker orderings for these two words, on
  either side, in any language binding). Release-publish + independent load is NOT
  sufficient here and is explicitly forbidden: the Dekker-style argument below needs
  both critical accesses in the single total order that only seq_cst provides.
- **Consumer parking:** spin up to a *time-based* budget (default tuned during the
  initial performance-spike benchmarking; a zero-spin mode exists and must be
  correct, only slower) → seq_cst exchange of the
  state word to `PARKED` → **re-load the ring tail (seq_cst)** → if work appeared,
  store `AWAKE` and consume; otherwise block on eventfd read. **On every wake
  (eventfd or spurious), the consumer stores `AWAKE` first, then re-scans the ring** —
  the parked state is never left dangling after a wake.
- **Producer signaling:** write payload → write descriptor → **seq_cst store of the
  tail** → seq_cst load of the consumer state word → if `PARKED`, write the eventfd.
  Because tail-store/state-load (producer) and state-store/tail-load (consumer) are all
  in the seq_cst total order, at least one side observes the other's write in every
  interleaving: a wakeup can be spurious but never lost. `shm-abi.md` must include
  litmus tests covering every interleaving of these four accesses, including the
  post-eventfd `AWAKE` transition.
- **Eventfd semantics:** non-semaphore mode, reads drain the counter (coalescing is
  fine — the consumer always re-scans the ring after waking); `EINTR`/`EAGAIN` retried;
  a dedicated shutdown word + eventfd write unparks waiters during teardown so no
  goroutine sleeps through region destruction.

Go runtime integration is a design input, not a benchmark afterthought: the eventfd is
wrapped so blocking waits go through the runtime poller (goroutine parks, OS thread is
released) rather than pinning a thread in a raw read; spinning yields to the scheduler
(`runtime.Gosched`) at sub-budget intervals and is capped by wall time, never pure
iteration count; spin is disabled automatically when `GOMAXPROCS==1` or the cgroup CPU
quota is below a threshold — a spinner must never be able to starve the producer,
dispatcher, heartbeat, or GC of the only runnable P. The scheduler-regime test matrix
(run during both the initial performance-spike benchmarking and the later hardening
work) exercises exactly these regimes.

Eventfds are created by the host, passed with the region fd. Under sustained load both
sides stay awake and syscall count approaches zero; when idle, CPU approaches zero.
Futex-on-shared-memory is a v2 experiment only if the performance-spike and
SHM-transport benchmarks show eventfd overhead matters.

## 16. Code Generation

Custom `protoc-gen-go-styx`, consuming ordinary gRPC-compatible `service` definitions
(works under the device gateway's existing buf pipeline). Emits:

- `New<Service>Client(conn styx.ClientConn) <Service>Client` — same method shapes as
  gRPC: `(ctx, req) (resp, error)` for unary, and gRPC-style stream objects
  (`Send`/`Recv`/`CloseSend`) for streaming methods.
- `Register<Service>Server(srv *styx.PluginServer, impl <Service>Server)` with a
  generated dispatch table (service ID + method ID = FNV-64 of full names, collision-
  checked at generation and handshake time).

No gRPC dependency in generated code. Runtime protobuf reflection fallback is a
non-goal for v1 (codegen only).

## 17. Error Model

Three disjoint classes, distinguishable with `errors.As`/`errors.Is`:

- **Application errors** — returned by the remote handler; carried as status payloads;
  wrap cleanly (`styx.Status{Code, Message, Details}`).
- **Plugin-fault errors** — `PluginCrashError{ExitStatus, Reason}`, `PluginPanicError`
  (a plugin fault: the process is tainted and restarted), `ErrPluginUnavailable`
  (restarting), `ErrDrained`, and `ErrOutcomeUnknown` (the handler may or may not have
  executed). Retryable-ness is explicit (`styx.IsRetryable(err)`):
  `ErrOutcomeUnknown` is never automatically retryable; crash-before-dispatch is.
- **Framework errors** — `ErrIncompatible`, `ErrDeadlineExceeded`, `ErrCanceled`,
  `ErrBackpressure`, `ErrRequestDeclined`, `ErrPoisoned`, `ErrServiceNotFound`,
  `ErrMethodNotFound`.

This taxonomy is what lets the device gateway map "critical device fault → fast shutdown" versus
"transient → retry/restart" without string matching.

## 18. Process Supervision

Built-in supervisor per plugin: control-plane heartbeats, `SIGCHLD`/wait (death),
restart policy (max restarts, exponential backoff with jitter, reset window), crash
reason capture (exit status, last stderr lines, poison cause), stdout/stderr capture
with pluggable sinks, and a structured event stream
(`Starting/Ready/Unhealthy/Crashed/Restarting/GaveUp`). "GaveUp" is a terminal event the
host must handle — Styx never silently stops supervising.

**Heartbeat is a progress contract, not a liveness ping.** A control-loop that answers
heartbeats while the data-plane consumer is deadlocked must not count as healthy. Each
heartbeat reply carries data-plane progress counters (descriptors consumed/produced,
in-flight count, arena occupancy — mirrored from the sync page) **and active-handler
leases**: for each executing handler, its call ID, start time, and lease renewal (the
dispatcher renews leases while a handler is genuinely running). The classifier
distinguishes queued transport work from executing handlers:

- **wedged** — evaluated per component, so one healthy handler cannot mask an unrelated
  stall: (a) *transport-wedged* — unconsumed descriptors exist and the consume counter
  is unchanged across the wedge window; handler leases are irrelevant, because a live
  handler does not excuse a stalled ring consumer; or (b) *dispatch-wedged* — responses
  are owed for calls that have NO renewing active-handler lease, and the produce
  counter is unchanged across the window. Either → `Unhealthy` → restart. A
  legitimately long-running handler is NOT wedged: an executing call with a renewing
  lease is governed by its own deadline (every call has one — a configurable default
  applies when the caller sets none).
- **overloaded** — counters advancing, occupancy high → backpressure territory; never a
  restart trigger, so load spikes cannot cause restart storms.
- **draining** — expected stall during hot-reload/shutdown phases; progress checks
  suspended for the phase's own deadline.

Defaults (fixed today, not exposed on `PluginSpec`): 1 s interval, 3 missed
heartbeats, or 5 s of no-transport-progress-with-queued-work → unhealthy. Restart
storms are damped by the existing backoff policy.

**Sinks and subscribers can be slow; supervision must not care.** Child stdout/stderr
pipes are always drained by dedicated goroutines into bounded buffers with per-line
size caps and explicit drop accounting — a blocked sink drops output (counted) rather
than filling the pipe and blocking the plugin inside a write, and the capture goroutine
itself is panic-isolated: a panicking user-supplied sink or malformed plugin output must
never crash the host (a recurring `arloliu/go-plugin` fork pain point, §27 Open
Question 2). `host.Events()` delivery
is per-subscriber buffered and non-blocking: informational events are
drop-oldest-with-counter; lifecycle-critical events (`Unhealthy`, `Crashed`, `GaveUp`,
`Poisoned`) fill a bounded backlog sized to one whole failure incident's worth of
critical events per publisher, rather than a single coalescing slot, so a verdict and
the outcome that followed it always arrive together, in order, instead of either one
silently vanishing on its own. `internal/supervisor.EventBus` has exactly one
publisher (the plugin's own supervisor), so its backlog holds one incident's worth;
`Host.Events()` fans in every configured plugin's own `EventBus`, so its backlog holds
one incident's worth *per plugin* — only an older, already-superseded incident's
critical events (that plugin's own earlier one, or a different plugin's) can still be
evicted to make room for a newer one. An unread subscription can never stall the
supervisor. Observability hooks run on non-hot-path goroutines, panic-isolated, with
documented ordering and drop policy.

## 19. Flow Control and Backpressure

Everything bounded: ring capacity, arena size, per-plugin max in-flight requests,
per-stream message credits and per-connection stream cap, the writer-goroutine
submission queue, and the population of blocked waiters. When full, callers either
block until space or ctx deadline (default) or receive `ErrBackpressure` immediately
(option). **Admission control runs before any resource is allocated:** a call is
admitted (or rejected/queued, bounded) before it takes a request-table slot, arena
slab, or ring descriptor, and the capacity invariant `max_inflight ≤ f(ring capacity,
arena capacity / max payload)` is validated at startup — configurations that can admit
more calls than the region can represent refuse to load. Waiting callers block on
their own call context — never holding the writer's lock — so cancellation and
short-deadline calls are never head-of-line blocked by a stalled sender, for a call that
marshals into a wire buffer first; lifecycle frames (`CANCEL`, `STREAM_ACK` — the
lifecycle lane) have a reserved descriptor budget so data traffic can't starve them.
A call that instead fills the transport's own send buffer directly (a fill send) is
cancellable only up to the point the writer goroutine claims it: arena backpressure is
still checked, and still abandonable, before that claim. Once claimed, the frame is
committed to publication, and a caller then waiting on a full ring window blocks until
the window opens or the transport shuts down, no longer honoring its own context — a
genuine head-of-line stall this one send mode accepts. The trade is deliberate: it is
what lets a fill-send caller always learn the frame's true fate, published or provably
not, where an abandoning wire-buffer sender can only ever be told `ErrOutcomeUnknown`.
Fairness in v1 is FIFO admission with per-stream
credits; per-service quotas are an explicit extension point. Large messages: payloads
above a negotiated threshold are rejected with a typed error in v1 (chunking/bulk
transfer is roadmap).
Slow-consumer handling is therefore deterministic and local — no unbounded queues, no
OOM-by-queue.

## 20. Security Model

Trust model stated plainly: **Styx assumes host and plugins run as the same user on the
same machine and are mutually non-malicious once launched; it defends against bugs, not
adversaries.** Concretely provided: anonymous memfd regions (no filesystem exposure),
fd passing only over the private socketpair, per-launch handshake nonce, optional binary
SHA-256 pinning, environment sanitization on spawn, and full validation of all
SHM-derived values (a compromised plugin must not be able to crash the host through the
protocol). Explicitly out of scope for v1: seccomp/namespace/cgroup sandboxing (roadmap
integration points), cross-user isolation, plugin authentication beyond binary identity.

## 21. Observability

`observe` package defines small interfaces (`MetricsSink`, `Logger`, `TraceInjector`)
with no-op defaults and no vendored stacks. Built-in instrumentation points: RPC latency
histograms, ring depth/occupancy, arena utilization, backpressure events, timeouts,
cancellations, restarts, heartbeat misses, bytes moved, wakeup syscalls per second.
Trace context rides the descriptor's reserved trace field (W3C trace-context binary
form) so the device gateway can add OTel later without wire changes.

## 22. Benchmark Plan

Baselines: direct in-process call, net/rpc over UDS, gRPC over UDS, gRPC over TCP
loopback, hashicorp/go-plugin (gRPC mode), raw UDS ping-pong.
Dimensions: unary 64 B / 4 KiB / 1 MiB payloads; 1/8/64/512 concurrent callers;
p50/p95/p99/p999 latency; throughput; allocs/op; CPU per op; wakeup syscalls/op;
crash-to-restored-service latency. Benchmarks live in `bench/`, run in CI on dedicated
runners, and regressions gate merges. The early two-process spike prototype runs this
same suite before framework code is written.

## 23. Testing Strategy

- **Unsafe core (ring/arena):** property-based tests of invariants, deterministic
  interleaving tests, fuzzing of descriptor/index/offset values, `-race` for in-process
  concurrency. Documented honestly: Go's race detector cannot see cross-process SHM
  races — that risk is contained by SPSC design (one writer per field) and saturated by
  the tests below.
- **Differential testing:** identical RPC workloads through `uds` and `shm` transports
  must produce identical results; divergence fails CI.
- **Deterministic crash-window matrix (failpoints), not just randomized chaos:** the
  correctness-defining windows are a few instructions wide (between payload write and
  descriptor write, descriptor write and tail publish, tail publish and wakeup arming,
  slab release, fd transfer, ready-ack, unmap) — random SIGKILL may never hit them.
  Instrumented failpoints enumerate every protocol transition; the harness kills or
  stalls at each one and asserts: bounded completion of all outstanding calls, exact
  fd/mapping counts after recovery, allocator invariants hold, and no response is ever
  delivered to the wrong call. Randomized chaos (SIGKILL, byte corruption, SIGSTOP
  wedging, arena starvation) runs on top of, not instead of, the matrix.
- **Scheduler-regime matrix:** the full suite runs under `GOMAXPROCS=1`, restrictive
  cgroup CPU quotas, forced GC pressure, and asynchronous preemption churn — the
  regimes where spin/park bugs and priority starvation actually appear.
- **Cross-process integration tests** for lifecycle: handshake, version mismatch,
  hot-reload with state handoff, drain semantics, deadline/cancel propagation.
- **Soak:** multi-hour high-concurrency runs with periodic random restarts; leak checks
  on fds, memory, goroutines.

## 24. MVP Scope (v1)

In: unary RPC; streaming RPC (gRPC-shaped stubs over plain ring messages); Linux
amd64 (arm64 CI-built, best-effort); protobuf codec; `protoc-gen-go-styx`; control-plane
handshake with three-axis versioning + heartbeat; SHM transport (single region,
SPSC ring pair, slab arena, eventfd hybrid wakeup); UDS fallback transport; supervisor
with restart policy; hot-reload with state handoff; deadlines/cancellation; full error
taxonomy; observability hooks; benchmark + chaos + differential test suites.

Ordering within v1: unary ships first (built on UDS, then validated over shared
memory); streaming lands after unary is differential-tested, since it reuses the same
descriptor path.

Out (deliberately): non-Go plugins, sharded rings, futex wakeups, bulk/zero-copy handle
transfer, sandboxing integrations, Windows/macOS production support, runtime protobuf
reflection, stream-aware transport features (windows/priorities).

## 25. Milestones

- **Spike & proof.** Two-process prototype: ring + arena + eventfd hybrid (with
  the race-free wakeup arming protocol — the race-free version is what gets measured) vs.
  gRPC-over-UDS on the benchmark suite. **Gate — both absolute and relative, on target
  hardware:** small-payload unary p50 ≤ 3 µs and p99 ≤ 10 µs warm; idle-to-active
  (park/wake) p99 ≤ 25 µs; ≥10× vs. gRPC-over-UDS at p50; no pathological tails under
  concurrency, GC churn, or a 2-CPU cgroup quota. Numbers may be recalibrated once with
  recorded justification; failing the recalibrated gate kills or reshapes the SHM
  premise before framework code exists.
- **Framework on UDS.** Public API, codegen, RPC runtime, handshake, lifecycle —
  correctness end-to-end with zero SHM risk.
- **SHM transport.** Region, rings, arena, wakeups, poisoning, crash recovery;
  differential tests vs. the UDS-based framework. **Gated on `docs/specs/shm-abi.md`
  existing first;** the failpoint crash-window matrix is part of this phase's
  definition of done.
- **Enterprise features.** Streaming RPC — **gated on `docs/specs/
  stream-protocol.md`** — over the proven descriptor path; supervisor policies,
  hot-reload/state handoff, observability, error taxonomy hardening.
- **Hardening.** Fuzz, chaos, soak, CI benchmark gates, docs and examples.
- **Device-gateway pilot.** Implement the device gateway's device-plugin contract
  on Styx; migrate one low-risk device type (e.g. `example_device`) behind config.

## 26. Risks

1. **Go scheduler latency variance** may erode the SHM win under load — measured
   directly during the initial performance-spike benchmarking; the gate exists for this
   reason.
2. **Cross-process memory-ordering bugs** — the worst bug class here; contained by SPSC
   single-writer design, a tiny unsafe core, and differential/chaos testing.
3. **Two-transport maintenance cost** — accepted deliberately: fallback story + test
   oracle are worth it.
4. **Codegen surface drift** vs. protobuf/buf ecosystem evolution — pin toolchain
   versions, follow the device gateway's existing buf pipeline conventions.

## 27. Open Questions

1. Final module path/org — resolved: `github.com/arloliu/styx` (Arlo, 2026-07-18;
   matches the personal-namespace convention of the other reference projects,
   e.g. `zapwire`, `mebo`, `parti`, `go-secs`, `helix`, `otx`).
2. Catalog what the `arloliu/go-plugin` fork changed vs. upstream — resolved (Arlo,
   2026-07-18). The fork diverges from `hashicorp/go-plugin` at commit `d662936`
   (40 commits ahead as of this review; full history at
   `/home/arlo/projects/go-plugin`, `git log d662936..HEAD`). Commit messages are
   unusually self-documenting (failure mode + fix + regression test each), and the
   recurring "mission-critical"/"fleet scale"/equipment language strongly suggests
   these are real production incidents from a device-gateway-shaped consumer, not
   speculative hardening — though no commit or doc names the specific consumer, so
   that link is inferred, not confirmed.
   Disposition of each pain point against this design:
   - **Folded in as new requirements** (not previously explicit in this doc):
     process-group-scoped kill instead of single-PID (§5, §9); configurable
     shutdown/teardown grace period instead of hard-coded (§9); panic isolation for
     internal goroutines that touch plugin-controlled or user-supplied data — log/stdout
     capture, not just observability hooks (§5, §18).
   - **Already satisfied by the existing design, no change needed:** no silent
     fallback on negotiation failure (§10's typed `ErrIncompatible` vs. the fork's fix
     for a `sync.Once`-swallowed mux-init error causing silent fallback); guarding
     against attaching to a stale/wrong process (§10's per-launch nonce vs. the fork's
     `os.FindProcess(0)` reattach bug); structured internal diagnostics instead of raw
     `log.Printf` (§21's `observe.Logger` seam vs. the fork's `SetInternalLogger`);
     configurable liveness deadlines (§18's heartbeat-as-progress-contract already
     exceeds the fork's simple configurable `Ping` timeout).
   - **Not applicable:** fork-only housekeeping (module rename, dependency bumps,
     lint/CI setup) and gRPC-broker-specific bugs (mux timeouts, broker map/stream
     leaks) — Styx has no gRPC broker or mux to inherit that bug class.
3. eventfd vs. futex numbers on target device-gateway hardware — decided by
   performance-spike and SHM-transport benchmark data, not opinion.
4. Whether the device gateway's plugin *package fetching* (its package store/fetcher)
   belongs in Styx or stays host-side — resolved: host-side (Arlo, 2026-07-18).
   `PluginSpec` takes a path and optional SHA-256 hash only; fetching/caching plugin
   binaries stays the host application's job (the device gateway's own package store),
   keeping Styx's dependency surface
   minimal and consistent with the non-goals in §4.
5. Go version floor — resolved: `go 1.26.0` (Arlo, 2026-07-17; supersedes the kickoff's 1.27).
