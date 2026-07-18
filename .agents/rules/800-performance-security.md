# 800 - Performance and Security

Apply to hot paths (`internal/ring`, `internal/arena`, `internal/transport`,
`internal/rpcruntime`, the codec path), cross-process/external input, the
handshake, or anything touching credentials/identity. Confirm the code is
actually hot before optimizing.

## Performance
- Pre-size slices/maps (`make([]T, 0, expectedCap)`, `make(map[K]V, expectedSize)`);
  avoid `append` growth in tight loops with predictable size.
- Keep hot functions small for inlining. Pass small structs by value;
  pointers only when mutating or the struct is large.
- Avoid unneeded interface indirection in the hottest paths (ring push/pop,
  arena alloc/free, codec encode/decode) — the design's whole premise is a
  low-single-digit-microsecond round trip, and indirection that
  would be invisible elsewhere is a measurable tax here.
- Profile with `pprof` before optimizing — never guess bottlenecks.
- **Perf claims need benchmark evidence, not intuition.** Any change to a hot
  path must cite a `bench/`-style before/after comparison (see the design
  document's benchmark plan: unary 64B/4KiB/1MiB payloads, 1/8/64/512 concurrent callers,
  p50/p95/p99/p999, allocs/op, wakeup syscalls/op). "Should be faster" is not
  a review-passing statement.
- Concurrency: see [200](200-coding-standards.md#go-style) (`sync/atomic` vs
  `sync.Mutex`) — matters more under contention. `internal/ring` is SPSC by
  design: one writer per field, one reader — don't casually widen
  that to multi-producer without redesigning the memory-ordering story.
- No perf-sensitive third-party libraries are adopted yet — see
  [100](100-project-map.md#dependency-policy) before reaching for one; the
  unsafe core in particular should have close to zero dependencies.

## Security
Styx's stated trust model: **host and plugins run as the same user
on the same machine and are mutually non-malicious once launched; Styx
defends against bugs, not adversaries.** That does not relax input handling —
it changes what the input boundary is.

- **Never trust the other side of the wall** (project philosophy).
  Every value read from shared memory or the control-plane socket — offsets,
  lengths, generation counters, descriptor fields — is potentially corrupt
  (bug, stale write, wrong generation) and must be bound-checked against the
  sealed region before use. On violation: poison the region and let the
  supervisor restart, don't attempt in-place repair.
- The control-plane handshake carries the actual security-relevant surface:
  per-launch nonce (guards against attaching to a stale/foreign process),
  optional binary SHA-256 pinning, environment sanitization on spawn. Treat
  changes here as security-sensitive even though there's no network exposure.
- memfd regions are anonymous (no filesystem path) and sealed
  (`F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL`) — don't introduce a named
  `/dev/shm` path or a mutable-size region without understanding why the
  design deliberately avoided both.
- Never log or commit secrets. No credential-loading library is adopted yet
  in this repo — if one becomes necessary, raise it rather than hand-rolling
  `os.Getenv`/YAML parsing for secrets.
- Explicitly out of scope for v1 (don't build speculatively): seccomp/
  namespace/cgroup sandboxing, cross-user isolation, plugin authentication
  beyond binary identity.
- **Internal goroutines that touch plugin-controlled or user-supplied data
  must be panic-isolated** — a malformed line from a plugin's stdout/stderr,
  or a panicking user-supplied `supervisor.Sink`, must never crash the host.
  This isn't speculative hardening: the `arloliu/go-plugin` fork hit exactly
  this failure mode in production. `internal/supervisor/capture.go`'s
  `deliverLoop` recovers a panicking `sink.WriteLine` and counts it
  (`StdioCapture.PanicCount`) rather than letting it escape — match this
  pattern for any other goroutine that calls into plugin- or user-supplied
  code.
