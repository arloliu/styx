// Package shm builds the single writer goroutine and the two-lane intent queue
// that sit on top of one already-attached descriptor ring and payload arena
// (internal/ring, internal/arena). It is the producer choke point of one
// direction of the shared-memory data plane: every concurrent caller hands the
// writer a fully-formed, immutable send request, and the writer alone allocates
// payload slabs, stamps descriptors, and publishes the ring tail. Nothing else
// touches the ring or arena, which is what keeps the single-producer discipline
// the ring's memory-ordering story depends on (design docs §12).
//
// # One writer, two lanes
//
// Single-producer/single-consumer rings only stay correct while exactly one task
// writes each ring (design §12). Concurrent callers therefore never write the
// ring directly; they submit intents through two bounded in-process queues that a
// single writer goroutine drains. The lanes exist to protect liveness:
//
//   - The data lane carries the payload-bearing unary kinds. Data admission is
//     bounded; a full data-submission queue is backpressure, resolved either by
//     blocking the caller until space frees or by returning ErrBackpressure
//     immediately, per the writer's admission mode (design §19).
//   - The lifecycle lane carries the descriptor-only CANCEL. A CANCEL must make
//     progress regardless of data traffic: losing one is worse than briefly
//     blocking a caller, so the lifecycle lane never returns ErrBackpressure and
//     the capacity invariant reserves ring slots reachable only by lifecycle
//     frames (shm-abi.md §18). The writer gives it strict priority: it drains
//     every pending lifecycle intent before it considers, or resumes, any data
//     work (design §12).
//
// # Backpressure never blocks lifecycle
//
// ring.Push and arena.Alloc are non-blocking and return typed errors, so the
// hazard is not a blocked syscall inside the writer but the writer's own policy
// when a data emit cannot be placed (a full ring window or an exhausted arena
// class). Both are reachable only under a slow or wedged consumer, because
// admission sizes the data lane within the ring's data budget (C - R,
// shm-abi.md §18); recovering a wedged consumer is wedge-detection's job, not
// this writer's. The writer must never spin on that backpressure while a CANCEL
// waits (design §12), so a data intent that cannot be placed is set aside and the
// writer returns to its lifecycle-first wait, retrying the set-aside intent when a
// resume signal reports the consumer freed a ring slot or slab. That signal is a
// seam here (see signalRetry); the assembly layer wires it to the real
// consumer→producer wakeup. It never busy-waits and never blocks the lifecycle
// lane on data-lane progress.
//
// # Completion protocol
//
// submit queues an intent and then waits on the intent's completion channel or
// the caller's context, whichever resolves first; a canceled or deadline-passed
// caller returns its own context error and never holds any writer lock, so a
// stalled sender cannot head-of-line block cancellations or short-deadline calls
// (design §19). The completion channel is buffered so the writer's single result
// send never blocks, even for an intent whose caller already returned on a
// context cancel — an abandoned intent may still be emitted, which is harmless
// because nobody waits on it and the writer never blocks reporting it.
//
// # Scope
//
// This package builds only the writer mechanism over an already-attached
// ring/arena pair. Startup capacity-invariant validation and the C - R
// arithmetic, real region attachment, the eventfd wakeup that signals a parked
// consumer after a push, poison-on-corruption, generation-staleness discard, and
// the Transport.Send/Recv surface are assembled around it elsewhere; the depths
// handed to the writer are a trusted-caller contract.
package shm
