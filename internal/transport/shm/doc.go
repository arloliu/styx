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
// writer returns to its lifecycle-first wait, resuming that intent on the next
// lifecycle intent or at shutdown. A resume driven by the consumer itself freeing
// space (signalRetry) is a deliberately-unwired seam: no production caller signals
// it yet, because the cross-process consumer→producer "space-available" wake is
// not specified for this milestone (shm-abi.md §11/§12 define only
// producer→consumer wakes); a test drives it directly. The writer never busy-waits
// and never blocks the lifecycle lane on data-lane progress.
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
// # The assembled transport
//
// Attach wires this writer, plus a SpinWaiter-driven inbound reader, onto a real
// region: it opens the memfd, validates the capacity invariant against the
// region's actual geometry before allocating any writer or arena state
// (shm-abi.md §18), then carves the per-direction rings, arenas, and sync-page
// words (§1/§3) and returns a Transport that satisfies transport.Transport. Send
// hands a frame to the writer on the lane its kind selects; Recv waits for
// inbound work, discards stale-generation descriptors (§15), verifies an
// optional CRC32C trailer (§5), copies the payload out before releasing the slot
// (§9), and returns the decoded frame. Close performs teardown step 4 (munmap)
// exactly once; steps 1-3 (admission stop, waiter wake, goroutine join) are the
// caller's, and the eventfds are the caller's to close.
//
// # Head-gated reclaim and its idle-stuck limitation
//
// The writer frees a slab once the consumer's ring head has passed the
// descriptor that referenced it (shm-abi.md §6): on every publish, and again
// before each allocation, it reclaims every slab below the current head. Under
// continuous traffic this keeps the arena from leaking, including when a burst
// of no-slab frames (a CANCEL storm, empty-payload data) reuses an earlier data
// frame's ring slot.
//
// Reclaim runs only while the writer is making progress; it cannot run while the
// writer is parked. So a writer already stuck on arena exhaustion does not
// recover from its draining consumer alone: it resumes on its next lifecycle
// intent or at shutdown — both re-drive the set-aside intent through
// place → build → stampPayload → reclaim → alloc — never from pure data traffic,
// since a stuck writer stops pulling data to stay within its queue bound, so
// further data sends do not wake it. A workload of pure data frames with no
// lifecycle traffic, under a consumer that drains but sends no cancels, is the
// uncovered corner: nothing wakes the stuck writer until the cross-process
// consumer→producer "space-available" wake is wired, which is not specified for
// this milestone (shm-abi.md §11/§12 define only producer→consumer wakes) and is
// left to a later load/recovery task.
//
// # Conformance faults poison the region
//
// A received descriptor that violates the frame contract — ring depth over
// capacity, an unassigned kind, a flag outside allowed_flags, a descriptor-only
// frame carrying payload state, a payload span outside the arena, or a CRC32C
// mismatch — is detected, surfaced as a typed error (errRingCorrupt,
// errBadFrame, errChecksum) to the Recv caller, and actuates the §16
// poison(cause) helper (CAS the mapped cause, set shutdown, wake both
// directions) so the peer stops too, not just this call; an illegal
// park-state value observed by the producer signal does the same as
// errBadSync. Recv does not deliver the offending frame. A later Send/Recv
// call on either side observes ErrPoisoned. Generation mismatch is a discard
// (counted), never a poison (§15): PoisonFlag and the fault->cause mapping
// live in poison.go, the generation-recovery helpers and the discard-
// escalation policy in recovery.go.
package shm
