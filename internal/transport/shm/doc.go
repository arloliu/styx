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
// lifecycle intent, on a bounded backoff timer, on a peer-progress signal, or at
// shutdown. The peer-progress signal (notePeerProgress) is raised by this side's
// own receive path: any inbound frame is a hint that the peer may have consumed
// something and moved its head on this side's outbound ring, so a set-aside
// intent is worth retrying now rather than at the next timer fire. It is only a
// hint — an inbound frame need not be caused by anything this side sent (a
// fresh request, a stream chunk, a heartbeat are all unsolicited), and even a
// caused frame does not confirm the freed slab lands in the size class this
// writer is waiting on — so a wrong guess costs one failed retry. It is also a
// local signal only — the cross-process consumer→producer "space-available"
// wake is not specified (shm-abi.md §11/§12 define producer→consumer wakes
// only), so a stall neither side can report through an inbound frame still
// resolves on the timer. The writer never busy-waits and never blocks the
// lifecycle lane on data-lane progress.
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
// A data intent carries its payload one of two ways, and that changes what
// abandoning it means. A wire intent snapshots immutable bytes at submit, which
// is why it may still be emitted after its caller gave up. A fill intent carries
// a size and a callback that marshals into the slab on the writer goroutine;
// that callback reads a message its caller still owns, so it must never run
// after the caller has resumed. An atomic handshake on the intent decides which
// side gets it: the writer claims it right before it fills, and a cancelling
// caller either wins the claim first — which is proof the frame was never
// published, so it returns its context error — or loses and waits out the
// writer's report, returning that report rather than a context error. See
// intent.go's fill-state constants.
//
// # The assembled transport
//
// Attach wires this writer, plus a SpinWaiter-driven inbound reader, onto a real
// region: it opens the memfd, validates the capacity invariant against the
// region's actual geometry before allocating any writer or arena state
// (shm-abi.md §18), then carves the per-direction rings, arenas, and sync-page
// words (§1/§3) and returns a Transport that satisfies transport.Transport. Send
// hands a frame to the writer on the lane its kind selects; Recv waits for
// inbound work, discards stale-generation descriptors (§15), consumes the
// payload before releasing the slot, and returns the decoded frame.
//
// §9 permits consuming either by copying the bytes out or by decoding them in
// place, and this transport does both. Recv always copies, because the frame it
// returns outlives the slab. RecvViewConsume hands the frame to a callback whose
// Payload aliases the arena slab, advancing the head only after that callback
// returns — the borrow that saves the copy, bounded by the one construct that can
// bound it. A frame carrying the per-frame CRC32C_PRESENT flag (§5) is copied out
// and verified over that private copy either way, which is what makes the check
// end-to-end. Close performs teardown step 4 (munmap)
// exactly once; steps 1-3 (admission stop, waiter wake, goroutine join) are the
// caller's, and the eventfds are the caller's to close.
//
// # Head-gated reclaim and the exhausted writer's retry
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
// recover from its draining consumer directly: something must re-drive the
// set-aside intent through place → build → stampPayload → reclaim → alloc. The
// backoff timer always does, on its own, with no help from either peer; a
// lifecycle intent, an inbound frame's peer-progress signal, or shutdown each do
// it sooner. A workload of pure data frames with no lifecycle traffic, under a
// consumer that drains but sends no cancels, therefore degrades — every wait
// costs at most the timer's 5 millisecond cap — rather than wedging. The
// cross-process consumer→producer "space-available" wake would remove even that
// wait, but it is not specified (shm-abi.md §11/§12 define only
// producer→consumer wakes) and is not needed for liveness.
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
//
// # A consume fault is contained; an unbroken run of them is not
//
// A frame this side could not consume for a reason of its own — its decode
// panicked, or a consume callback declined it — is not a peer fault. Recv
// discards that one frame, advances past it, fails the call the descriptor
// names, and leaves the region healthy and serving (shm-abi.md §9). Nothing an
// individual frame does escalates.
//
// The reason it cannot end there is that §9 hands the peer-versus-consumer
// attribution to the consume callback, and this transport cannot check its work:
// it proves peer fault for every class it can decide itself (descriptor
// validation, CRC32C, status-body decode, all before the callback runs), and what
// remains is an opaque body whose schema it does not hold. A callback that
// declines what was really the peer publishing garbage therefore keeps a corrupt
// region alive.
//
// So the fault stream is escalated on its shape: once faults arrive back to back
// with no successful delivery between them, and the run reaches
// EscalationConfig.ConsumeFaultRunThreshold, the region is poisoned with
// PoisonGeneric. Any single delivered frame resets the run to zero, which is what
// makes the rule mean something — a consumer that is merely busy or backpressured
// keeps succeeding between its declines and never accumulates a run, while a
// region whose every frame is unusable accumulates one without bound. A rate of
// faults would not distinguish those two, because a fault fires once per inbound
// frame and both cases produce them at whatever rate the peer publishes.
//
// The escalation is deliberately conservative, because poisoning is unrepairable
// and bilateral: it tears down both sides and fails every call in flight. It can
// be tuned, or switched off for one side, through
// EscalationConfig.ConsumeFaultRunThreshold and ConsumeFaultEscalationDisabled,
// which leaves the faults to Transport.ConsumeFaults and the supervisor — the
// owner §16 names for escalation policy.
//
// Two properties of that control are easy to over-read, and both are set out on
// the constants themselves. The threshold counts frames, so the rule means the
// same event everywhere, but the amount of stall time it tolerates shrinks as the
// link gets faster (DefaultConsumeFaultRunThreshold). And each side runs its own
// guard over its own inbound stream with no negotiation between them, so
// disabling one side does not keep the region alive when the other side's guard
// fires (ConsumeFaultEscalationDisabled).
//
// # Telling this teardown apart from any other
//
// The escalation records PoisonGeneric, because this side genuinely cannot tell a
// peer publishing garbage from its own consumer having stopped — which is the
// premise of the whole rule, and why naming the peer would be a false report. The
// cost is that a region torn down this way reports "shm: region poisoned:
// generic", the same reason string an ordinary control-plane teardown produces.
//
// Transport.ConsumeFaults is the evidence, and it is per side, so read it that
// way. The side whose run fired has a count of at least its configured
// ConsumeFaultRunThreshold — below that the escalation cannot have fired, so a
// merely nonzero count proves nothing on its own. The side that did NOT escalate
// is torn down by the same bilateral poison while its own count stays wherever it
// was, commonly zero. So a zero count rules out THIS side having escalated and
// rules out nothing whatever about the peer, which is the case worth suspecting
// precisely because disabling one side leaves the other armed.
//
// The count is reported to an operator's metrics sink as observe.MetricConsumeFault,
// by the periodic reporter on both the host and the plugin, so this is readable
// without access to the Transport itself.
package shm
