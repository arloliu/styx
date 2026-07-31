package testutil

import (
	"bytes"
	"runtime/pprof"
	"testing"
	"time"
)

// Defaults for the settle loop RequireNoGoroutineLeak and RequireNoGoroutineOrFDLeak
// register. tests/soak's own settleGoroutines (tests/soak/soak_test.go) is the same
// consecutive-samples-within-a-bounded-window pattern; these are the general-purpose
// numbers for a single test that shares its binary's process with the Go test
// framework's own bookkeeping and, potentially, other tests still unwinding.
const (
	// goroutineSettleSlack is how far above baseline the goroutine count may sit and
	// still count as settled. It is 0: every goroutine a test starts, directly or
	// through the machinery it drives, must be gone by the time that test's own
	// teardown has finished, so a SINGLE leaked goroutine fails the check — the
	// smallest leak there is, and the size a per-Host or per-subscription leak
	// arrives in. There is no tolerated per-test goroutine: styx.NewHost's own
	// subscription to its top-level event bus (the one Events() hands out) is ended
	// by Host.Stop, so its forwarder goroutine belongs to the Host and not to the
	// process. Slack 0 was verified stable (0 failures) across the full instrumented
	// suite at -race -count=3.
	//
	// Raising this back above 0 would blind the check to a leak of exactly that
	// size: a goroutine leaked once per test, never growing, settles inside the band
	// and passes. Fix the leak instead; a slack of 1 is one whole per-Host leak's
	// worth of cover.
	goroutineSettleSlack = 0

	// goroutineSettleSamples is how many consecutive polls must land at or below
	// baseline+slack before the count is accepted as settled. A sustained leak — a
	// count that stays above baseline+slack for the whole settleMaxWait window — can
	// never assemble goroutineSettleSamples consecutive passing samples, so it always
	// runs out the window and fails. Consecutive samples, rather than a single final
	// one, are what separate a goroutine still unwinding right after teardown from
	// one that is not going to exit at all.
	goroutineSettleSamples = 5

	// settlePoll is the interval between samples, shared by the goroutine and fd
	// settle loops. Short enough that a clean test's cleanup — the common case —
	// finishes in a few hundred milliseconds: goroutineSettleSamples consecutive
	// passes back to back cost (goroutineSettleSamples-1)*settlePoll in the best case.
	settlePoll = 100 * time.Millisecond

	// settleMaxWait, divided by settlePoll, is the FIXED ITERATION COUNT the settle
	// loops run for — not a wall-clock deadline. Each iteration sleeps settlePoll
	// between samples, so on an idle machine total elapsed time tracks close to
	// settleMaxWait, but under load a delayed goroutine scheduler can stretch any
	// given sleep past its nominal duration, and the loop runs its full iteration
	// count regardless — so actual wall-clock time can exceed settleMaxWait. That is
	// the safe direction (a slow, still-settling process gets more real time to
	// finish, not less), so it never turns a benign delay into a false failure; it
	// only means settleMaxWait is a target duration under ideal scheduling, not an
	// enforced ceiling.
	settleMaxWait = 20 * time.Second
)

// RequireNoGoroutineLeak registers a t.Cleanup that fails tb unless the process's
// goroutine count has returned to at or below baseline+goroutineSettleSlack —
// baseline captured right now, at call time — by the time the test's own cleanup
// has finished.
//
// Call it FIRST in the test, before starting whatever it will tear down (a host, a
// process, a supervisor run) AND before subscribing to anything that spawns its own
// background goroutine (e.g. an event bus's Subscribe): t.Cleanup runs in
// reverse-registration order, so the cleanup registered first runs last, after the
// teardown this check depends on; and baselining before such a subscription means
// its goroutine is counted, not silently absorbed into the baseline where a bug that
// leaves it running would go unseen.
//
// LIMITS — what this does NOT detect: a leaked child process (it counts this
// process's own goroutines, nothing about a plugin process that outlives its
// supervisor); a leaked shared-memory region mapping; retained/growing heap; a
// goroutine that happens to exit only because an earlier-registered cleanup (e.g.
// Host.Stop) already ran before this one — this check proves nothing leaked past
// THAT teardown, not that nothing misbehaved before it; and a goroutine that
// leaked but is offset by an unrelated one exiting late, since this is a net
// count, not a per-goroutine identity check. What it DOES see, at
// goroutineSettleSlack of 0, is a single goroutine that outlives the test that
// started it — including one leaked once per host, per subscription, or per call
// without ever growing further.
//
// The registered cleanup polls at a fixed cadence and requires goroutineSettleSamples
// consecutive samples at or below baseline+slack before treating the count as
// settled; it fails only once that never happens within the settleMaxWait iteration
// budget. A transient spike immediately after teardown — a goroutine still unwinding,
// not one that leaked — therefore never trips it, while a count that stays above
// baseline+slack for the whole window fails every time: there is no way to assemble
// goroutineSettleSamples consecutive passes without the count actually having fallen
// back into the tolerated band. See RequireNoGoroutineOrFDLeak for a test that also
// drives a real plugin process and wants an fd-count baseline alongside this one.
func RequireNoGoroutineLeak(tb testing.TB) {
	tb.Helper()

	baseline := CountGoroutines()
	tb.Cleanup(func() {
		tb.Helper()
		if !settleGoroutines(tb, baseline) {
			tb.FailNow()
		}
	})
}

// RequireNoGoroutineOrFDLeak is RequireNoGoroutineLeak plus an open-fd baseline, for
// a test that drives a real plugin process lifecycle — spawn, crash, restart,
// teardown — where the host process's own descriptors (control sockets, region
// memfds, eventfds) are exactly what such a defect leaves open. Call it FIRST, for
// the same reverse-registration and pre-subscription reasons RequireNoGoroutineLeak
// documents.
//
// The name is deliberately narrow, not RequireNoProcessLeak or similar: it checks
// exactly two counters in THIS process, nothing about the plugin process(es) it
// spawns. In particular it does NOT detect an orphaned or leaked CHILD process, a
// leaked shared-memory region mapping, retained/growing heap, or a leak whose fd and
// goroutine footprint happens to cancel out against something else closing early —
// the fd dimension below is a net count, not a per-descriptor identity check, so a
// leaked fd offset by an unrelated extra close reads as zero net change. See
// RequireNoGoroutineLeak's LIMITS paragraph for the rest, which applies here too.
//
// Both dimensions are always checked and reported even when the goroutine dimension
// fails first: this uses tb.Errorf internally, not tb.Fatalf, so a goroutine failure
// does not abort the cleanup before the fd count is sampled and asserted.
//
// Both dimensions require the count to return to AT OR BELOW the baseline: this
// test's descriptors and its goroutines are alike its own to account for once its
// host has stopped, so neither has a legitimate reason to sit above baseline. The
// two differ only in how long they are given to get there — a goroutine may still
// be unwinding when the fd it held is already closed — which the shared settle
// loop's consecutive-sample requirement covers for both.
func RequireNoGoroutineOrFDLeak(tb testing.TB) {
	tb.Helper()

	goroutineBaseline := CountGoroutines()
	fdBaseline := CountOpenFDs(tb)
	tb.Cleanup(func() {
		tb.Helper()
		goroutineOK := settleGoroutines(tb, goroutineBaseline)
		fdOK := settleFDs(tb, fdBaseline)
		if !goroutineOK || !fdOK {
			tb.FailNow()
		}
	})
}

// settleGoroutines polls the goroutine count at settlePoll for up to the
// settleMaxWait iteration budget, reporting a failure via tb.Errorf (not Fatalf, so
// a caller can still run a second check afterward) unless it sees
// goroutineSettleSamples consecutive samples at or below baseline+slack. Returns
// whether it settled.
//
// The two ways this can fail get two different messages, because they mean
// different things: a final sample that is itself still above baseline+slack really
// did leak and never settled — but a final sample that IS within the band, having
// simply not sustained it for enough consecutive samples, is a flapping count that
// ran out of time, not one proven to have stayed elevated the whole window. Reusing
// one phrasing for both reads as self-contradictory ("final=+2, allowed +3, leak") in
// the second case. tests/soak's settleGoroutines draws the same distinction (see the
// nil-vs-non-nil last-error split there).
func settleGoroutines(tb testing.TB, baseline int) bool {
	tb.Helper()

	start := time.Now()
	consecutive := 0
	var last int
	for range int(settleMaxWait / settlePoll) {
		last = CountGoroutines()
		if last <= baseline+goroutineSettleSlack {
			consecutive++
			if consecutive >= goroutineSettleSamples {
				return true
			}
		} else {
			consecutive = 0
		}
		time.Sleep(settlePoll)
	}

	elapsed := time.Since(start).Round(time.Millisecond)
	dumpGoroutines(tb)

	if last <= baseline+goroutineSettleSlack {
		tb.Errorf(
			"goroutine count flapped, never settled: baseline=%d final=%d (within baseline+slack=%d) but only "+
				"reached %d/%d consecutive stable samples in %s",
			baseline, last, baseline+goroutineSettleSlack, consecutive, goroutineSettleSamples, elapsed,
		)

		return false
	}

	tb.Errorf(
		"goroutine leak: baseline=%d final=%d (+%d, allowed +%d) never settled for %d consecutive samples within %s",
		baseline, last, last-baseline, goroutineSettleSlack, goroutineSettleSamples, elapsed,
	)

	return false
}

// settleFDs polls the open-fd count at settlePoll for up to the settleMaxWait
// iteration budget, reporting a failure via tb.Errorf unless it sees
// goroutineSettleSamples consecutive samples at or below baseline. Returns whether
// it settled. Shares its sample count and cadence with settleGoroutines so the two
// dimensions RequireNoGoroutineOrFDLeak checks settle on the same schedule.
func settleFDs(tb testing.TB, baseline int) bool {
	tb.Helper()

	start := time.Now()
	consecutive := 0
	var last int
	for range int(settleMaxWait / settlePoll) {
		last = CountOpenFDs(tb)
		if last <= baseline {
			consecutive++
			if consecutive >= goroutineSettleSamples {
				return true
			}
		} else {
			consecutive = 0
		}
		time.Sleep(settlePoll)
	}

	elapsed := time.Since(start).Round(time.Millisecond)
	if last <= baseline {
		tb.Errorf(
			"fd count flapped, never settled: baseline=%d final=%d but only reached %d/%d consecutive stable "+
				"samples in %s",
			baseline, last, consecutive, goroutineSettleSamples, elapsed,
		)

		return false
	}

	tb.Errorf(
		"fd leak: baseline=%d final=%d (+%d) never settled for %d consecutive samples within %s",
		baseline, last, last-baseline, goroutineSettleSamples, elapsed,
	)

	return false
}

// dumpGoroutines writes the current goroutine profile — one stack trace per
// distinct call stack, with a count of how many goroutines share it (pprof's debug
// level 1) — to tb's log. A bare count says a leak happened; this says what is
// still running, which is what a CI log needs to actually diagnose one without a
// human reproducing it locally first.
func dumpGoroutines(tb testing.TB) {
	tb.Helper()

	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		tb.Logf("goroutine profile: could not capture: %v", err)

		return
	}
	tb.Logf("goroutine profile at leak-check failure:\n%s", buf.String())
}
