package supervisor_test

import (
	"testing"
	"time"

	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
)

// The tracker measures persistence on the plugin's heartbeat Sequence — one increment
// per admitted build, at least MinHeartbeatSpacing(interval) apart — so a stall spans the
// window once its span reaches ceil(window / minSpacing) increments. At the default one-
// second nominal, minSpacing is 875ms and a five-second window converts to six increments.
const (
	trackerInterval = time.Second
	trackerWindow   = 5 * trackerInterval
	// trackerWindowBeats is ceil(trackerWindow / MinHeartbeatSpacing(trackerInterval)) =
	// ceil(5s / 875ms) = 6: the span an adjacent qualifying run must reach to fire.
	trackerWindowBeats = uint64(6)
)

// wedgeKinds is the pair of wedge kinds the tracker distinguishes; the persistence
// captures run identically for each (same tracker, same sender-timebase rule).
var wedgeKinds = []struct {
	name string
	kind supervisor.WedgeKind
}{
	{"transport", supervisor.WedgeTransport},
	{"dispatch", supervisor.WedgeDispatch},
}

// Test a qualifying stall that clears before the window elapses never firing: two
// intervals of a five-interval window, then progress. The window is measured on the
// heartbeat sequence, not real elapsed time.
func TestWedgeTracker_SubWindowStall_DoesNotFire(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	// Two qualifying transport-wedge pairs, one and two intervals in.
	_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 1)
	require.False(t, fire)
	_, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 2)
	require.False(t, fire, "a stall shorter than the window must not fire")

	// Progress clears it; the stall never persisted the whole window.
	_, fire = tr.Observe(supervisor.HealthOK, supervisor.WedgeNone, 3)
	require.False(t, fire)
}

// Test a stall that persists the full window firing exactly once, on the first pair at
// or past ceil(window / minSpacing) sequence increments from the stall's first observation.
func TestWedgeTracker_FullWindowStall_FiresOnce(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	// The first qualifying pair (sequence 1) anchors; each pair short of the window
	// span (six increments) does not fire.
	for seq := uint64(1); seq <= 6; seq++ {
		kind, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire, "must not fire before the window elapses (sequence %d)", seq)
		require.Equal(t, supervisor.WedgeNone, kind)
	}

	// The pair a full window (six increments) past the anchor fires.
	kind, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 7)
	require.True(t, fire, "a stall persisting the whole window must fire")
	require.Equal(t, supervisor.WedgeTransport, kind)

	// A further qualifying pair of the same kind does not fire again: firing once is a
	// property of the tracker itself, not only of the heartbeat loop returning.
	kind, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 8)
	require.False(t, fire, "the same continuous stall must not fire a second time")
	require.Equal(t, supervisor.WedgeNone, kind)
}

// Test a recovering long-running handler — a per-pair verdict that is never wedged —
// never firing across many windows.
func TestWedgeTracker_HealthySequence_NeverFires(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	for seq := uint64(1); seq <= 100; seq++ {
		_, fire := tr.Observe(supervisor.HealthOK, supervisor.WedgeNone, seq)
		require.False(t, fire)
	}
}

// Test a single healthy pair in the middle of a stall restarting the clock, so an
// intermittent stall that keeps recovering never accumulates a full window.
func TestWedgeTracker_ProgressRestartsTheClock(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	// Four intervals of stall, one interval of progress, then stall again: the clock
	// restarts, so no single continuous stall reaches the window.
	for seq := uint64(1); seq <= 4; seq++ {
		_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire)
	}
	_, fire := tr.Observe(supervisor.HealthOK, supervisor.WedgeNone, 5)
	require.False(t, fire)
	// The stall resumes; its clock restarts here, so four more intervals still do not fire.
	for seq := uint64(6); seq <= 9; seq++ {
		_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire, "the clock must restart after progress (sequence %d)", seq)
	}
}

// Test Clear discarding an accumulating stall, as a serviced reload or a fresh
// generation does — so a stall observed before the reset never carries across it.
func TestWedgeTracker_Clear_DiscardsAccumulatedStall(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	// Four intervals of stall accumulate.
	for seq := uint64(1); seq <= 4; seq++ {
		_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire)
	}

	// A reload/generation reset clears the tracker.
	tr.Clear()

	// The same stall resumes on an adjacent run from here; its clock restarts, so it
	// must persist a fresh full window (six increments) before firing.
	_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 5)
	require.False(t, fire, "the pre-reset stall time must not carry across the clear")
	for seq := uint64(6); seq <= 10; seq++ {
		_, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire, "still short of a full window since the clear (sequence %d)", seq)
	}
	kind, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 11)
	require.True(t, fire)
	require.Equal(t, supervisor.WedgeTransport, kind)
}

// Test a switch of wedge kind restarting the clock: transport-wedged that becomes
// dispatch-wedged does not carry the transport stall's elapsed time into the
// dispatch stall.
func TestWedgeTracker_KindSwitch_RestartsTheClock(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	for seq := uint64(1); seq <= 4; seq++ {
		_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire)
	}
	// Now dispatch-wedged: a different fault, its own fresh window, anchored here and
	// persisted across an adjacent run.
	kind, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeDispatch, 5)
	require.False(t, fire)
	require.Equal(t, supervisor.WedgeNone, kind)
	for seq := uint64(6); seq <= 10; seq++ {
		_, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeDispatch, seq)
		require.False(t, fire, "the dispatch stall's own window is not yet full (sequence %d)", seq)
	}
	kind, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeDispatch, 11)
	require.True(t, fire)
	require.Equal(t, supervisor.WedgeDispatch, kind)
}

// Test a non-increasing sequence clearing the tracker: a heartbeat that did not
// advance the sequence (a restart or an out-of-contract anomaly) cannot be trusted
// as elapsed sender time, so any accumulated stall is discarded and the sequence
// re-anchors.
func TestWedgeTracker_NonIncreasingSequence_ClearsTracker(t *testing.T) {
	tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

	// A stall accumulates against high sequences (anchor 100).
	for seq := uint64(100); seq <= 103; seq++ {
		_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire)
	}

	// The sequence drops back to 1 (a fresh generation's first heartbeat). The old
	// anchor at 100 must not make delta (1 - 100, wrapping) fabricate a fire; the
	// tracker re-anchors at 1 instead.
	_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 1)
	require.False(t, fire, "a non-increasing sequence must re-anchor, not fire")

	// From the re-anchor, a fresh full window of adjacent increments is required.
	for seq := uint64(2); seq <= 6; seq++ {
		_, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, seq)
		require.False(t, fire)
	}
	kind, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 7)
	require.True(t, fire, "a full window past the re-anchor fires")
	require.Equal(t, supervisor.WedgeTransport, kind)
}

// Test the sender-timebase guarantee against a slow host consumer: qualifying pairs
// one sender interval apart must stay healthy even if the host dequeued them a full
// window of wall-clock time apart, then recovered. Host receipt time is not an input
// to the tracker, so the queued sub-window stall cannot be stretched into a wedge.
// Pre-fix (host-receipt clock) fired WedgeTransport on the second sample.
func TestWedgeTracker_QueuedSubWindowStall_StaysHealthy(t *testing.T) {
	for _, wk := range wedgeKinds {
		t.Run(wk.name, func(t *testing.T) {
			tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

			// H1 and H2 are one sender interval apart (sequences 41, 42); a real host
			// could dequeue them five seconds apart. H3 recovers.
			_, fire := tr.Observe(supervisor.HealthWedged, wk.kind, 41)
			require.False(t, fire)
			_, fire = tr.Observe(supervisor.HealthWedged, wk.kind, 42)
			require.False(t, fire, "a one-interval sender stall must never fire, whatever the host gap")
			_, fire = tr.Observe(supervisor.HealthOK, supervisor.WedgeNone, 43)
			require.False(t, fire)
		})
	}
}

// Test the continuity guard: a stall whose qualifying samples straddle a sequence GAP
// spanning the window must NOT fire. The heartbeats between the gap's ends were never
// observed by the host, so their classification is unknown — one may have carried a
// recovery — and the jump cannot be counted as continuous stall time. Pre-fix (any
// increasing jump counted) the jump satisfied the whole span and fired.
func TestWedgeTracker_SequenceGapSpanningWindow_DoesNotFire(t *testing.T) {
	for _, wk := range wedgeKinds {
		t.Run(wk.name, func(t *testing.T) {
			tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

			// Two adjacent qualifying pairs anchor the stall at sequence 1.
			_, fire := tr.Observe(supervisor.HealthWedged, wk.kind, 1)
			require.False(t, fire)
			_, fire = tr.Observe(supervisor.HealthWedged, wk.kind, 2)
			require.False(t, fire)

			// A gap: the sequence jumps a full window past the anchor (sequences 3..7 were
			// never delivered). A jump that counted as elapsed time (8-1=7 >= the six-increment
			// window) would fire; the continuity guard clears and re-anchors at 8 instead.
			_, fire = tr.Observe(supervisor.HealthWedged, wk.kind, 8)
			require.False(t, fire, "a gap spanning the window must clear, not fire")

			// The stall re-anchored at 8, so a single further adjacent pair is only one
			// increment in — nowhere near a fresh window.
			_, fire = tr.Observe(supervisor.HealthWedged, wk.kind, 9)
			require.False(t, fire)
		})
	}
}

// Test a non-integral window/minSpacing ratio firing at the mathematical ceiling: with a
// one-second nominal cadence minSpacing is 875ms, so a 2.5-second window needs three
// adjacent increments (ceil(2500ms / 875ms) = 3) to span it — a span of two is short and
// a span of three fires.
func TestWedgeTracker_NonIntegralWindowRatio_FiresAtCeiling(t *testing.T) {
	// ceil(2500ms / MinHeartbeatSpacing(1s)=875ms) = ceil(2.857) = 3 increments.
	tr := supervisor.NewWedgeTrackerForTest(2500*time.Millisecond, time.Second)

	_, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 1) // anchor
	require.False(t, fire)
	_, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 2) // span 1
	require.False(t, fire)
	_, fire = tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 3) // span 2, short of ceil = 3
	require.False(t, fire, "a span of two is short of ceil(2500ms/875ms) = 3")
	kind, fire := tr.Observe(supervisor.HealthWedged, supervisor.WedgeTransport, 4) // span 3
	require.True(t, fire, "a span of three reaches ceil(2500ms/875ms) = 3")
	require.Equal(t, supervisor.WedgeTransport, kind)
}

// Test the false-negative guard: qualifying pairs whose SEQUENCES span the window
// must fire exactly once even when the host dequeues them in a single burst with no
// wall-clock time between them. The sender-side window is satisfied; host time is
// nearly zero. Pre-fix (host-receipt clock) never fired on burst receipt.
func TestWedgeTracker_SenderWindowBurst_FiresOnce(t *testing.T) {
	for _, wk := range wedgeKinds {
		t.Run(wk.name, func(t *testing.T) {
			tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

			// Sequences 1..8 fed back to back (a drained backlog processed in a burst):
			// exactly one fire, on the first pair a full window past the anchor.
			fires := 0
			for seq := uint64(1); seq <= 8; seq++ {
				kind, fire := tr.Observe(supervisor.HealthWedged, wk.kind, seq)
				if fire {
					fires++
					require.Equal(t, wk.kind, kind)
					require.Equal(t, uint64(7), seq, "must fire on the sequence a full window past the anchor")
				}
			}
			require.Equal(t, 1, fires, "a sender window satisfied in a host burst must fire exactly once")
		})
	}
}

// Test the admitted-minimum-spacing conversion: the sender's spacing guard admits two
// built heartbeats as close as MinHeartbeatSpacing(cadence) apart — below a full cadence
// — so each adjacent Sequence increment proves only that admitted minimum of real sender
// time, not a whole cadence. The window must therefore convert to ceil(window/minSpacing)
// increments: an adjacent qualifying run must NOT fire until its span covers that many
// increments, and MUST fire once it does — never a beat early on the smaller
// ceil(window/cadence) count.
//
// For a five-cadence window at a one-cadence nominal (minSpacing = 875ms) the span must
// reach ceil(5s / 875ms) = 6 increments; 6 * 875ms = 5.25s covers the window, while a
// span of five would cover only 4.375s and understate it. Pre-fix the tracker converted
// on the full cadence (five increments) and fired on the sixth heartbeat — a beat early.
func TestWedgeTracker_AdmittedMinSpacingConversion_FiresNoBeatEarly(t *testing.T) {
	for _, wk := range wedgeKinds {
		t.Run(wk.name, func(t *testing.T) {
			tr := supervisor.NewWedgeTrackerForTest(trackerWindow, trackerInterval)

			// Anchor at 1, then an adjacent qualifying run. Six increments (through
			// sequence 7) are required; every pair short of that must stay healthy —
			// including sequence 6 (span five), which the pre-fix conversion fired on.
			for seq := uint64(1); seq <= 6; seq++ {
				_, fire := tr.Observe(supervisor.HealthWedged, wk.kind, seq)
				require.False(t, fire,
					"must not fire until the admitted-minimum span covers the window (sequence %d)", seq)
			}

			// Span six (sequence 7) is the first whose minSpacing-derived elapsed time
			// covers the window; it fires exactly here, never before.
			kind, fire := tr.Observe(supervisor.HealthWedged, wk.kind, 7)
			require.True(t, fire, "the admitted-minimum span covering the window fires")
			require.Equal(t, wk.kind, kind)
		})
	}
}

// Test the window-to-beats conversion directly: the beat count is ceil(window/minSpacing),
// and at that count span * minSpacing covers the window while one fewer increment would
// fall short — so the ceil is tight, never fires early, and never over-delays by more than
// the rounding demands. minSpacing is the admitted minimum the sender's guard enforces, not
// the full cadence.
func TestWedgeTracker_WindowBeats_CeilsOnAdmittedMinimum(t *testing.T) {
	require.Equal(t, 875*time.Millisecond, supervisor.MinHeartbeatSpacing(time.Second),
		"the admitted minimum is the cadence minus its eighth")

	cases := []struct {
		name            string
		window, cadence time.Duration
		wantBeats       uint64
	}{
		// The production default: 5s / 875ms = 5.714..., ceil 6.
		{"default nonintegral", trackerWindow, trackerInterval, trackerWindowBeats},
		// An integral ratio: 1750ms / 875ms = 2 exactly.
		{"integral", 1750 * time.Millisecond, time.Second, 2},
		// A small non-integral ratio: 2500ms / 875ms = 2.857..., ceil 3.
		{"small nonintegral", 2500 * time.Millisecond, time.Second, 3},
		// A different cadence, integral ratio: minSpacing(8s) = 7s; 14s / 7s = 2 exactly.
		{"integral other cadence", 14 * time.Second, 8 * time.Second, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr := supervisor.NewWedgeTrackerForTest(c.window, c.cadence)
			require.Equal(t, c.wantBeats, tr.WindowBeats())

			minSpacing := supervisor.MinHeartbeatSpacing(c.cadence)
			require.GreaterOrEqual(t, time.Duration(c.wantBeats)*minSpacing, c.window,
				"span * minSpacing must cover the window at the fire")
			require.Less(t, time.Duration(c.wantBeats-1)*minSpacing, c.window,
				"one fewer increment must fall short — the ceil is tight, not over-delayed")
		})
	}
}
