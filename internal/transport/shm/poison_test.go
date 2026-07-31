package shm

import (
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/transport"
)

// testPoisonFlag bundles a PoisonFlag under test with direct access to its
// backing words and eventfds, so a test can assert on them without a mapped
// region.
type testPoisonFlag struct {
	flag         *PoisonFlag
	poisonWord   *uint32
	shutdownWord *uint32
	hpEFD        *event.EventFD
	phEFD        *event.EventFD
}

// newTestPoisonFlag builds a PoisonFlag over plain heap words and two real
// eventfds. Both eventfds start undrained; the caller reads them to observe a
// wake. testing.TB so a benchmark can build a real, never-poisoned PoisonFlag
// too (the pre-publish gate's two atomic loads are only live when poison is
// wired, matching production's newRegionWriter).
func newTestPoisonFlag(t testing.TB) *testPoisonFlag {
	t.Helper()

	var poison, shutdown uint32
	hp, err := event.NewEventFD()
	require.NoError(t, err)
	ph, err := event.NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = hp.Close()
		_ = ph.Close()
	})

	return &testPoisonFlag{
		flag:         NewPoisonFlag(&poison, &shutdown, hp, ph),
		poisonWord:   &poison,
		shutdownWord: &shutdown,
		hpEFD:        hp,
		phEFD:        ph,
	}
}

// Test the seq_cst CAS is first-setter-wins under concurrent callers racing
// different nonzero causes (shm-abi.md §16): exactly one Set call wins, and
// Check reports that winner's cause regardless of which goroutines lost.
// In-process -race proves no Go-level data race on the shared word; it does
// not by itself prove cross-process safety (the real protocol spans two OS
// processes) -- shm-abi.md's seq_cst CAS contract is what does that (see
// .agents/rules/300-testing.md's note on the unsafe core's extra bar). Run
// with -race.
func TestPoisonFlag_Set_FirstSetterWinsUnderConcurrentCallers(t *testing.T) {
	// Given a fresh PoisonFlag and N distinct nonzero causes racing Set.
	p := newTestPoisonFlag(t).flag
	causes := []PoisonCause{
		PoisonGeneric, PoisonBadGeometry, PoisonBadFrame, PoisonRingCorrupt,
		PoisonChecksum, PoisonPeerCrash, PoisonBadSync,
	}

	var wg sync.WaitGroup
	results := make([]bool, len(causes))
	for i, cause := range causes {
		wg.Go(func() {
			results[i] = p.Set(cause)
		})
	}
	wg.Wait()

	// Then exactly one call won the CAS, and Check reports that winner's cause.
	wins := 0
	winner := -1
	for i, won := range results {
		if won {
			wins++
			winner = i
		}
	}
	require.Equal(t, 1, wins, "exactly one concurrent Set call must win the first-setter-wins CAS")

	gotCause, poisoned := p.Check()
	require.True(t, poisoned)
	require.Equal(t, causes[winner], gotCause, "Check must report the winning call's cause, not a later loser's")
}

// Test that Set performs the unconditional §16 teardown wake even when its own
// CAS is lost: the shutdown word is stored to 1 and both eventfds are written
// regardless of which call won, so a consumer parked in either direction is
// released (shm-abi.md §15/§16). Both eventfds are drained after the winning
// call before the losing call runs, so the wake this test asserts on is
// attributable to the losing call alone -- without draining first, a single
// post-read would pass even if the losing call wrote neither fd, because
// non-semaphore eventfds coalesce (a read drains the accumulated counter
// regardless of how many writes landed).
func TestPoisonFlag_Set_WakesBothDirectionsUnconditionally(t *testing.T) {
	// Given an already-poisoned flag (the first Set has already won the CAS),
	// with both eventfds drained.
	tf := newTestPoisonFlag(t)
	require.True(t, tf.flag.Set(PoisonBadFrame))
	require.NoError(t, tf.hpEFD.Read(t.Context()), "drain the winning call's hp wake")
	require.NoError(t, tf.phEFD.Read(t.Context()), "drain the winning call's ph wake")

	// When a second call races a different cause and necessarily loses the CAS.
	won := tf.flag.Set(PoisonChecksum)

	// Then the CAS was lost (the first cause is preserved)...
	require.False(t, won)
	cause, poisoned := tf.flag.Check()
	require.True(t, poisoned)
	require.Equal(t, PoisonBadFrame, cause, "a lost CAS must not overwrite the original cause")

	// ...but the unconditional wake still ran, and can only have come from the
	// losing call itself since both fds were drained beforehand: shutdown is
	// set, and both eventfds were signaled again (a stuck Read would time out
	// the test via its own ctx if the losing call's wake never happened).
	require.Equal(t, uint32(1), *tf.shutdownWord)
	require.NoError(t, tf.hpEFD.Read(t.Context()), "hp eventfd must have been written by the lost-CAS call too")
	require.NoError(t, tf.phEFD.Read(t.Context()), "ph eventfd must have been written by the lost-CAS call too")
}

// Test the frozen numeric wire values (shm-abi.md §3): the enum's iota
// ordering must stay exactly 0-8 in the ABI table's order, so an accidental
// reorder that would silently change a cross-process wire value fails this
// test instead of shipping.
func TestPoisonCause_FrozenWireValues(t *testing.T) {
	require.Equal(t, PoisonCause(0), PoisonNone)
	require.Equal(t, PoisonCause(1), PoisonGeneric)
	require.Equal(t, PoisonCause(2), PoisonBadGeometry)
	require.Equal(t, PoisonCause(3), PoisonBadFrame)
	require.Equal(t, PoisonCause(4), PoisonRingCorrupt)
	require.Equal(t, PoisonCause(5), PoisonChecksum)
	require.Equal(t, PoisonCause(6), PoisonStaleStamp)
	require.Equal(t, PoisonCause(7), PoisonPeerCrash)
	require.Equal(t, PoisonCause(8), PoisonBadSync)
}

// Test that Set panics for any cause outside the exact ABI-frozen valid set
// {1,2,3,4,5,7,8} (shm-abi.md §3): 0 (POISON_NONE, not a cause), 6
// (POISON_STALE_STAMP, reserved-unused in v1), and every value >= 9 (reserved
// for a future layout_version, never writable under v1). Passing one of these
// is a construction bug, not a runtime condition, so Set panics rather than
// CASing a reserved wire value into the cross-process word.
func TestPoisonFlag_Set_PanicsOnCauseOutsideTheFrozenValidSet(t *testing.T) {
	cases := []struct {
		name  string
		cause PoisonCause
	}{
		{"zero (POISON_NONE) is not a cause", PoisonNone},
		{"POISON_STALE_STAMP is reserved-unused in v1", PoisonStaleStamp},
		{"the first value past the frozen table", PoisonCause(9)},
		{"a large reserved value", PoisonCause(1000)},
		{"the uint32 max is still reserved", PoisonCause(math.MaxUint32)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPoisonFlag(t).flag
			require.Panics(t, func() { p.Set(tc.cause) })
		})
	}
}

// Test the fault->cause mapping documented on errors.go and required by
// shm-abi.md §16's "who may set it" list: each typed conformance fault this
// package detects maps to its frozen ABI §3 cause, and errGenerationMismatch
// -- a construction bug, not a peer fault -- and any unrelated error map to
// no cause at all (ok=false), since a generation mismatch is the canonical
// discard-not-poison case (§15/§16) and must never reach PoisonFlag.Set.
func TestFaultToPoison_MapsEachConformanceFaultToItsCause(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantCause PoisonCause
		wantOK    bool
	}{
		{
			name: "ring corruption maps to POISON_RING_CORRUPT", err: errRingCorrupt,
			wantCause: PoisonRingCorrupt, wantOK: true,
		},
		{name: "bad frame maps to POISON_BAD_FRAME", err: errBadFrame, wantCause: PoisonBadFrame, wantOK: true},
		{
			name: "checksum mismatch maps to POISON_CHECKSUM", err: errChecksum,
			wantCause: PoisonChecksum, wantOK: true,
		},
		{name: "bad sync maps to POISON_BAD_SYNC", err: errBadSync, wantCause: PoisonBadSync, wantOK: true},
		{name: "lost wake maps to the generic cause", err: errLostWake, wantCause: PoisonGeneric, wantOK: true},
		{name: "generation mismatch is never a poison cause", err: errGenerationMismatch, wantOK: false},
		{name: "an unrelated error is never a poison cause", err: errors.New("shm: something else"), wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given / When
			cause, ok := faultToPoisonCause(tc.err)

			// Then
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantCause, cause)
			}
		})
	}
}

// Test ErrPoisoned classifying as the cross-transport poison sentinel while
// rendering its own text unchanged. The classification is what a host read loop
// and a plugin serve loop act on — neither knows this package's sentinel, so a
// poison that does not carry transport.ErrPoisoned escalates nothing and leaves
// the plugin heartbeating on a dead region. The rendering is what keeps
// TeardownError's message ending in the poison cause, where a reader looks for it.
func TestErrPoisoned_ClassifiesAsTheTransportPoison_AndRendersItsOwnTextOnly(t *testing.T) {
	// Given a poisoned region.
	p := newTestPoisonFlag(t)
	p.flag.Set(PoisonGeneric)

	// When its teardown error is taken.
	teardown := p.flag.TeardownError()

	// Then it is the poison every reader loop recognizes, without becoming
	// indistinguishable from a uds mid-frame desync in the other direction.
	require.ErrorIs(t, ErrPoisoned, transport.ErrPoisoned)
	require.ErrorIs(t, teardown, ErrPoisoned)
	require.ErrorIs(t, teardown, transport.ErrPoisoned)
	require.NotErrorIs(t, transport.ErrPoisoned, ErrPoisoned,
		"a uds mid-frame desync must never match this region-specific sentinel")

	// And a graceful shutdown is still the other thing entirely.
	require.NotErrorIs(t, transport.ErrClosed, transport.ErrPoisoned)

	// And the rendering names the region and then the cause, with nothing of the
	// wrapped sentinel between them.
	require.Equal(t, "shm: region poisoned", ErrPoisoned.Error())
	require.Equal(t, "shm: region poisoned: generic", teardown.Error())
}
