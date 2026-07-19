//go:build eventhook

package shm

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/transport"
)

// pauseOnFirstCall returns a hook that blocks — until release is called — the
// first time it fires, and is a no-op on every later call. armed is closed when
// that first call is reached, so a test can wait for the checkpoint before
// deciding what the producer does next.
func pauseOnFirstCall() (hook func(), armed <-chan struct{}, release func()) {
	armedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var calls atomic.Int32

	hook = func() {
		if calls.Add(1) == 1 {
			close(armedCh)
			<-releaseCh
		}
	}
	release = func() { close(releaseCh) }

	return hook, armedCh, release
}

// Test the parked-reader wake with the §12 eventfd path forced deterministically:
// the reader is paused exactly at the C2 tail re-check (shm-abi.md §11) via the
// event package's post-arm-check hook, before any work is published. Because no
// send has happened yet, C2 loads the old tail; releasing the hook then commits
// the reader to the blocking eventfd read (the captured tail still equals
// lastSeen, so the re-check cannot return the work). The subsequent Send can
// reach the reader only through the producer signal — a dropped signal would hang
// Recv forever, so delivery is proof the eventfd wake path ran.
//
// This closes the C1–C2 async-preemption window the always-on companion
// (TestTransport_SendRecv_WakesParkedReader_UnderSingleProc) cannot rule out: the
// hook physically holds the reader at C2 until the test releases it, so the send
// provably lands after the re-check regardless of scheduling. GOMAXPROCS(1) still
// forces the spin budget to 0 so the reader parks rather than catching the work
// in a spin. Run with -tags eventhook -race.
func TestTransport_SendRecv_WakesParkedReader_ForcedPastArmRecheck(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	hook, armed, release := pauseOnFirstCall()
	event.SetHookAfterArmCheck(hook)
	t.Cleanup(func() { event.SetHookAfterArmCheck(nil) })

	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	want := transport.Frame{CallID: 9, Kind: transport.FrameUnaryReq, Payload: []byte("forced eventfd wake")}

	got := make(chan transport.Frame, 1)
	errc := make(chan error, 1)
	go func() {
		f, err := ep.plugin.Recv(t.Context())
		if err != nil {
			errc <- err

			return
		}
		got <- f
	}()

	// Wait until the reader has resolved C2 against the old tail and is paused in
	// the hook; releasing it commits the reader to the blocking eventfd read.
	select {
	case <-armed:
	case err := <-errc:
		t.Fatalf("Recv failed before reaching the arm re-check: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not reach the C2 arm re-check (shm-abi.md §11)")
	}
	release()

	// Only now publish and signal: the reader is past C2, so the eventfd signal is
	// the sole thing that can wake it (shm-abi.md §12).
	require.NoError(t, ep.host.Send(t.Context(), want))

	select {
	case f := <-got:
		require.Equal(t, want.CallID, f.CallID)
		require.Equal(t, want.Payload, f.Payload)
	case err := <-errc:
		t.Fatalf("Recv failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("a reader forced past the arm re-check was not woken by the producer signal (shm-abi.md §12)")
	}
}
