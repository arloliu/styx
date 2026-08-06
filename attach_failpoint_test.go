package styx_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// countRegionMappings counts the shared-memory region mappings this process holds,
// by matching the region memfd's name in /proc/self/maps. Each CreateRegion or
// OpenRegion mapping is one line, so a leaked munmap (which the fd count cannot
// catch, since Region.Close closes the fd even if Munmap fails) shows up here as a
// surviving line. A small local copy, mirroring internal/supervisor's own.
func countRegionMappings(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)

	return strings.Count(string(data), "styx-shm-region")
}

// goroutinesSettled reports whether the live goroutine count is back at or below
// baseline, polling for about a second so an asynchronous teardown gets a bounded
// window without a real leak (which never clears) ever passing. It samples on the
// caller's own goroutine deliberately: a poll helper that runs its condition in a
// goroutine of its own would count that goroutine too and never see the baseline.
func goroutinesSettled(baseline int) bool {
	for range 50 {
		if testutil.CountGoroutines() <= baseline {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}

	return false
}

// leanLayoutForAttachTest is a small valid shared-memory geometry for the
// plugin-side per-step attach test's scripted host. It is deliberately minimal
// and belongs to this test rather than tracking any shipped profile: what is
// under test is the attach step sequence, not the geometry.
func leanLayoutForAttachTest() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 512, SlabCount: 64}, {SlabSize: 4096, SlabCount: 64}}

	return shm.Layout{
		Generation:       7,
		RingCapacity:     512,
		LifecycleReserve: 32,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// Test that the plugin-side shared-memory attach closes exactly what it owns when
// a deterministic failure is injected after EACH construction step — after the fd
// receive, after each eventfd wrapper, after the burst wrapper, after the local
// attach, and at the ack send. A scripted host sends a real region and two
// eventfds over SCM_RIGHTS (plus a burst socketpair end when the burst path is
// active); the plugin receives them, aborts at the injected step, and its process
// fd count AND region mapping count both return exactly to their pre-attach
// values. The two are asserted separately because Region.Close closes the fd even
// if its Munmap fails, so an fd count alone cannot prove the mapping was
// released. Goroutines are counted too: nothing an aborted attach started may
// outlive it. Every step runs with the burst path off and on, because a
// burst-active attach holds one more raw descriptor across the same windows. The
// crash-window variants are the chaos suite's job; these are the deterministic
// unit-level counts.
func TestPluginServer_PluginAttachSHM_PerStepFailure_ClosesExactlyWhatItOwns(t *testing.T) {
	const ceiling = 1 << 20

	plain := []string{"recv-fds", "hp-wrap", "ph-wrap", "attach", "ack-send"}
	burst := []string{"recv-fds", "hp-wrap", "ph-wrap", "burst-wrap", "attach", "ack-send"}

	for _, mode := range []struct {
		name    string
		steps   []string
		ceiling uint32
	}{
		{name: "burst-off", steps: plain, ceiling: 0},
		{name: "burst-on", steps: burst, ceiling: ceiling},
	} {
		t.Run(mode.name, func(t *testing.T) {
			for _, step := range mode.steps {
				t.Run(step, func(t *testing.T) {
					fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
					require.NoError(t, err)
					hostConn := control.NewConn(fds[0], 7)
					pluginConn := control.NewConn(fds[1], 7)
					t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

					// Scripted host: a real region and two eventfds, sent over SCM_RIGHTS,
					// plus a real socketpair end when the burst path is active.
					region, err := shm.CreateRegion(leanLayoutForAttachTest())
					require.NoError(t, err)
					hpEFD, err := event.NewEventFD()
					require.NoError(t, err)
					phEFD, err := event.NewEventFD()
					require.NoError(t, err)
					t.Cleanup(func() { _ = region.Close(); _ = hpEFD.Close(); _ = phEFD.Close() })

					sendFDs := []int{region.FD(), hpEFD.FD(), phEFD.FD()}
					if mode.ceiling > 0 {
						pair, perr := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
						require.NoError(t, perr)
						t.Cleanup(func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) })
						sendFDs = append(sendFDs, pair[1])
					}

					attachMsg := &controlpb.ControlMessage{
						Body: &controlpb.ControlMessage_AttachRegion{
							AttachRegion: &controlpb.AttachRegion{
								Generation: 7, LayoutSize: region.Layout().RegionSize,
								LayoutVersion: 1, FdCount: uint32(len(sendFDs)), MaxDataInflight: 32,
								BurstMaxPayload: mode.ceiling,
							},
						},
					}
					require.NoError(t, hostConn.SendFDs(t.Context(), attachMsg, sendFDs))

					srv := styx.NewPluginServer(styx.PluginServerConfig{})
					injected := errors.New("plugin attach failpoint")
					t.Cleanup(styx.SetPluginAttachSHMFailAtForTest(func(s string) error {
						if s == step {
							return injected
						}

						return nil
					}))

					tuple := control.Tuple{
						Transport: "shm", LayoutVersion: 1, Codec: "proto",
						Features: map[string]bool{control.FeatureBurst: mode.ceiling > 0},
					}

					fdsBefore := testutil.CountOpenFDs(t)
					mapsBefore := countRegionMappings(t) // includes the scripted host's own region mapping
					goroutinesBefore := testutil.CountGoroutines()
					aerr := srv.PluginAttachSHMForTest(t.Context(), pluginConn, tuple)

					require.ErrorIs(t, aerr, injected, "the attach must abort at the injected step")
					require.Equal(t, fdsBefore, testutil.CountOpenFDs(t), "step %q leaked a plugin fd", step)
					require.Equal(t, mapsBefore, countRegionMappings(t), "step %q leaked a region mapping", step)
					require.True(t, goroutinesSettled(goroutinesBefore), "step %q leaked a goroutine", step)
				})
			}
		})
	}
}
