package styx_test

import (
	"errors"
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/shm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// leanLayoutForAttachTest is a small valid shared-memory geometry (the lean
// device-gateway profile) for the plugin-side per-step attach test's scripted
// host.
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
// receive, after each eventfd wrapper, after the local attach, and at the ack
// send. A scripted host sends a real region and two eventfds over SCM_RIGHTS; the
// plugin receives them, aborts at the injected step, and its process fd count
// returns exactly to the pre-attach value. Because the region and transport close
// release each mapping with its fd, an exact fd count also proves no mapping leak.
// The crash-window variants are the chaos suite's job; these are the deterministic
// unit-level counts.
func TestPluginServer_PluginAttachSHM_PerStepFailure_ClosesExactlyWhatItOwns(t *testing.T) {
	steps := []string{"recv-fds", "hp-wrap", "ph-wrap", "attach", "ack-send"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			hostConn := control.NewConn(fds[0], 7)
			pluginConn := control.NewConn(fds[1], 7)
			t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

			// Scripted host: a real region and two eventfds, sent over SCM_RIGHTS.
			region, err := shm.CreateRegion(leanLayoutForAttachTest())
			require.NoError(t, err)
			hpEFD, err := event.NewEventFD()
			require.NoError(t, err)
			phEFD, err := event.NewEventFD()
			require.NoError(t, err)
			t.Cleanup(func() { _ = region.Close(); _ = hpEFD.Close(); _ = phEFD.Close() })

			attachMsg := &controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_AttachRegion{
					AttachRegion: &controlpb.AttachRegion{
						Generation: 7, LayoutSize: region.Layout().RegionSize,
						LayoutVersion: 1, FdCount: 3, MaxDataInflight: 32,
					},
				},
			}
			require.NoError(t, hostConn.SendFDs(t.Context(), attachMsg,
				[]int{region.FD(), hpEFD.FD(), phEFD.FD()}))

			srv := styx.NewPluginServer()
			injected := errors.New("plugin attach failpoint")
			t.Cleanup(styx.SetPluginAttachSHMFailAtForTest(func(s string) error {
				if s == step {
					return injected
				}

				return nil
			}))

			tuple := control.Tuple{Transport: "shm", LayoutVersion: 1, Codec: "proto", Features: map[string]bool{}}

			fdsBefore := countOpenFDs(t)
			aerr := srv.PluginAttachSHMForTest(t.Context(), pluginConn, tuple)

			require.ErrorIs(t, aerr, injected, "the attach must abort at the injected step")
			require.Equal(t, fdsBefore, countOpenFDs(t), "step %q leaked a plugin fd (and thus a mapping)", step)
		})
	}
}
