package styx_test

import (
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// Test that the plugin's shared-memory attach returns the burst composite exactly
// when the burst path is active for the generation, and a plain shared-memory
// transport otherwise.
//
// The serving session receives every inbound frame through whatever this attach
// returns, so this one value decides whether an oversize request can arrive at
// all, whether the readiness pump exists, and which receive guard the socket
// applies. A tuple that negotiated the feature but arrives with a zero ceiling has
// the path off for this generation and must get the pre-burst wiring exactly.
func TestPluginServer_PluginAttachSHM_ReturnsTheCompositeOnlyWhenTheBurstPathIsActive(t *testing.T) {
	const ceiling = 1 << 20

	cases := []struct {
		name      string
		featureOn bool
		ceiling   uint32
		wantBurst bool
	}{
		{name: "negotiated-and-configured", featureOn: true, ceiling: ceiling, wantBurst: true},
		{name: "not-negotiated", featureOn: false, ceiling: ceiling},
		{name: "negotiated-but-not-configured", featureOn: true, ceiling: 0},
		{name: "neither", featureOn: false, ceiling: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nfds := 3
			if tc.featureOn && tc.ceiling > 0 {
				nfds = 4
			}

			sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			pluginConn := control.NewConn(sockets[1], 7)
			t.Cleanup(func() { _ = unix.Close(sockets[0]); _ = pluginConn.Close() })

			region, err := shm.CreateRegion(leanLayoutForAttachTest())
			require.NoError(t, err)
			hpEFD, err := event.NewEventFD()
			require.NoError(t, err)
			phEFD, err := event.NewEventFD()
			require.NoError(t, err)
			pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = region.Close()
				_ = hpEFD.Close()
				_ = phEFD.Close()
				_ = unix.Close(pair[0])
				_ = unix.Close(pair[1])
			})

			attachMsg := &controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_AttachRegion{
					AttachRegion: &controlpb.AttachRegion{
						Generation: 7, LayoutSize: region.Layout().RegionSize,
						LayoutVersion: 1, FdCount: uint32(nfds), MaxDataInflight: 32, //nolint:gosec // 3 or 4
						BurstMaxPayload: tc.ceiling,
					},
				},
			}
			wire, merr := proto.Marshal(attachMsg)
			require.NoError(t, merr)
			sendFDs := []int{region.FD(), hpEFD.FD(), phEFD.FD(), pair[1]}[:nfds]
			require.NoError(t, unix.Sendmsg(sockets[0], wire, unix.UnixRights(sendFDs...), nil, 0))

			srv := styx.NewPluginServer(styx.PluginServerConfig{})
			tuple := control.Tuple{
				Transport: "shm", LayoutVersion: 1, Codec: "proto",
				Features: map[string]bool{control.FeatureBurst: tc.featureOn},
			}

			goroutinesBefore := testutil.CountGoroutines()
			fdsBefore := testutil.CountOpenFDs(t)

			tr, release, aerr := srv.PluginAttachSHMTransportForTest(t.Context(), pluginConn, tuple)
			require.NoError(t, aerr)
			require.NotNil(t, tr)

			bt, isBurst := tr.(*rpcruntime.BurstTransport)
			require.Equal(t, tc.wantBurst, isBurst,
				"the burst path is active for this generation exactly when the tuple and the ceiling both say so")
			if isBurst {
				require.Equal(t, uint32(ceiling), bt.Ceiling(),
					"the composite must route on the ceiling that arrived on the attach")
			}

			release()

			require.Equal(t, fdsBefore, testutil.CountOpenFDs(t),
				"releasing the transport must close every descriptor the attach took over, including the burst end")
			require.True(t, goroutinesSettled(goroutinesBefore),
				"releasing the transport must join the readiness pump the composite started")
		})
	}
}
