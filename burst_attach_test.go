package styx_test

import (
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// Test that the plugin derives how many descriptors an AttachRegion must carry
// from the NEGOTIATED TUPLE, not from the message's own self-consistency.
//
// The generic control-plane check only compares the declared fd_count against the
// number of descriptors that actually arrived, so a message can be perfectly
// self-consistent and still be wrong for the tuple both sides acknowledged:
// fd_count 3 with three real descriptors on a burst-active tuple is the case that
// matters, because the burst end the plugin is required to wrap is simply absent
// and indexing the fourth position would panic. Every count the tuple does not
// require must be a protocol violation that closes every descriptor received, and
// the two counts the tuple does require (three for plain shared memory, four when
// burst is active) must still attach.
func TestPluginServer_PluginAttachSHM_FDCountIsDerivedFromTheTuple(t *testing.T) {
	const ceiling = 1 << 20

	cases := []struct {
		name      string
		featureOn bool   // the acknowledged tuple carries the burst feature
		ceiling   uint32 // burst_max_payload as sent on the wire
		nfds      int    // descriptors actually attached to the message
		declared  int    // fd_count declared on the message (-1: same as nfds)
		wantOK    bool
	}{
		{name: "plain/three", nfds: 3, declared: -1, wantOK: true},
		{name: "plain/two", nfds: 2, declared: -1},
		{name: "plain/four", nfds: 4, declared: -1},
		{name: "plain/five", nfds: 5, declared: -1},
		{name: "plain/none", nfds: 0, declared: -1},

		{name: "burst/four", featureOn: true, ceiling: ceiling, nfds: 4, declared: -1, wantOK: true},
		{name: "burst/three-self-consistent", featureOn: true, ceiling: ceiling, nfds: 3, declared: -1},
		{name: "burst/two", featureOn: true, ceiling: ceiling, nfds: 2, declared: -1},
		{name: "burst/five", featureOn: true, ceiling: ceiling, nfds: 5, declared: -1},
		{name: "burst/none", featureOn: true, ceiling: ceiling, nfds: 0, declared: -1},

		// A tuple that negotiated the feature but arrives with a zero ceiling has
		// the burst path off for this generation, so it is a three-descriptor
		// attach and a fourth descriptor is a violation.
		{name: "dormant/three", featureOn: true, ceiling: 0, nfds: 3, declared: -1, wantOK: true},
		{name: "dormant/four", featureOn: true, ceiling: 0, nfds: 4, declared: -1},

		// The generic declared-vs-received check still fires underneath the
		// tuple-derived one, in both directions.
		{name: "burst/declared-four-sent-three", featureOn: true, ceiling: ceiling, nfds: 3, declared: 4},
		{name: "burst/declared-three-sent-four", featureOn: true, ceiling: ceiling, nfds: 4, declared: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			pluginConn := control.NewConn(sockets[1], 7)
			t.Cleanup(func() { _ = unix.Close(sockets[0]); _ = pluginConn.Close() })

			// The scripted host holds a real region, two real eventfds, a real burst
			// socketpair end, and one spare, in the fixed order the plugin mirrors —
			// so a row that attaches the right count really attaches, and a row that
			// does not is rejected for its count rather than for junk descriptors.
			region, err := shm.CreateRegion(leanLayoutForAttachTest())
			require.NoError(t, err)
			hpEFD, err := event.NewEventFD()
			require.NoError(t, err)
			phEFD, err := event.NewEventFD()
			require.NoError(t, err)
			t.Cleanup(func() { _ = region.Close(); _ = hpEFD.Close(); _ = phEFD.Close() })

			pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			spare, err := unix.Dup(phEFD.FD())
			require.NoError(t, err)
			t.Cleanup(func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]); _ = unix.Close(spare) })

			pool := []int{region.FD(), hpEFD.FD(), phEFD.FD(), pair[1], spare}
			sendFDs := pool[:tc.nfds]

			declared := tc.nfds
			if tc.declared >= 0 {
				declared = tc.declared
			}
			attachMsg := &controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_AttachRegion{
					AttachRegion: &controlpb.AttachRegion{
						Generation: 7, LayoutSize: region.Layout().RegionSize,
						LayoutVersion: 1, FdCount: uint32(declared), MaxDataInflight: 32,
						BurstMaxPayload: tc.ceiling,
					},
				},
			}
			// Sent raw rather than through SendFDs: that helper asserts its own
			// declared-count/len(fds) agreement, and these rows exist to put a
			// disagreeing message on the wire for the receiver to judge.
			wire, merr := proto.Marshal(attachMsg)
			require.NoError(t, merr)
			var oob []byte
			if len(sendFDs) > 0 {
				oob = unix.UnixRights(sendFDs...)
			}
			require.NoError(t, unix.Sendmsg(sockets[0], wire, oob, nil, 0))

			srv := styx.NewPluginServer(styx.PluginServerConfig{})
			tuple := control.Tuple{
				Transport: "shm", LayoutVersion: 1, Codec: "proto",
				Features: map[string]bool{control.FeatureBurst: tc.featureOn},
			}

			fdsBefore := testutil.CountOpenFDs(t)
			mapsBefore := countRegionMappings(t) // includes the scripted host's own region mapping
			aerr := srv.PluginAttachSHMForTest(t.Context(), pluginConn, tuple)

			if tc.wantOK {
				require.NoError(t, aerr, "the count this tuple requires must attach")
			} else {
				require.ErrorIs(t, aerr, control.ErrProtocolViolation,
					"a descriptor count the tuple does not require must be a protocol violation")
			}
			require.Equal(t, fdsBefore, testutil.CountOpenFDs(t),
				"every received descriptor must be closed, whatever the outcome")
			require.Equal(t, mapsBefore, countRegionMappings(t), "the attach leaked a region mapping")
		})
	}
}
