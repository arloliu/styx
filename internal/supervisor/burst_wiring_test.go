package supervisor_test

import (
	"testing"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Test that the host's shared-memory attach returns the burst composite exactly
// when the burst path is active for the generation, and a plain shared-memory
// transport otherwise.
//
// The wire shape (a fourth descriptor and the ceiling) is asserted elsewhere;
// what matters here is which transport the rest of the host is handed, because
// that single value decides whether a giant payload has a socket to travel on,
// whether the readiness pump exists, and which receive guard inbound frames meet.
// A generation whose tuple or ceiling leaves the path off must get exactly the
// transport it got before the burst path existed — one underside, no pump.
func TestSupervisor_AttachSHM_ReturnsTheCompositeOnlyWhenTheBurstPathIsActive(t *testing.T) {
	const ceiling = 1 << 20

	cases := []struct {
		name       string
		configured uint32
		featureOn  bool
		wantBurst  bool
	}{
		{name: "negotiated-and-configured", configured: ceiling, featureOn: true, wantBurst: true},
		{name: "not-negotiated", configured: ceiling, featureOn: false},
		{name: "negotiated-but-not-configured", configured: 0, featureOn: true},
		{name: "neither", configured: 0, featureOn: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			hostConn := control.NewConn(fds[0], 7)
			pluginConn := control.NewConn(fds[1], 7)
			t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

			sup := supervisor.New(supervisor.Config{
				Transport: "shm", ShmLayout: leanShmLayout(), MaxDataInflight: 32,
				BurstMaxPayload: tc.configured,
			}, supervisor.NewEventBus())

			tuple := control.Tuple{
				Transport: "shm", LayoutVersion: 1, Codec: "proto",
				Features: map[string]bool{control.FeatureBurst: tc.featureOn},
			}

			seenCh := make(chan attachSeen, 1)
			go scriptPluginAttachAck(pluginConn, 4, seenCh)

			goroutinesBefore := testutil.CountGoroutines()
			fdsBefore := testutil.CountOpenFDs(t)

			tr, release, aerr := sup.AttachSHMTransportForTest(t.Context(), hostConn, 7, tuple)
			require.NoError(t, aerr)
			require.NotNil(t, tr)

			bt, isBurst := tr.(*rpcruntime.BurstTransport)
			require.Equal(t, tc.wantBurst, isBurst,
				"the burst path is active for this generation exactly when the tuple and the ceiling both say so")
			if isBurst {
				require.Equal(t, uint32(ceiling), bt.Ceiling(),
					"the composite must route on the same ceiling the attach put on the wire")
			}

			release()
			<-seenCh

			require.Equal(t, fdsBefore, testutil.CountOpenFDs(t),
				"releasing the transport must close every descriptor the attach took over, including the burst end")
			require.True(t, goroutinesSettled(goroutinesBefore),
				"releasing the transport must join the readiness pump the composite started")
		})
	}
}

// Test that a host with a burst ceiling configured, talking to a plugin that
// never offers the feature, both negotiates the path away and then refuses an
// oversize payload exactly as it did before the feature existed.
//
// The ceiling is a grant this host made; the plugin's offer is what says whether
// anything can be built to use it. With no feature resolved there is no socket, so
// the caller's answer for a payload the region cannot hold has to be the same
// refusal it always was — not a hang, and not a send onto a descriptor the peer
// never received.
func TestSupervisor_RejectsAnOversizePayload_AgainstAPluginThatDoesNotOfferBurst(t *testing.T) {
	const ceiling = 4 << 20

	sup := supervisor.New(supervisor.Config{
		Transport: "shm", ShmLayout: leanShmLayout(), MaxDataInflight: 32,
		BurstMaxPayload: ceiling,
	}, supervisor.NewEventBus())

	// Given this host's real offer, negotiated against a plugin that agrees on
	// everything except the burst flag it never learned to send.
	hostOffer := sup.HostOfferForTest()
	oldPlugin := hostOffer
	oldPlugin.Services = nil
	oldPlugin.Features = nil
	for _, f := range hostOffer.Features {
		if f.Name != control.FeatureBurst {
			oldPlugin.Features = append(oldPlugin.Features, f)
		}
	}

	tuple, err := control.Negotiate(hostOffer, oldPlugin, nil)
	require.NoError(t, err, "the burst feature is optional: its absence must not fail the handshake")
	require.False(t, tuple.Features[control.FeatureBurst], "a feature only the host offers resolves false")
	require.False(t, control.BurstActive(tuple, ceiling), "a configured ceiling activates nothing on its own")

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	hostConn := control.NewConn(fds[0], 7)
	pluginConn := control.NewConn(fds[1], 7)
	t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

	seenCh := make(chan attachSeen, 1)
	go scriptPluginAttachAck(pluginConn, 4, seenCh)

	tr, release, aerr := sup.AttachSHMTransportForTest(t.Context(), hostConn, 7, tuple)
	require.NoError(t, aerr)
	t.Cleanup(release)

	// The attach put the pre-burst message on the wire and built the pre-burst
	// transport: three descriptors, no ceiling, one underside.
	seen := <-seenCh
	require.NoError(t, seen.err)
	require.Equal(t, 3, seen.nfds, "a dormant tuple carries no burst descriptor")
	require.Zero(t, seen.ceiling, "a dormant tuple carries no ceiling")
	_, isBurst := tr.(*rpcruntime.BurstTransport)
	require.False(t, isBurst, "a tuple without the feature must get the plain shared-memory transport")

	shmTr, ok := tr.(*shmtransport.Transport)
	require.True(t, ok)

	// When a payload above the region's own limit is sent.
	err = tr.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 1, Method: 1,
		Payload: make([]byte, int(shmTr.MaxSendPayload())+1),
	})

	// Then it is refused before anything is published.
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge,
		"a dormant burst tuple refuses an oversize payload exactly as a pre-burst one did")
	require.True(t, transport.NeverPublished(err),
		"a refused payload must not leave the caller guessing whether the peer saw it")
}
