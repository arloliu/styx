package supervisor_test

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The negotiated streaming flag flows out of the REAL handshake exchange, not an
// injected value: handshakeAndAttach drives Hello/HelloAck and AttachRegion against
// a scripted plugin, and returns streaming=true exactly when the plugin's offer
// also supports it, which newLiveInstance then stores on Instance.Streaming.
func TestHandshakeAndAttach_ReturnsNegotiatedStreaming(t *testing.T) {
	run := func(t *testing.T, pluginSupportsStreaming bool) bool {
		t.Helper()
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		require.NoError(t, err)
		hostConn := control.NewConn(fds[0], 1)
		pluginConn := control.NewConn(fds[1], 1)
		t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

		go scriptPluginHandshake(t, pluginConn, pluginSupportsStreaming)

		// The host offer always lists streaming (optional here); the tuple resolves it
		// to true only when the plugin also supports it.
		sup := supervisor.New(supervisor.Config{}, supervisor.NewEventBus())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tr, streaming, err := sup.HandshakeAndAttachForTest(ctx, hostConn, 1)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		return streaming
	}

	require.True(t, run(t, true), "streaming negotiates true when both sides support it")
	require.False(t, run(t, false), "streaming resolves false when the plugin does not support it")
}

// scriptPluginHandshake plays the plugin side of the handshake: it acknowledges the
// host's Hello with a negotiated tuple (its own offer supporting streaming iff
// supportsStreaming), then receives the AttachRegion fd and acknowledges it.
func scriptPluginHandshake(t *testing.T, conn *control.Conn, supportsStreaming bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	helloMsg, err := conn.Recv(ctx)
	if err != nil {
		t.Errorf("plugin: recv Hello: %v", err)

		return
	}
	hello := helloMsg.GetHello()

	features := []control.FeatureFlag{}
	if supportsStreaming {
		features = append(features, control.FeatureFlag{Name: "streaming"})
	}
	pluginOffer := control.Offer{
		ProtocolMin: 1, ProtocolMax: 1,
		Transports: []string{"uds"}, Codecs: []string{"proto"}, Features: features,
	}
	tuple, err := control.Negotiate(control.HelloToOffer(hello), pluginOffer, nil)
	if err != nil {
		t.Errorf("plugin: negotiate: %v", err)

		return
	}
	ack := control.TupleToHelloAck(tuple, hello.GetNonce(), control.PluginIdentity{}, nil, pluginOffer)
	ackMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_HelloAck{HelloAck: ack}}
	if err := conn.Send(ctx, ackMsg); err != nil {
		t.Errorf("plugin: send HelloAck: %v", err)

		return
	}

	_, attachFDs, err := conn.RecvFDs(ctx, 1)
	if err != nil {
		t.Errorf("plugin: recv AttachRegion: %v", err)

		return
	}
	for _, fd := range attachFDs {
		_ = unix.Close(fd)
	}
	attachAck := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegionAck{AttachRegionAck: &controlpb.AttachRegionAck{}},
	}
	if err := conn.Send(ctx, attachAck); err != nil {
		t.Errorf("plugin: send AttachRegionAck: %v", err)
	}
}
