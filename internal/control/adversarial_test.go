package control_test

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// adversarialBlobs is a go-fuzz-style (deterministic table, not a live
// fuzzer) set of hostile byte inputs a corrupt or malicious peer might put
// on the control wire: empty, single bytes, truncated/overlong protobuf
// varints, oversized payloads past MaxMessageSize, and seeded pseudo-random
// noise. The fork-delta review obligation is that none of these ever panic
// a parser — every one must yield a typed error or a harmless zero value.
func adversarialBlobs() [][]byte {
	r := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic corpus, not security-sensitive
	blobs := make([][]byte, 0, 42)
	blobs = append(blobs,
		nil,
		[]byte{},
		[]byte{0x00},
		[]byte{0xFF},
		[]byte{0x08}, // field-1 varint tag, value truncated
		[]byte{0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // overlong varint
		[]byte{0x0A, 0x7F}, // length-delimited field claiming 127 bytes, none present
		bytes.Repeat([]byte{0xAB}, control.MaxMessageSize),     // oversized
		bytes.Repeat([]byte{0x00}, control.MaxMessageSize+100), // oversized zeros
		append([]byte{0x0A, 0x04}, []byte("uds\x00")...),       // embedded NUL in a string field
	)
	for range 32 {
		b := make([]byte, r.Intn(5000))
		_, _ = r.Read(b)
		blobs = append(blobs, b)
	}

	return blobs
}

// Test the control-plane proto parse helpers never panicking on adversarial
// input: whatever partial/garbage message proto.Unmarshal yields (or a nil
// message), HelloToOffer / HelloAckToTuple / HelloAckIncompatible must
// return a value, never panic. Locks in the nil-safety the generated
// getters provide against a future refactor that dereferences directly.
func TestControl_ParseHelpers_NeverPanic_OnAdversarialInput(t *testing.T) {
	for _, blob := range adversarialBlobs() {
		var hello controlpb.Hello
		_ = proto.Unmarshal(blob, &hello) // failure is fine; the result is what we probe

		var ack controlpb.HelloAck
		_ = proto.Unmarshal(blob, &ack)

		require.NotPanics(t, func() { _ = control.HelloToOffer(&hello) })
		require.NotPanics(t, func() { _ = control.HelloAckToTuple(&ack) })
		require.NotPanics(t, func() { _, _, _ = control.HelloAckIncompatible(&ack) })
	}

	// Nil receivers must be safe too (a body-less message routed here).
	require.NotPanics(t, func() { _ = control.HelloToOffer(nil) })
	require.NotPanics(t, func() { _ = control.HelloAckToTuple(nil) })
	require.NotPanics(t, func() { _, _, _ = control.HelloAckIncompatible(nil) })
}

// Test Conn.Recv returning a typed error (never panicking, never a torn
// message) for every adversarial datagram a hostile peer could send. A blob
// that even a raw Sendmsg can't put on the wire is skipped; every one that
// reaches Recv must come back as (nil-or-message, error) without panicking.
func TestControl_Recv_AdversarialDatagrams_NeverPanic(t *testing.T) {
	for _, blob := range adversarialBlobs() {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		require.NoError(t, err)

		// Feed the blob as one raw datagram from the peer end, bypassing Send
		// (which would reject oversized/garbage) so Recv faces exactly what a
		// hostile peer could emit.
		if sendErr := unix.Sendmsg(fds[0], blob, nil, nil, 0); sendErr != nil {
			_ = unix.Close(fds[0])
			_ = unix.Close(fds[1])

			continue
		}

		recv := control.NewConn(fds[1], 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		require.NotPanics(t, func() {
			_, _ = recv.Recv(ctx)
		})
		cancel()

		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
	}
}
