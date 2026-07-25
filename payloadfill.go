package styx

import (
	"errors"
	"fmt"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/transport"
	"google.golang.org/protobuf/proto"
)

// errPayloadFill marks an error raised by the marshal callback a transport runs
// to produce a frame's payload, as opposed to a failure of the transport around
// it. A send path uses the distinction to tell "this one message could not be
// encoded" — which costs one call — from "the connection is gone", which costs
// the session.
var errPayloadFill = errors.New("styx: payload fill failed")

// errShortSizedMarshal is returned by the payload-fill callback when the codec
// reports writing a different number of bytes than the size it was asked for.
// See newPayloadFillFunc for why that has to be caught here and nowhere else.
var errShortSizedMarshal = errors.New("styx: sized marshal wrote a different byte count than its declared size")

// payloadFiller is a resolved fill-mode send: the transport that will run the
// callback, the exact payload length it will hand it, and the callback itself.
type payloadFiller struct {
	sender transport.PayloadFillSender
	size   int
	fill   func(dst []byte) error
}

// resolvePayloadFiller reports whether m's payload can be encoded straight into
// tr's own send buffer, and returns the send that does it.
//
// Three things must hold, and each has a legitimate way of not holding: the
// transport must have a send buffer worth filling (uds does not implement
// transport.PayloadFillSender, so uds always falls back), the codec must be able
// to marshal into a caller-provided buffer at all, and it must be able to size
// THIS message (codec.SizedMarshaler.Size reports false for a message with no
// sized-marshal support). Nothing else differs between the two paths: same
// frame, same routing, same bytes on the wire — only whether they are produced
// into a wire buffer the transport then copies, or into the transport's buffer
// directly.
func resolvePayloadFiller(tr transport.Transport, cdc codec.Codec, m proto.Message) (payloadFiller, bool) {
	sender, ok := tr.(transport.PayloadFillSender)
	if !ok {
		return payloadFiller{}, false
	}

	sm, ok := cdc.(codec.SizedMarshaler)
	if !ok {
		return payloadFiller{}, false
	}

	size, ok := sm.Size(m)
	if !ok {
		return payloadFiller{}, false
	}

	return payloadFiller{sender: sender, size: size, fill: newPayloadFillFunc(sm, m, size)}, true
}

// newPayloadFillFunc returns the callback the transport runs on its writer
// goroutine to encode m into the exact-size payload window of its send buffer.
//
// The byte-count comparison is the callback's reason to exist beyond the
// MarshalTo call it wraps. transport.PayloadFillSender hands the callback a
// window of buffer memory that is reused rather than zeroed, computes any
// checksum over that window AFTER the callback returns, and takes no byte count
// back from it — so a codec that wrote fewer than size bytes would ship the
// previous frame's residue to the peer under a valid checksum, undetectably.
// This is the only component holding both the declared size and the count
// MarshalTo actually wrote, so a disagreement has to fail the frame here.
func newPayloadFillFunc(sm codec.SizedMarshaler, m proto.Message, size int) func(dst []byte) error {
	return func(dst []byte) error {
		n, err := sm.MarshalTo(m, dst)
		if err != nil {
			return fmt.Errorf("%w: %w", errPayloadFill, err)
		}
		if n != size {
			return fmt.Errorf("%w: %w: wrote %d of %d bytes", errPayloadFill, errShortSizedMarshal, n, size)
		}

		return nil
	}
}
