package styx

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// viewServiceID and viewMethodID name the one-method service these tests
// register: a method whose request is a BytesValue, so a test can drive
// arbitrary payload bytes through the two-phase handler contract.
var (
	viewServiceID = fnv64a("view.Svc")
	viewMethodID  = fnv64a("Take")
)

// newViewTestService builds the one-method service descriptor these tests
// register, handing each decoded request to take.
func newViewTestService(take func(*wrapperspb.BytesValue)) *ServiceDesc {
	return &ServiceDesc{
		ServiceName: "view.Svc",
		ServiceID:   viewServiceID,
		Methods: []MethodDesc{{
			MethodName: "Take",
			MethodID:   viewMethodID,
			NewRequest: func() proto.Message { return &wrapperspb.BytesValue{} },
			Handler: func(_ any, _ context.Context, req proto.Message) (proto.Message, error) {
				body, _ := req.(*wrapperspb.BytesValue)
				take(body)

				return &wrapperspb.BytesValue{}, nil
			},
		}},
	}
}

// newViewTestDeps wires the serve-loop dependencies these tests exercise
// directly, without a transport: the seams under test are the request
// preparation and the decode, both of which take a frame and a codec.
func newViewTestDeps(t *testing.T, cdc codec.Codec, desc *ServiceDesc) *serveDeps {
	t.Helper()

	d := rpcruntime.NewDispatcher()
	if desc != nil {
		d.Register(desc.ServiceID, newServiceHandler(registeredService{desc: desc}, cdc, nil))
	}

	return newServeDeps(nil, cdc, d, nil, nil, nil)
}

// countingCodec records how many times its decode ran, so a test can assert a
// decode did NOT happen rather than only that its result was unused.
type countingCodec struct {
	codec.Proto
	decodes atomic.Int64
}

func (c *countingCodec) Unmarshal(data []byte, m proto.Message) error {
	c.decodes.Add(1)

	return c.Proto.Unmarshal(data, m)
}

// rejectingCodec refuses every payload, standing in for a peer that published
// bytes this side's chosen message type cannot make sense of.
type rejectingCodec struct{ codec.Proto }

func (rejectingCodec) Unmarshal([]byte, proto.Message) error {
	return errors.New("these bytes are not a Take request")
}

// panickingCodec panics inside the decode, the failure mode a peer can provoke
// by publishing a payload the codec chokes on.
type panickingCodec struct{ codec.Proto }

func (panickingCodec) Unmarshal([]byte, proto.Message) error { panic("decode boom") }

// Test the plugin receiving through a borrowed view only when the negotiated
// codec leaves the decoded request owning its bytes.
//
// Both halves are load-bearing. Without a view-capable transport there is
// nothing to borrow. Without the codec's ownership promise the decoded request
// would keep pointing into the host's arena, which the transport hands back the
// moment the callback returns — so the borrow is simply not taken and every
// frame arrives as a private copy, exactly as before.
func TestServeViewReceiver_TakesTheBorrow_OnlyWhenTheCodecOwnsItsDecodedBytes(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	_, udsPlugin := newInProcessTransportPairForTest(t)

	require.NotNil(t, serveViewReceiver(pair.Plugin, codec.Proto{}),
		"a view-capable transport and an owning codec take the borrow")
	require.Nil(t, serveViewReceiver(pair.Plugin, lazyCodec{}),
		"a codec that decodes lazily must never be handed the peer's memory")
	require.Nil(t, serveViewReceiver(udsPlugin, codec.Proto{}),
		"a transport with nothing to lend receives the ordinary way")
}

// lazyCodec answers the ownership question with no, which is what any codec that
// does not implement codec.OwningUnmarshaler is assumed to mean.
type lazyCodec struct{ codec.Proto }

func (lazyCodec) DecodedMessageOwnsBytes() bool { return false }

// Test a unary request's frame carrying no payload onward once its request has
// been decoded.
//
// The decode is the payload's one and only reader. Dropping the slice is what
// makes that true by construction instead of by audit: nothing downstream — not
// dispatch, not the reply path, not a diagnostic — can read a slab the transport
// has since handed back to the peer (shm-abi.md §9).
func TestPrepareInboundFrame_CarriesNoPayloadOnward_ForAUnaryRequest(t *testing.T) {
	// Given a unary request whose payload is a live decodable body.
	cdc := codec.Proto{}
	deps := newViewTestDeps(t, cdc, newViewTestService(func(*wrapperspb.BytesValue) {}))
	body, err := cdc.Marshal(wrapperspb.Bytes([]byte("borrowed bytes")))
	require.NoError(t, err)
	f := transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID, Payload: body,
	}

	// When the receive path prepares it.
	out, req := prepareInboundFrame(deps, f, true)

	// Then the request was decoded and the frame carries none of the bytes it was
	// decoded from.
	require.Nil(t, out.Payload, "a consumed request payload must not travel past the borrow")
	require.Empty(t, req.DecodeFault)
	decoded, ok := req.Msg.(*wrapperspb.BytesValue)
	require.True(t, ok, "the decode must yield the message NewRequest built")
	require.Equal(t, []byte("borrowed bytes"), decoded.GetValue())
}

// Test a borrowed stream payload being copied before it travels on.
//
// A stream payload crosses to the goroutine that reads the stream, long after
// this frame's memory is gone, so this copy is the only one it gets — the
// transport stopped making its own, and lends streaming payloads like any other.
// A callback that queued one without copying would hand a consumer the peer's
// recycled slab.
func TestPrepareInboundFrame_CopiesABorrowedStreamPayload(t *testing.T) {
	deps := newViewTestDeps(t, codec.Proto{}, nil)
	lent := []byte("a stream message the lender will reclaim")
	f := transport.Frame{CallID: 2, Kind: transport.FrameStreamMsg, Control: 1, Payload: lent}

	// When a borrowed stream frame is prepared, the payload is copied out...
	out, _ := prepareInboundFrame(deps, f, true)
	require.Equal(t, lent, out.Payload, "the copy must say what arrived")
	require.NotSame(t, &lent[0], &out.Payload[0], "a queued stream payload must not alias the lender's memory")

	// ...and the lender overwriting its buffer leaves the copy intact.
	for i := range lent {
		lent[i] = 0xEE
	}
	require.Equal(t, []byte("a stream message the lender will reclaim"), out.Payload)

	// A frame that was never borrowed needs no copy: its bytes are already private.
	private := []byte("private bytes")
	out, _ = prepareInboundFrame(deps, transport.Frame{
		CallID: 3, Kind: transport.FrameStreamMsg, Control: 1, Payload: private,
	}, false)
	require.Same(t, &private[0], &out.Payload[0], "a private payload is not copied a second time")
}

// Test that a borrowed STREAM_CHUNK fragment gets the identical copy
// prepareInboundFrame gives every other stream kind (isStreamKind covers
// FrameStreamChunk exactly as it does FrameStreamMsg). The copy is what
// reassembly then appends to a pending logical message, which outlives the
// lender's frame by many more frames than a delivered STREAM_MSG does, and it
// must hold whatever the connection's chunking policy decides: a fragment's
// bytes must never alias reclaimed transport memory from the moment they are
// received, including on a connection where the fragment is a violation.
func TestPrepareInboundFrame_CopiesABorrowedStreamChunkPayload(t *testing.T) {
	// Given a borrowed STREAM_CHUNK frame.
	deps := newViewTestDeps(t, codec.Proto{}, nil)
	lent := []byte("a fragment the lender will reclaim")
	f := transport.Frame{CallID: 2, Kind: transport.FrameStreamChunk, Control: 1, Payload: lent}

	// When it is prepared.
	out, _ := prepareInboundFrame(deps, f, true)

	// Then the payload is copied out, not aliased...
	require.Equal(t, lent, out.Payload, "the copy must say what arrived")
	require.NotSame(t, &lent[0], &out.Payload[0], "a queued fragment must not alias the lender's memory")

	// ...proven by the lender overwriting its buffer leaving the copy intact.
	for i := range lent {
		lent[i] = 0xEE
	}
	require.Equal(t, []byte("a fragment the lender will reclaim"), out.Payload)
}

// Test a frame whose service or method resolves to nothing skipping the decode
// entirely — not decoding into a throwaway message, and not reading the payload
// at all.
//
// The status such a frame is answered with needs no request, so constructing or
// decoding one would be work spent on the receive goroutine, which holds up every
// later inbound frame while it runs.
func TestDecodeUnaryRequest_SkipsTheDecodeEntirely_ForAnUnknownServiceOrMethod(t *testing.T) {
	for _, tc := range []struct {
		name              string
		service, method   uint64
		registerTheServic bool
	}{
		{name: "an unregistered service", service: fnv64a("no.Such"), method: viewMethodID},
		{name: "a method the service does not have", service: viewServiceID, method: fnv64a("NoSuch"),
			registerTheServic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cdc := &countingCodec{}
			var desc *ServiceDesc
			if tc.registerTheServic {
				desc = newViewTestService(func(*wrapperspb.BytesValue) {})
			}
			deps := newViewTestDeps(t, cdc, desc)

			// When a frame naming it is prepared.
			_, req := prepareInboundFrame(deps, transport.Frame{
				CallID: 1, Kind: transport.FrameUnaryReq,
				Service: tc.service, Method: tc.method, Payload: []byte("never read"),
			}, true)

			// Then nothing was decoded, and no decode fault was invented for it either:
			// dispatch answers such a frame with its own not-found classification.
			require.Zero(t, cdc.decodes.Load(), "no decode may run for a frame that resolves to no method")
			require.Nil(t, req.Msg)
			require.Empty(t, req.DecodeFault)
		})
	}
}

// Test a payload the codec rejects becoming an answerable failure rather than a
// declined frame, and carrying out text rather than the decoder's own error.
//
// The text matters as much as the failure: this runs with the payload still
// borrowed, and an error a decoder built from the bytes it was handed would carry
// a reference to them into a value dispatch keeps well past the point the
// transport reclaims them.
func TestDecodeUnaryRequest_ReportsAnAnswerableFault_WhenTheCodecRejectsThePayload(t *testing.T) {
	deps := newViewTestDeps(t, rejectingCodec{}, newViewTestService(func(*wrapperspb.BytesValue) {}))

	_, req := prepareInboundFrame(deps, transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
		Payload: []byte("not a Take request"),
	}, true)

	require.Nil(t, req.Msg, "a rejected decode yields no request")
	require.Contains(t, req.DecodeFault, "these bytes are not a Take request")
}

// Test a codec that panics mid-decode being contained where it happens, so the
// panic becomes the same answerable failure a returned decode error is.
//
// Two things ride on the containment. The bytes are the peer's choice, so an
// uncontained panic would let a peer end this process by publishing a payload the
// codec chokes on. And the panic would unwind out of the consume callback past
// the ring-head advance its caller owes, stranding the slot and its slab for the
// region's lifetime (shm-abi.md §9's protected consume).
func TestDecodeUnaryRequest_ContainsACodecPanic_AndReportsItAsAnAnswerableFault(t *testing.T) {
	deps := newViewTestDeps(t, panickingCodec{}, newViewTestService(func(*wrapperspb.BytesValue) {}))

	require.NotPanics(t, func() {
		_, req := prepareInboundFrame(deps, transport.Frame{
			CallID: 1, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
			Payload: []byte("whatever the codec chokes on"),
		}, true)

		require.Nil(t, req.Msg)
		require.Contains(t, req.DecodeFault, "decode boom")
	})
}

// churnSlabs writes over every slab in the size class len(over) bytes fall into,
// by publishing more frames than the class has slabs, and waits until the peer
// has consumed all of them. Each frame names no live call, so it is consumed and
// dropped, which is exactly what releases its slab back for the next to reuse.
//
// It is what makes a lending bug visible: cross-process slab reuse is invisible
// to the race detector, so only the peer genuinely taking its memory back shows
// whether a retained value was ever really a copy.
// It returns the byte pattern it wrote, so a test can assert what an aliased value
// reads afterwards rather than only that it changed.
func churnSlabs(ctx context.Context, t *testing.T, pair *shmtest.Pair, over []byte, alreadyReceived uint64) []byte {
	t.Helper()

	const churn = 320 // above the pair's per-class slab count, so the class wraps
	recycled := make([]byte, len(over))
	for i := range recycled {
		recycled[i] = 0xEE
	}
	for i := range churn {
		require.NoError(t, pair.Host.Send(ctx, transport.Frame{
			CallID: uint64(100000 + i), Kind: transport.FrameUnaryResp, Payload: recycled,
		}))
	}

	counter, ok := any(pair.Plugin).(transport.FrameCounter)
	require.True(t, ok, "the shared-memory transport must report frame progress")
	require.Eventually(t, func() bool { return counter.FramesReceived() >= alreadyReceived+churn },
		30*time.Second, time.Millisecond, "the serve loop must drain every published frame")

	return recycled
}

// Test that a unary request is decoded straight out of the bytes the receive path
// was lent, with no defensive copy in between — and, in the same motion, what the
// codec's ownership promise is actually load-bearing for.
//
// The two arms differ only in the codec. Under the production codec the request
// the handler holds keeps saying what arrived after the host has written over
// every slab in its class, because the decode copied. Under a codec that claims
// ownership and aliases anyway, that same request reads whatever the host wrote
// next — the peer's recycled slab here, and unmapped memory after teardown.
//
// The aliasing arm is also what pins the ABSENCE of a copy: on a receive path
// that copied the payload before decoding, the aliased message would point at
// that private copy and keep saying what it said at handler entry, so the arm
// would fail — and that copy is exactly the cost the borrow exists to remove.
//
// Both arms compare against what the request said AT HANDLER ENTRY, not against
// the bytes the host sent. An aliasing decode of a length-prefixed body yields
// the whole encoded payload rather than the field inside it, so a comparison
// against the sent bytes would differ under every implementation and prove
// nothing.
func TestRunServeLoop_DecodesTheRequestFromTheLentBytes(t *testing.T) {
	sent := []byte("request bytes the host is about to reclaim")

	for _, tc := range []struct {
		name string
		cdc  codec.Codec
		owns bool // whether the codec's ownership promise is true
	}{
		{name: "a codec that owns what it decodes", cdc: codec.Proto{}, owns: true},
		{name: "a codec that claims ownership and aliases", cdc: aliasingCodec{}, owns: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
			require.NoError(t, err)

			// The handler records what its request says on entry and hands the message
			// itself back untouched: what this test asks is whether those same bytes still
			// say it LATER, after the host has taken its memory back.
			type entry struct {
				req  *wrapperspb.BytesValue
				said []byte
			}
			taken := make(chan entry, 1)
			desc := newViewTestService(func(req *wrapperspb.BytesValue) {
				taken <- entry{req: req, said: bytes.Clone(req.GetValue())}
			})
			d := rpcruntime.NewDispatcher()
			d.Register(desc.ServiceID, newServiceHandler(registeredService{desc: desc}, tc.cdc, nil))

			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = runServeLoop(context.Background(), pair.Plugin, tc.cdc, d, nil, nil, nil)
			}()
			t.Cleanup(func() { _ = pair.Close(); <-done })

			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()

			// When the host publishes a request the serve loop decodes from the lent bytes,
			// and then writes over every slab in that payload's class.
			body, err := codec.Proto{}.Marshal(wrapperspb.Bytes(sent))
			require.NoError(t, err)
			require.NoError(t, pair.Host.Send(ctx, transport.Frame{
				CallID: 1, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
				Payload: body,
			}))

			var got entry
			select {
			case got = <-taken:
			case <-ctx.Done():
				t.Fatal("the handler never received the request")
			}
			require.NotEmpty(t, got.said, "the handler must have been given a decoded request")

			recycled := churnSlabs(ctx, t, pair, body, 1)

			// Then the request still says what it said only if the decode owned its bytes.
			if tc.owns {
				require.Equal(t, sent, got.said, "a real decode yields the field, not the encoded body")
				require.Equal(t, got.said, got.req.GetValue(),
					"a decode that copies keeps saying what it said after the peer reclaims the slab")

				return
			}
			// Asserted as the exact recycled pattern rather than merely "changed": a
			// future receive path that zeroed or dropped the value would satisfy a
			// NotEqual while proving nothing about where the message was pointing.
			require.Equal(t, recycled[:len(got.said)], got.req.GetValue(),
				"an aliasing decode reads whatever the peer wrote into the slab it is still pointing at")
		})
	}
}

// Test the whole borrowed-stream-payload path on the PLUGIN side end to end: a
// real STREAM_MSG published by the host, delivered to the serve loop as a view of
// the host's arena, queued for the stream handler's goroutine, and still intact
// after the host has written over every slab in its size class.
//
// The serve loop's clone is the only copy a lent stream payload gets — the
// transport stopped making its own and now lends streaming payloads like any
// other — so this is the hazard the plugin side inherited whole when it started
// receiving through a view. It cannot be reproduced by driving the frame handler
// with a test-owned buffer, and the race detector cannot see it, because it is
// cross-process memory reuse rather than a Go data race.
func TestRunServeLoop_KeepsALentStreamPayload_AfterTheHostRecyclesEverySlab(t *testing.T) {
	// Given a plugin serving one stream handler over a real shared-memory pair.
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)

	const service, method = "view.Stream", "Hold"
	accepted := make(chan *Stream, 1)
	release := make(chan struct{})
	handlerRead := make(chan []byte, 1)
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(st *Stream) error {
			accepted <- st
			<-release
			got, rerr := st.RecvMsg(context.Background())
			if rerr != nil {
				return rerr
			}
			handlerRead <- bytes.Clone(got)

			return nil
		}},
	}
	srv := newStreamServer(pair.Plugin, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pair.Plugin, codec.Proto{}, rpcruntime.NewDispatcher(), srv, nil, nil)
	}()
	t.Cleanup(func() {
		close(release)
		_ = pair.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), pair.Host, codec.Proto{})
	st, err := cc.OpenStream(ctx, service, method)
	require.NoError(t, err)
	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal("the stream handler never accepted the stream")
	}

	// When the host sends a stream message the serve loop queues for that handler...
	delivered := []byte("a stream message the host is about to write over")
	require.NoError(t, st.SendMsg(ctx, delivered))

	// ...and then writes over every slab in that message's size class. The serve
	// loop is still consuming (the handler is what is parked, on its own goroutine),
	// so the churn is drained and its slabs really are reused.
	counter, ok := any(pair.Plugin).(transport.FrameCounter)
	require.True(t, ok, "the shared-memory transport must report frame progress")
	_ = churnSlabs(ctx, t, pair, delivered, counter.FramesReceived())

	// Then the queued message still says what the host sent, not what it wrote next.
	release <- struct{}{}
	select {
	case got := <-handlerRead:
		require.Equal(t, delivered, got,
			"a queued stream payload must be a copy: the host has since reused that slab")
	case <-ctx.Done():
		t.Fatal("the stream handler never read its message")
	}
}

// Test the plugin answering a request it could not decode, rather than declining
// the frame — over a real shared-memory region, where declining has consequences
// no unit test can show.
//
// Three of them, all asserted here. The host's call is terminated instead of
// waiting out a budget shm-abi.md §4 lets be zero (no deadline at all). The
// region's consume-fault counter stays at zero, so no run toward the transport's
// run-threshold teardown ever begins — a declining callback would tear the region
// down after enough consecutive failures, taking every call on it. And the plugin
// keeps serving: the next request succeeds on the same connection.
func TestRunServeLoop_AnswersAnUndecodableRequest_WithoutDecliningTheFrame(t *testing.T) {
	for _, tc := range []struct {
		name string
		cdc  codec.Codec
		want string
	}{
		{name: "a codec that rejects the payload", cdc: rejectingCodec{}, want: "these bytes are not a Take request"},
		{name: "a codec that panics on the payload", cdc: panickingCodec{}, want: "decode boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a plugin serving one method whose decode always fails.
			pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
			require.NoError(t, err)

			handlerRan := false
			desc := newViewTestService(func(*wrapperspb.BytesValue) { handlerRan = true })
			d := rpcruntime.NewDispatcher()
			d.Register(desc.ServiceID, newServiceHandler(registeredService{desc: desc}, tc.cdc, nil))

			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = runServeLoop(context.Background(), pair.Plugin, tc.cdc, d, nil, nil, nil)
			}()
			t.Cleanup(func() { _ = pair.Close(); <-done })

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			// When the host publishes a request with no deadline of its own.
			require.NoError(t, pair.Host.Send(ctx, transport.Frame{
				CallID: 7, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
				Payload: []byte("bytes the plugin cannot decode"),
			}))

			// Then the call is answered rather than stranded.
			reply, err := pair.Host.Recv(ctx)
			require.NoError(t, err)
			require.Equal(t, transport.FrameUnaryErr, reply.Kind, "an undecodable request is answered, never dropped")
			require.Equal(t, uint64(7), reply.CallID)
			require.NotNil(t, reply.Status)
			require.Equal(t, rpcruntime.StatusCodeInternal, reply.Status.Code)
			require.Contains(t, reply.Status.Message, tc.want)
			require.False(t, handlerRan, "an undecodable request must not reach the handler")

			// And the frame was accepted, not declined: no consume fault was counted, so
			// no run toward the region's teardown threshold ever started.
			faults, ok := any(pair.Plugin).(interface{ ConsumeFaults() uint64 })
			require.True(t, ok, "the shared-memory transport must report consume faults")
			require.Zero(t, faults.ConsumeFaults(),
				"answering a request is an acceptance; declining it would count a fault toward teardown")

			// And the plugin is still serving the same connection.
			require.NoError(t, pair.Host.Send(ctx, transport.Frame{
				CallID: 8, Kind: transport.FrameUnaryReq, Service: fnv64a("no.Such"), Method: viewMethodID,
			}))
			next, err := pair.Host.Recv(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(8), next.CallID, "the serve loop must keep serving after a failed decode")
			require.Equal(t, rpcruntime.StatusCodeServiceNotFound, next.Status.Code)
		})
	}
}

// Test a method descriptor written against an older contract — one with a
// handler but no NewRequest — being answered as unknown rather than dispatched
// with no request.
//
// The two functions are emitted together by the generator, so a missing one means
// the descriptor was hand-built. Answering terminates those calls at the host
// instead of hanging them, and it keeps a nil request away from the generated
// handler's type assertion.
func TestServiceHandler_TreatsAMethodWithNoRequestConstructor_AsUnknown(t *testing.T) {
	desc := &ServiceDesc{
		ServiceName: "legacy.Svc",
		ServiceID:   fnv64a("legacy.Svc"),
		Methods: []MethodDesc{{
			MethodName: "Take",
			MethodID:   viewMethodID,
			Handler: func(any, context.Context, proto.Message) (proto.Message, error) {
				return &wrapperspb.BytesValue{}, nil
			},
		}},
	}
	h := newServiceHandler(registeredService{desc: desc}, codec.Proto{}, nil)

	// Asserted before the constructor call below, so a handler that stopped refusing
	// such a descriptor fails here on the classification rather than on the nil call
	// NewRequest would then make.
	_, status, err := h.Handle(t.Context(), viewMethodID, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, rpcruntime.StatusCodeMethodNotFound, status.Code)

	msg, ok := h.NewRequest(viewMethodID)
	require.False(t, ok, "a method with no request constructor has nothing to decode into")
	require.Nil(t, msg)
}

// Test a known method dispatched with no decoded request being answered with an
// internal status rather than reaching the generated handler.
//
// A generated handler asserts the concrete type of the message its own
// NewRequest built, so a nil request would panic inside it. Refusing here makes
// that unreachable and terminates the call, which is what a caller with no
// deadline needs.
func TestServiceHandler_AnswersInternalStatus_WhenNoRequestWasDecoded(t *testing.T) {
	ran := false
	desc := newViewTestService(func(*wrapperspb.BytesValue) { ran = true })
	h := newServiceHandler(registeredService{desc: desc}, codec.Proto{}, nil)

	_, status, err := h.Handle(t.Context(), viewMethodID, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, rpcruntime.StatusCodeInternal, status.Code)
	require.Contains(t, status.Message, "no request was decoded")
	require.False(t, ran, "the generated handler must never be reached without its request")
}

// Test a request constructor that panics being contained exactly as a panicking
// decode is: answered, head advanced, plugin still serving, and no consume fault
// counted against the region.
//
// MethodDesc.NewRequest is a public field. Generated bodies are composite literals
// and cannot panic, but a hand-written ServiceDesc can put anything there, which
// makes it the one call on the receive path that runs code the framework did not
// write. Left outside the fault barrier it would cost all three of the guarantees
// this path exists for at once — the ring head would not advance through this
// side's own barrier, the host's call would be left with nothing to reap it, and
// repeated it would extend the consume-fault run to a region teardown.
func TestRunServeLoop_ContainsAPanickingRequestConstructor(t *testing.T) {
	// Given a plugin whose one method's request constructor panics.
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)

	handlerRan := false
	desc := &ServiceDesc{
		ServiceName: "view.Svc",
		ServiceID:   viewServiceID,
		Methods: []MethodDesc{{
			MethodName: "Take",
			MethodID:   viewMethodID,
			NewRequest: func() proto.Message { panic("constructor boom") },
			Handler: func(any, context.Context, proto.Message) (proto.Message, error) {
				handlerRan = true

				return &wrapperspb.BytesValue{}, nil
			},
		}},
	}
	d := rpcruntime.NewDispatcher()
	d.Register(desc.ServiceID, newServiceHandler(registeredService{desc: desc}, codec.Proto{}, nil))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pair.Plugin, codec.Proto{}, d, nil, nil, nil)
	}()
	t.Cleanup(func() { _ = pair.Close(); <-done })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// When the host publishes a request against it, with no deadline of its own.
	require.NoError(t, pair.Host.Send(ctx, transport.Frame{
		CallID: 3, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
		Payload: []byte("a request whose constructor will panic"),
	}))

	// Then the call is answered rather than stranded.
	reply, err := pair.Host.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, transport.FrameUnaryErr, reply.Kind)
	require.Equal(t, uint64(3), reply.CallID)
	require.NotNil(t, reply.Status)
	require.Equal(t, rpcruntime.StatusCodeInternal, reply.Status.Code)
	require.Contains(t, reply.Status.Message, "constructor boom")
	require.False(t, handlerRan)

	// And the frame was accepted, not declined: nothing counted toward the region's
	// consume-fault teardown threshold.
	faults, ok := any(pair.Plugin).(interface{ ConsumeFaults() uint64 })
	require.True(t, ok, "the shared-memory transport must report consume faults")
	require.Zero(t, faults.ConsumeFaults(),
		"a panicking constructor must be answered, not declined into the escalation run")

	// And the plugin is still serving the same connection.
	require.NoError(t, pair.Host.Send(ctx, transport.Frame{
		CallID: 4, Kind: transport.FrameUnaryReq, Service: fnv64a("no.Such"), Method: viewMethodID,
	}))
	next, err := pair.Host.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(4), next.CallID)
	require.Equal(t, rpcruntime.StatusCodeServiceNotFound, next.Status.Code)
}

// hugeErrorCodec fails every decode with an error far longer than any status reply
// should carry.
//
// The rune is deliberately three bytes wide. maxDecodeFaultBytes is 512, which is
// not a multiple of three, so the byte the bound lands on is mid-rune and the
// truncation's backoff is actually exercised — with a one- or two-byte rune the
// bound would already fall on a rune start and the backoff would never run, leaving
// the test green with that half of the production code deleted.
type hugeErrorCodec struct{ codec.Proto }

func (hugeErrorCodec) Unmarshal([]byte, proto.Message) error {
	return errors.New(strings.Repeat("€", 4096) + "tail")
}

// Test a decode fault's text being bounded before it becomes a status reply, and cut
// at a rune boundary when it is.
//
// A decoder is free to return an error of any length — one that embeds the payload
// it was handed renders the whole frame — and that text becomes a frame this plugin
// must be able to send. Unbounded, one undecodable request could produce a reply too
// large to publish, whose send failure stops the serve loop: a dead session from a
// single bad frame.
func TestDecodeUnaryRequest_BoundsTheFaultText(t *testing.T) {
	deps := newViewTestDeps(t, hugeErrorCodec{}, newViewTestService(func(*wrapperspb.BytesValue) {}))

	_, req := prepareInboundFrame(deps, transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
		Payload: []byte("rejected"),
	}, true)

	const elision = "... (truncated)"
	require.True(t, strings.HasSuffix(req.DecodeFault, elision), "a clipped reason must say so")
	require.LessOrEqual(t, len(req.DecodeFault), maxDecodeFaultBytes+len(elision))

	// The kept prefix is strictly shorter than the bound: the bound lands mid-rune, so
	// a cut that did not back off would keep exactly maxDecodeFaultBytes bytes and
	// split a rune that arrived whole.
	kept := strings.TrimSuffix(req.DecodeFault, elision)
	require.Less(t, len(kept), maxDecodeFaultBytes, "the cut must back off to a rune start")
	require.True(t, utf8.ValidString(req.DecodeFault), "the cut must not split a rune that was whole")
}

// Test the frame the serve loop leaves in its cleared receive scratch routing
// nowhere.
//
// The scratch is only ever read after a receive that overwrote it, so this value is
// not reachable today. It is chosen anyway for what it would do if that ever changed:
// a zero transport.Frame is a FrameUnaryReq, so clearing to the zero value would
// leave a synthetic request for call 0 of service 0 — which the loop would answer
// with an unsolicited status frame rather than ignore. This pins the sentinel against
// every arm of the loop's routing switch.
func TestClearReceiveScratch_LeavesAFrameThatRoutesNowhere(t *testing.T) {
	deps := newServeDeps(nil, codec.Proto{}, rpcruntime.NewDispatcher(), nil, nil, nil)
	deps.frame = transport.Frame{CallID: 9, Kind: transport.FrameUnaryReq, Payload: []byte("stale")}
	deps.req = rpcruntime.Request{Msg: &wrapperspb.BytesValue{}}

	deps.clearReceiveScratch()

	require.Zero(t, deps.req, "a cleared scratch carries no request")
	require.Nil(t, deps.frame.Payload)
	require.NotEqual(t, transport.FrameUnaryReq, deps.frame.Kind,
		"a cleared frame must not read as a request the loop would dispatch")
	require.False(t, isStreamKind(deps.frame.Kind), "nor as a stream frame the stream half would route")
	require.NotEqual(t, transport.FrameCancel, deps.frame.Kind, "nor as a cancel")
	require.Empty(t,
		rpcruntime.NewDispatcher().Dispatch(t.Context(), deps.frame, rpcruntime.Request{}, time.Now()),
		"the dispatcher must discard it, so nothing is ever sent for a frame that was never received")
}

// unrenderablePanic panics again while being rendered, and panics with itself, so the
// second render panics too.
//
// That second level is the point. fmt catches a panic in a value's own String method
// once and prints a placeholder, but re-panics when rendering THAT panic value panics
// in turn — and the render runs inside the deferred recover that is the decode fault
// barrier, so a re-panic there unwinds straight out of the consume callback.
type unrenderablePanic struct{}

func (unrenderablePanic) String() string { panic(unrenderablePanic{}) }

// Test a panic value that cannot be rendered still being contained and answered.
//
// This is the last way a hand-written descriptor could reach the outcomes the fault
// barrier exists to prevent: not by panicking, which the barrier catches, but by
// panicking with something whose rendering panics — inside the barrier's own recover,
// where nothing else is watching.
func TestRunServeLoop_ContainsAPanicValueThatCannotBeRendered(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)

	desc := &ServiceDesc{
		ServiceName: "view.Svc",
		ServiceID:   viewServiceID,
		Methods: []MethodDesc{{
			MethodName: "Take",
			MethodID:   viewMethodID,
			NewRequest: func() proto.Message { panic(unrenderablePanic{}) },
			Handler: func(any, context.Context, proto.Message) (proto.Message, error) {
				return &wrapperspb.BytesValue{}, nil
			},
		}},
	}
	d := rpcruntime.NewDispatcher()
	d.Register(desc.ServiceID, newServiceHandler(registeredService{desc: desc}, codec.Proto{}, nil))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pair.Plugin, codec.Proto{}, d, nil, nil, nil)
	}()
	t.Cleanup(func() { _ = pair.Close(); <-done })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	require.NoError(t, pair.Host.Send(ctx, transport.Frame{
		CallID: 5, Kind: transport.FrameUnaryReq, Service: viewServiceID, Method: viewMethodID,
		Payload: []byte("a request whose panic value cannot be rendered"),
	}))

	reply, err := pair.Host.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, transport.FrameUnaryErr, reply.Kind)
	require.Equal(t, uint64(5), reply.CallID)
	require.NotNil(t, reply.Status)
	require.Equal(t, rpcruntime.StatusCodeInternal, reply.Status.Code)
	require.Contains(t, reply.Status.Message, "unrenderable",
		"the fault names what it could not render, by type rather than by value")

	faults, ok := any(pair.Plugin).(interface{ ConsumeFaults() uint64 })
	require.True(t, ok, "the shared-memory transport must report consume faults")
	require.Zero(t, faults.ConsumeFaults(),
		"an unrenderable panic value must be answered, not declined into the escalation run")
}
