package transport_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// The shared consume barrier: which of the three dispositions each callback
// outcome lands on, what the fault report carries out of it, and what it
// deliberately does not carry out — no value the callback produced, only text
// rendered from one while the frame's memory was still valid.

// aliasingError is an error whose message is the frame's payload verbatim. It
// stands the callback that hands transport-owned memory to its caller: the
// barrier must render it and keep no reference to the bytes.
type aliasingError struct{ body []byte }

func (e *aliasingError) Error() string { return string(e.body) }

// panickingError is an error whose own rendering panics, the case where
// reporting the fault is itself a fault.
type panickingError struct{ peer bool }

func (e *panickingError) Error() string { panic("rendering this error panics") }

func (e *panickingError) Is(target error) bool {
	return e.peer && errors.Is(target, transport.ErrPayloadMalformed)
}

// Test that a callback returning nil accepts the frame.
func TestProtectedConsume_AcceptsTheFrame_WhenTheCallbackSucceeds(t *testing.T) {
	// Given
	f := transport.Frame{CallID: 7, Kind: transport.FrameUnaryResp, Payload: []byte("body")}
	var got []byte

	// When
	disp, err := transport.ProtectedConsume(f, func(in transport.Frame) error {
		got = append(got, in.Payload...)

		return nil
	})

	// Then
	require.Equal(t, transport.ConsumeAccepted, disp)
	require.NoError(t, err)
	require.Equal(t, []byte("body"), got)
}

// Test that an ordinary callback error is this side's fault: it names the call
// and the kind, is not marked as a panic, and carries no stack.
func TestProtectedConsume_ReportsThisSidesFault_WhenTheCallbackDeclines(t *testing.T) {
	// Given
	f := transport.Frame{CallID: 42, Kind: transport.FrameUnaryResp}

	// When
	disp, err := transport.ProtectedConsume(f, func(transport.Frame) error {
		return errors.New("the delivery queue is full")
	})

	// Then
	require.Equal(t, transport.ConsumeFaulted, disp)
	require.ErrorIs(t, err, transport.ErrConsumeFault)

	var fault *transport.ConsumeFaultError
	require.ErrorAs(t, err, &fault)
	require.Equal(t, uint64(42), fault.CallID)
	require.Equal(t, transport.FrameUnaryResp, fault.Kind)
	require.False(t, fault.Panicked)
	require.Nil(t, fault.Stack)
	require.Contains(t, fault.Detail, "the delivery queue is full")
}

// Test that a callback naming the peer's bytes blames the peer, on the one
// sentinel that decides it.
func TestProtectedConsume_BlamesThePeer_WhenTheCallbackNamesMalformedBytes(t *testing.T) {
	// Given
	f := transport.Frame{CallID: 9, Kind: transport.FrameUnaryResp}

	// When
	disp, err := transport.ProtectedConsume(f, func(transport.Frame) error {
		return fmt.Errorf("field 3: %w", transport.ErrPayloadMalformed)
	})

	// Then
	require.Equal(t, transport.ConsumeMalformed, disp)
	require.ErrorIs(t, err, transport.ErrPayloadMalformed)
	require.NotErrorIs(t, err, transport.ErrConsumeFault)
	require.Contains(t, err.Error(), "field 3")
}

// Test that a panicking callback is the same fault arrived at less politely: it
// is contained, marked, and carries the stack of the code that panicked.
func TestProtectedConsume_ContainsAPanickingCallback_AndCarriesItsStack(t *testing.T) {
	// Given
	f := transport.Frame{CallID: 11, Kind: transport.FrameUnaryReq}

	// When
	disp, err := transport.ProtectedConsume(f, func(transport.Frame) error {
		panic("decoder bug")
	})

	// Then
	require.Equal(t, transport.ConsumeFaulted, disp)

	var fault *transport.ConsumeFaultError
	require.ErrorAs(t, err, &fault)
	require.Equal(t, uint64(11), fault.CallID)
	require.True(t, fault.Panicked)
	require.NotEmpty(t, fault.Stack)
	require.Contains(t, fault.Detail, "decoder bug")
}

// Test that nothing the callback produced leaves the barrier as an object: an
// error aliasing the payload comes out as text, and the payload it aliased is
// not reachable from the result.
func TestProtectedConsume_RendersTheFaultToText_WhenTheErrorAliasesThePayload(t *testing.T) {
	// Given
	body := []byte("payload bytes the transport owns")
	f := transport.Frame{CallID: 3, Kind: transport.FrameUnaryResp, Payload: body}

	// When
	_, err := transport.ProtectedConsume(f, func(in transport.Frame) error {
		return &aliasingError{body: in.Payload}
	})

	// Then
	var fault *transport.ConsumeFaultError
	require.ErrorAs(t, err, &fault)
	require.Contains(t, fault.Detail, "payload bytes the transport owns")

	// The rendered detail must survive the bytes being recycled under it, which is
	// what proves it is text rather than a window onto the frame.
	for i := range body {
		body[i] = 'x'
	}
	require.Contains(t, fault.Detail, "payload bytes the transport owns")

	var aliasing *aliasingError
	require.NotErrorAs(t, err, &aliasing, "the callback's own error value escaped the barrier")
}

// Test that a fault whose own rendering panics is still contained, and that the
// panic does not re-attribute a peer fault to this side.
func TestProtectedConsume_KeepsTheAttribution_WhenReportingTheFaultPanics(t *testing.T) {
	cases := []struct {
		name string
		peer bool
		want transport.ConsumeDisposition
	}{
		{"peer fault", true, transport.ConsumeMalformed},
		{"this side's fault", false, transport.ConsumeFaulted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			f := transport.Frame{CallID: 5, Kind: transport.FrameUnaryResp}

			// When
			disp, err := transport.ProtectedConsume(f, func(transport.Frame) error {
				return &panickingError{peer: tc.peer}
			})

			// Then
			require.Equal(t, tc.want, disp)
			require.Error(t, err)
			if tc.want == transport.ConsumeMalformed {
				require.ErrorIs(t, err, transport.ErrPayloadMalformed)
			} else {
				require.ErrorIs(t, err, transport.ErrConsumeFault)
			}
		})
	}
}

// Test that the rendered reason is bounded, so a callback that renders its whole
// payload cannot turn a large frame into a multi-megabyte error.
func TestProtectedConsume_BoundsTheRenderedReason_WhateverTheFrameSize(t *testing.T) {
	// Given
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryResp}
	huge := strings.Repeat("a", 64<<10)

	// When
	_, err := transport.ProtectedConsume(f, func(transport.Frame) error {
		return errors.New(huge)
	})

	// Then
	var fault *transport.ConsumeFaultError
	require.ErrorAs(t, err, &fault)
	require.LessOrEqual(t, len(fault.Detail), transport.FaultDetailMaxBytes+len("... (truncated)"))
	require.Contains(t, fault.Detail, "(truncated)")
}

// Test that truncation backs off to a rune start, so a reason clipped mid-rune
// never carries half of one out.
func TestTruncateFaultDetail_CutsAtARuneStart(t *testing.T) {
	// Given: multi-byte runes straddling the bound.
	s := strings.Repeat("é", transport.FaultDetailMaxBytes)

	// When
	got := transport.TruncateFaultDetail(s)

	// Then
	require.True(t, strings.HasSuffix(got, "... (truncated)"))
	require.NotContains(t, strings.TrimSuffix(got, "... (truncated)"), "�")
}
