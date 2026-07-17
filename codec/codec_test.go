package codec_test

import (
	"testing"

	"github.com/arloliu/styx/codec"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Test Proto codec round-tripping a message through Marshal/Unmarshal
func TestProto_RoundTrip_PreservesMessage(t *testing.T) {
	// Given
	c := codec.Proto{}
	inner, err := anypb.New(wrapperspb.String("payload"))
	require.NoError(t, err)

	// When
	data, err := c.Marshal(inner)
	require.NoError(t, err)
	got := &anypb.Any{}
	err = c.Unmarshal(data, got)

	// Then
	require.NoError(t, err)
	require.True(t, proto.Equal(inner, got))
}

// Test Proto.Name reporting the codec identifier used in handshake negotiation
func TestProto_Name_ReturnsProtoIdentifier(t *testing.T) {
	// Given
	c := codec.Proto{}

	// When / Then
	require.Equal(t, "proto", c.Name())
}
