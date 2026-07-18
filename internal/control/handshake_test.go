package control_test

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/arloliu/styx/internal/control"
	"github.com/stretchr/testify/require"
)

// Test Negotiate selecting the highest common protocol version and codec/transport
func TestNegotiate_SelectsHighestCommonVersion_OnValidOffers(t *testing.T) {
	// Given
	host := control.Offer{ProtocolMin: 1, ProtocolMax: 3, Transports: []string{"uds"}, Codecs: []string{"proto"}}
	plugin := control.Offer{ProtocolMin: 2, ProtocolMax: 4, Transports: []string{"uds"}, Codecs: []string{"proto"}}

	// When
	tuple, err := control.Negotiate(host, plugin, nil)

	// Then
	require.NoError(t, err)
	require.EqualValues(t, 3, tuple.ProtocolVersion) // max(2,1)..min(3,4) == [2,3], highest common = 3
	require.Equal(t, "uds", tuple.Transport)
	require.Equal(t, "proto", tuple.Codec)
}

// Test Negotiate failing closed on every negotiation failure mode
func TestNegotiate_ReturnsIncompatibleError_OnEachFailureMode(t *testing.T) {
	cases := []struct {
		name           string
		host, plugin   control.Offer
		pluginServices []control.ServiceVersion
		wantReasonHas  string
	}{
		{
			name: "empty protocol range intersection",
			host: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
			},
			plugin: control.Offer{
				ProtocolMin: 2, ProtocolMax: 2, Transports: []string{"uds"}, Codecs: []string{"proto"},
			},
			wantReasonHas: "protocol range",
		},
		{
			name: "no common transport",
			host: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
			},
			plugin: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"shm"}, Codecs: []string{"proto"},
			},
			wantReasonHas: "transport",
		},
		{
			name: "no common codec",
			host: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
			},
			plugin: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"cbor"},
			},
			wantReasonHas: "codec",
		},
		{
			name: "host requires unsupported feature",
			host: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
				Features: []control.FeatureFlag{{Name: "trace_context", Required: true}},
			},
			plugin: control.Offer{
				ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
			},
			wantReasonHas: "trace_context",
		},
		{
			name: "plugin service version outside host's required range",
			host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
				Services: []control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 2, MaxVersion: 2}}},
			plugin: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"},
				Codecs: []string{"proto"}},
			pluginServices: []control.ServiceVersion{{Service: "echo.Echo", Version: 1}},
			wantReasonHas:  "version 1 outside required range",
		},
		{
			name: "required service not advertised by plugin at all",
			host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
				Services: []control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 1, MaxVersion: 1}}},
			plugin: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"},
				Codecs: []string{"proto"}},
			pluginServices: nil, // plugin advertises no services — must say "not advertised", never "version 0"
			wantReasonHas:  "not advertised",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			_, err := control.Negotiate(tc.host, tc.plugin, tc.pluginServices)

			// Then
			var incompatErr *control.IncompatibleError
			require.ErrorAs(t, err, &incompatErr)
			require.Contains(t, incompatErr.Reason, tc.wantReasonHas)
		})
	}
}

// Test Negotiate failing closed when the PLUGIN requires a feature the host
// does not support — the mirror of the host-requires-unsupported case, so
// neither side's required flag can be silently ignored.
func TestNegotiate_FailsClosed_WhenPluginRequiresFeatureHostMissing(t *testing.T) {
	// Given: the plugin requires "trace_context"; the host offers no features.
	host := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}}
	plugin := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
		Features: []control.FeatureFlag{{Name: "trace_context", Required: true}}}

	// When
	_, err := control.Negotiate(host, plugin, nil)

	// Then
	var incompatErr *control.IncompatibleError
	require.ErrorAs(t, err, &incompatErr)
	require.Contains(t, incompatErr.Reason, "trace_context")
	require.Contains(t, incompatErr.Reason, "required by plugin")
}

// Test a Negotiate failure populating IncompatibleError with BOTH sides'
// full offers, not merely a reason string — so a caller can inspect exactly
// what each side proposed.
func TestNegotiate_PopulatesBothOffers_OnIncompatibleError(t *testing.T) {
	// Given: offers that share no transport, so Negotiate fails.
	host := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}}
	plugin := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"shm"}, Codecs: []string{"proto"}}

	// When
	_, err := control.Negotiate(host, plugin, nil)

	// Then: the error carries each side's offer verbatim.
	var incompatErr *control.IncompatibleError
	require.ErrorAs(t, err, &incompatErr)
	require.Equal(t, host, incompatErr.HostOffer)
	require.Equal(t, plugin, incompatErr.PluginOffer)
}

// Test Offer -> Hello -> Offer round-tripping every field a Hello carries
// (protocol range, transports, codecs, features, host service requirements)
// back to an equal Offer — the wire projection is lossless for the fields
// the Offer model keeps (the Supported echo bit, which HelloToOffer drops by
// design, is not part of an Offer).
func TestOfferToHello_HelloToOffer_RoundTripsOffer(t *testing.T) {
	// Given
	original := control.Offer{
		ProtocolMin: 1, ProtocolMax: 3,
		Transports: []string{"uds", "shm"},
		Codecs:     []string{"proto", "cbor"},
		Features:   []control.FeatureFlag{{Name: "trace_context", Required: true}, {Name: "checksum", Required: false}},
		Services:   []control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 1, MaxVersion: 2}},
	}

	// When
	got := control.HelloToOffer(control.OfferToHello(original, 0xABCD))

	// Then
	require.Equal(t, original, got)
}

// Test Tuple -> HelloAck -> Tuple round-tripping the negotiated tuple (both
// sides must reconstruct the identical Tuple). Identity and service
// versions ride the same ack but are not part of the Tuple, so they don't
// affect this equality.
func TestTupleToHelloAck_HelloAckToTuple_RoundTripsTuple(t *testing.T) {
	// Given
	original := control.Tuple{
		ProtocolVersion: 2,
		Transport:       "uds",
		LayoutVersion:   0,
		Codec:           "proto",
		Features:        map[string]bool{"trace_context": true, "checksum": false},
	}

	// When
	ack := control.TupleToHelloAck(original, 0xABCD, control.PluginIdentity{Name: "p"},
		[]control.ServiceVersion{{Service: "echo.Echo", Version: 1}})
	got := control.HelloAckToTuple(ack)

	// Then
	require.Equal(t, original, got)
}

// Test Negotiate treating an unsupported OPTIONAL feature as a non-error false entry
func TestNegotiate_AllowsUnsupportedOptionalFeature(t *testing.T) {
	// Given
	host := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
		Features: []control.FeatureFlag{{Name: "checksum", Required: false}}}
	plugin := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}}

	// When
	tuple, err := control.Negotiate(host, plugin, nil)

	// Then
	require.NoError(t, err)
	require.False(t, tuple.Features["checksum"])
}

// Test the nonce round-trip: OfferToHello embeds a caller-supplied nonce and
// VerifyNonce rejects a HelloAck whose echoed nonce does not match, returning
// ErrProtocolViolation (guards against attaching to a stale/foreign
// process).
func TestVerifyNonce_ReturnsProtocolViolation_OnMismatch(t *testing.T) {
	// Given: a Hello carries a specific nonce
	host := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}}
	hello := control.OfferToHello(host, 0xDEADBEEF)
	require.EqualValues(t, 0xDEADBEEF, hello.GetNonce())

	// When / Then: a matching echo is accepted, a mismatched echo is a violation
	require.NoError(t, control.VerifyNonce(hello.GetNonce(), 0xDEADBEEF))
	require.ErrorIs(t, control.VerifyNonce(hello.GetNonce(), 0x1234), control.ErrProtocolViolation)
}

// Test VerifyBinaryIdentity accepting a matching pinned hash
func TestVerifyBinaryIdentity_Accepts_WhenPinnedHashMatches(t *testing.T) {
	// Given: a temp file with known content and its SHA-256
	path := filepath.Join(t.TempDir(), "plugin-bin")
	require.NoError(t, os.WriteFile(path, []byte("plugin-bytes"), 0o755))
	sum := sha256.Sum256([]byte("plugin-bytes"))

	// When
	err := control.VerifyBinaryIdentity(path, sum[:])

	// Then
	require.NoError(t, err)
}

// Test VerifyBinaryIdentity rejecting a mismatched pinned hash with IncompatibleError
func TestVerifyBinaryIdentity_ReturnsIncompatible_OnHashMismatch(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "plugin-bin")
	require.NoError(t, os.WriteFile(path, []byte("tampered-bytes"), 0o755))
	wrong := sha256.Sum256([]byte("plugin-bytes"))

	// When
	err := control.VerifyBinaryIdentity(path, wrong[:])

	// Then
	var incompatErr *control.IncompatibleError
	require.ErrorAs(t, err, &incompatErr)
	require.Contains(t, incompatErr.Reason, "binary identity")
}

// Test VerifyBinaryIdentity is a no-op when no hash is pinned (pinning is optional)
func TestVerifyBinaryIdentity_NoOp_WhenNoPin(t *testing.T) {
	err := control.VerifyBinaryIdentity("/nonexistent/never-read", nil)
	require.NoError(t, err)
}

// Test IncompatibleToHelloAck/HelloAckIncompatible round-tripping a
// negotiation-rejection reason, the echoed nonce, AND the plugin's own
// structured offer (protocol version, transport, codec, features,
// advertised service versions) — IncompatibleError carries both sides'
// offers, not just prose in a reason string. A plugin-side Negotiate
// failure never reaches the host as a Go error value (Negotiate runs in
// the plugin's own process), so this wire round-trip is the only way the
// host ever sees the plugin's offer for a rejected handshake.
func TestIncompatibleToHelloAck_RoundTripsReasonNonceAndPluginOffer_ThroughHelloAckIncompatible(t *testing.T) {
	// Given: the plugin's own (currently fixed) offer and advertised versions.
	offer := control.Offer{
		ProtocolMin: 1, ProtocolMax: 1,
		Transports: []string{"uds"},
		Codecs:     []string{"proto"},
		Features:   []control.FeatureFlag{{Name: "trace_context", Required: false}},
	}
	services := []control.ServiceVersion{{Service: "echo.Echo", Version: 1}}

	// When
	ack := control.IncompatibleToHelloAck(offer, services,
		"service echo.Echo: version 1 outside required range [2,2]", 0xDEADBEEF)
	reason, pluginOffer, rejected := control.HelloAckIncompatible(ack)

	// Then: the reason and nonce round-trip exactly as before...
	require.True(t, rejected)
	require.Contains(t, reason, "echo.Echo")
	require.EqualValues(t, 0xDEADBEEF, ack.GetNonce())

	// ...and the plugin's offer is now reconstructed structurally, not just
	// named in the reason string. The current plugin offer is single-valued
	// on every axis (see IncompatibleToHelloAck's doc), so the round-trip is
	// exact even though it collapses through HelloAck's singular
	// protocol_version/transport/codec fields.
	require.EqualValues(t, 1, pluginOffer.ProtocolMin)
	require.EqualValues(t, 1, pluginOffer.ProtocolMax)
	require.Equal(t, []string{"uds"}, pluginOffer.Transports)
	require.Equal(t, []string{"proto"}, pluginOffer.Codecs)
	require.Equal(t, []control.FeatureFlag{{Name: "trace_context", Required: false}}, pluginOffer.Features)
	require.Equal(t,
		[]control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 1, MaxVersion: 1}},
		pluginOffer.Services,
	)
}

// Test IncompatibleToHelloAck falling back to a generic placeholder when
// reason is empty, so a rejection ack can never carry an empty reason
// indistinguishable from a malformed/truncated message.
func TestIncompatibleToHelloAck_UsesPlaceholder_WhenReasonEmpty(t *testing.T) {
	// Given / When
	ack := control.IncompatibleToHelloAck(control.Offer{}, nil, "", 0xDEADBEEF)
	reason, _, rejected := control.HelloAckIncompatible(ack)

	// Then
	require.True(t, rejected)
	require.NotEmpty(t, reason)
}

// Test IncompatibleToHelloAck truncating an over-long reason at a bounded
// length before it goes on the wire (MSG-size discipline: control.Conn
// caps any single encoded message at MaxMessageSize), producing valid
// UTF-8 rather than splitting a multi-byte rune at the cut point.
func TestIncompatibleToHelloAck_TruncatesOverlongReason(t *testing.T) {
	// Given: a reason far longer than any real Negotiate failure message,
	// ending in a multi-byte rune positioned to straddle a naive byte-slice
	// truncation boundary.
	huge := strings.Repeat("x", control.MaxIncompatibleReasonBytes-1) + "é"

	// When
	ack := control.IncompatibleToHelloAck(control.Offer{}, nil, huge, 0xDEADBEEF)
	reason, _, rejected := control.HelloAckIncompatible(ack)

	// Then
	require.True(t, rejected)
	require.LessOrEqual(t, len(reason), control.MaxIncompatibleReasonBytes)
	require.True(t, utf8.ValidString(reason), "truncation must not split a multi-byte rune")
}

// Test HelloAckIncompatible reporting ok=false for an ordinary success ack
// (empty IncompatibleReason), so the host's happy path is unaffected.
func TestHelloAckIncompatible_ReturnsFalse_ForSuccessAck(t *testing.T) {
	// Given: a normal success ack built the existing way, never touching the
	// new field.
	tuple := control.Tuple{ProtocolVersion: 1, Transport: "uds", Codec: "proto", Features: map[string]bool{}}
	ack := control.TupleToHelloAck(tuple, 0xDEADBEEF, control.PluginIdentity{}, nil)

	// When
	reason, pluginOffer, rejected := control.HelloAckIncompatible(ack)

	// Then
	require.False(t, rejected)
	require.Empty(t, reason)
	require.Equal(t, control.Offer{}, pluginOffer)
}
