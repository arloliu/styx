package control_test

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
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
		[]control.ServiceVersion{{Service: "echo.Echo", Version: 1}}, control.Offer{})
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

// Test Negotiate resolving the burst feature flag under every offer
// combination: it reads true only when both sides offer it, and an old peer
// that never offers it resolves false rather than erroring (burst is never
// required by either side here, matching an old peer that has no idea the
// flag exists).
func TestNegotiate_ResolvesBurstFlag_AcrossOfferCombinations(t *testing.T) {
	base := func(offersBurst bool) control.Offer {
		o := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}}
		if offersBurst {
			o.Features = []control.FeatureFlag{{Name: control.FeatureBurst}}
		}

		return o
	}

	cases := []struct {
		name                     string
		hostOffers, pluginOffers bool
		wantResolved             bool
	}{
		{name: "both sides offer burst", hostOffers: true, pluginOffers: true, wantResolved: true},
		{name: "host offers burst, plugin does not", hostOffers: true, pluginOffers: false, wantResolved: false},
		{name: "plugin offers burst, host does not", hostOffers: false, pluginOffers: true, wantResolved: false},
		{name: "neither side offers burst", hostOffers: false, pluginOffers: false, wantResolved: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			tuple, err := control.Negotiate(base(tc.hostOffers), base(tc.pluginOffers), nil)

			// Then
			require.NoError(t, err)
			require.Equal(t, tc.wantResolved, tuple.Features[control.FeatureBurst])
		})
	}
}

// Test that burst activation is the full conjunction and not the flag alone.
// The flag resolves independently of transport selection, so a legal
// uds-with-burst tuple exists and must stay dormant: no burst socket, no fourth
// attach descriptor, today's wiring on both sides. A zero ceiling means the same
// for the generation, whatever the flag says. Both sides call this to agree on
// how many descriptors the attach carries, so a rule that fired on any narrower
// input would put one side's expectation out of step with the other's.
func TestBurstActive_RequiresFlagAndSharedMemoryAndCeiling(t *testing.T) {
	tuple := func(transport string, flag bool) control.Tuple {
		return control.Tuple{Transport: transport, Features: map[string]bool{control.FeatureBurst: flag}}
	}

	require.True(t, control.BurstActive(tuple(control.TransportSHM, true), 1<<20),
		"shared memory + the negotiated flag + a ceiling activates the burst path")
	require.False(t, control.BurstActive(tuple(control.TransportSHM, true), 0),
		"a zero ceiling leaves the burst path off for this generation")
	require.False(t, control.BurstActive(tuple(control.TransportSHM, false), 1<<20),
		"a ceiling without the negotiated flag activates nothing")
	require.False(t, control.BurstActive(tuple(control.TransportUDS, true), 1<<20),
		"the flag is dormant on a uds tuple: there is no inline path to route against")
	require.False(t, control.BurstActive(control.Tuple{Transport: control.TransportSHM}, 1<<20),
		"a tuple with no feature map resolves the flag false")
}

// Test that ChunkingActive resolves the same conjunction as BurstActive
// (shared memory ∧ the negotiated flag ∧ a non-zero ceiling), covering all
// eight combinations of the three inputs so a future change to any one
// operand cannot silently widen or narrow the conjunction.
func TestChunkingActive_RequiresFlagAndSharedMemoryAndCeiling(t *testing.T) {
	tuple := func(transport string, flag bool) control.Tuple {
		return control.Tuple{Transport: transport, Features: map[string]bool{control.FeatureStreamChunking: flag}}
	}

	cases := []struct {
		name     string
		trans    string
		flag     bool
		ceiling  uint32
		wantsSet bool
	}{
		{"shm+flag+ceiling", control.TransportSHM, true, 1 << 20, true},
		{"shm+flag+zero", control.TransportSHM, true, 0, false},
		{"shm+noflag+ceiling", control.TransportSHM, false, 1 << 20, false},
		{"shm+noflag+zero", control.TransportSHM, false, 0, false},
		{"uds+flag+ceiling", control.TransportUDS, true, 1 << 20, false},
		{"uds+flag+zero", control.TransportUDS, true, 0, false},
		{"uds+noflag+ceiling", control.TransportUDS, false, 1 << 20, false},
		{"uds+noflag+zero", control.TransportUDS, false, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			given := tuple(tc.trans, tc.flag)

			// When
			got := control.ChunkingActive(given, tc.ceiling)

			// Then
			require.Equal(t, tc.wantsSet, got)
		})
	}

	// Given a tuple with no feature map at all.
	// When / Then: the flag resolves false rather than panicking on the nil map.
	require.False(t, control.ChunkingActive(control.Tuple{Transport: control.TransportSHM}, 1<<20),
		"a tuple with no feature map resolves the flag false")
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

	// Then: a binary-identity IncompatibleError, distinguishable from an
	// ordinary handshake negotiation failure via Kind, not just Reason prose.
	var incompatErr *control.IncompatibleError
	require.ErrorAs(t, err, &incompatErr)
	require.Contains(t, incompatErr.Reason, "binary identity")
	require.Equal(t, control.IncompatibleBinaryIdentity, incompatErr.Kind)
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

// Test Negotiate's transport selection and shared-memory layout-version
// negotiation: auto (both transports offered) selects the shared-memory
// transport when both sides offer it and falls back to uds when the plugin
// does not; a shared-memory-pinned host fails against a uds-only plugin; and
// when shm is negotiated the layout version is the highest common one, failing
// closed on a disjoint set (shm-abi.md §19).
func TestNegotiate_SelectsTransportAndLayoutVersion(t *testing.T) {
	cases := []struct {
		name          string
		host, plugin  control.Offer
		wantErr       bool
		wantErrReason string
		wantTransport string
		wantLayout    uint32
	}{
		{
			name: "auto negotiates shm when both offer it, highest common layout",
			host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"shm", "uds"}, LayoutVersions: []uint32{1, 2}},
			plugin: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"shm", "uds"}, LayoutVersions: []uint32{1, 2, 3}},
			wantTransport: "shm",
			wantLayout:    2, // highest common of {1,2} and {1,2,3}
		},
		{
			name: "auto falls back to uds when plugin lacks shm",
			host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"shm", "uds"}, LayoutVersions: []uint32{1}},
			plugin: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"uds"}},
			wantTransport: "uds",
			wantLayout:    0, // uds carries no region layout
		},
		{
			name: "shm-pinned host fails against a uds-only plugin",
			host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"shm"}, LayoutVersions: []uint32{1}},
			plugin: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"uds"}},
			wantErr:       true,
			wantErrReason: "transport",
		},
		{
			name: "shm negotiated but disjoint layout sets fails closed",
			host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"shm"}, LayoutVersions: []uint32{1}},
			plugin: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"},
				Transports: []string{"shm"}, LayoutVersions: []uint32{2}},
			wantErr:       true,
			wantErrReason: "layout_version",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tuple, err := control.Negotiate(tc.host, tc.plugin, nil)
			if tc.wantErr {
				var incompatErr *control.IncompatibleError
				require.ErrorAs(t, err, &incompatErr)
				require.Contains(t, incompatErr.Reason, tc.wantErrReason)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantTransport, tuple.Transport)
			require.Equal(t, tc.wantLayout, tuple.LayoutVersion)
		})
	}
}

// faithfulShmOffers returns a host and plugin offer that both negotiate the
// shared-memory transport at layout version 1 with an optional checksum feature,
// for the recompute-and-compare tests below.
func faithfulShmOffers() (host, plugin control.Offer) {
	host = control.Offer{
		ProtocolMin: 1, ProtocolMax: 2, Codecs: []string{"proto"},
		Transports: []string{"shm", "uds"}, LayoutVersions: []uint32{1},
		Features: []control.FeatureFlag{{Name: "checksum"}},
	}
	plugin = control.Offer{
		ProtocolMin: 1, ProtocolMax: 2, Codecs: []string{"proto"},
		Transports: []string{"shm", "uds"}, LayoutVersions: []uint32{1},
		Features: []control.FeatureFlag{{Name: "checksum"}},
	}

	return host, plugin
}

// Test ValidateAcknowledgedTuple accepting a faithfully-built success ack: the
// host recomputes Negotiate from its own offer plus the plugin offer echoed on
// the ack and, finding every axis identical, returns the validated tuple.
func TestValidateAcknowledgedTuple_AcceptsFaithfulAck(t *testing.T) {
	// Given: a faithful ack the plugin would build from a real negotiation.
	host, plugin := faithfulShmOffers()
	tuple, err := control.Negotiate(host, plugin, nil)
	require.NoError(t, err)
	ack := control.TupleToHelloAck(tuple, 0xABCD, control.PluginIdentity{}, nil, plugin)

	// When
	got, verr := control.ValidateAcknowledgedTuple(host, ack)

	// Then: the validated tuple matches the negotiation exactly.
	require.NoError(t, verr)
	require.Equal(t, "shm", got.Transport)
	require.EqualValues(t, 1, got.LayoutVersion)
	require.Equal(t, tuple, got)
}

// Test ValidateAcknowledgedTuple rejecting a forged ack on each axis, before any
// region fd would be created: a tuple the host's own recomputation does not
// produce is refused with a typed *IncompatibleError (shm-abi.md §19).
func TestValidateAcknowledgedTuple_RejectsForgedAck(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(ack *controlpb.HelloAck)
	}{
		{
			name:   "transport the host never offered",
			tamper: func(ack *controlpb.HelloAck) { ack.Transport = "quic" },
		},
		{
			name:   "codec the host never offered",
			tamper: func(ack *controlpb.HelloAck) { ack.Codec = "cbor" },
		},
		{
			name:   "protocol version outside the negotiated range",
			tamper: func(ack *controlpb.HelloAck) { ack.ProtocolVersion = 99 },
		},
		{
			name:   "layout version outside the advertised set",
			tamper: func(ack *controlpb.HelloAck) { ack.LayoutVersion = 7 },
		},
		{
			name: "resolved feature flipped to a value neither side computed",
			tamper: func(ack *controlpb.HelloAck) {
				for _, f := range ack.GetFeatures() {
					f.Supported = !f.GetSupported()
				}
			},
		},
		{
			name: "plugin offer echoed on the ack contradicts the acked transport",
			tamper: func(ack *controlpb.HelloAck) {
				// Acked shm, but the echoed plugin offer no longer offers shm — the
				// host's recomputation now yields uds and the shm tuple is refused.
				ack.GetPluginOffer().Transports = []string{"uds"}
			},
		},
		{
			name: "duplicate service advertisement",
			tamper: func(ack *controlpb.HelloAck) {
				// The same service advertised twice: checkServices would collapse the
				// duplicate into a map (a later entry overwriting an earlier one), so
				// the recompute-and-compare cannot see it — it must be rejected up front.
				ack.Services = []*controlpb.ServiceVersion{
					{Service: "echo.Echo", Version: 1},
					{Service: "echo.Echo", Version: 2},
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: a faithful ack, then a single tampered field.
			host, plugin := faithfulShmOffers()
			tuple, err := control.Negotiate(host, plugin, nil)
			require.NoError(t, err)
			ack := control.TupleToHelloAck(tuple, 0xABCD, control.PluginIdentity{}, nil, plugin)
			tc.tamper(ack)

			// When
			got, verr := control.ValidateAcknowledgedTuple(host, ack)

			// Then: rejected with a typed error and no validated tuple.
			var incompatErr *control.IncompatibleError
			require.ErrorAs(t, verr, &incompatErr)
			require.Equal(t, control.Tuple{}, got)
		})
	}
}

// Test ValidateAcknowledgedTuple rejecting an ack whose echoed plugin offer
// cannot satisfy the host's per-service version requirement: the recomputation
// itself fails, so the ack is refused rather than attached.
func TestValidateAcknowledgedTuple_RejectsWhenRecomputeFailsServices(t *testing.T) {
	// Given: the host requires echo.Echo v2; the ack advertises only v1.
	host := control.Offer{
		ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"}, Transports: []string{"uds"},
		Services: []control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 2, MaxVersion: 2}},
	}
	plugin := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Codecs: []string{"proto"}, Transports: []string{"uds"}}
	services := []control.ServiceVersion{{Service: "echo.Echo", Version: 1}}
	// The plugin's own Negotiate would have failed; simulate a forged success ack
	// that claims a clean tuple while echoing the real (unsatisfying) offer.
	tuple := control.Tuple{ProtocolVersion: 1, Transport: "uds", Codec: "proto", Features: map[string]bool{}}
	ack := control.TupleToHelloAck(tuple, 0xABCD, control.PluginIdentity{}, services, plugin)

	// When
	_, verr := control.ValidateAcknowledgedTuple(host, ack)

	// Then
	var incompatErr *control.IncompatibleError
	require.ErrorAs(t, verr, &incompatErr)
	require.Contains(t, incompatErr.Reason, "echo.Echo")
}

// Test HelloAckIncompatible reporting ok=false for an ordinary success ack
// (empty IncompatibleReason), so the host's happy path is unaffected.
func TestHelloAckIncompatible_ReturnsFalse_ForSuccessAck(t *testing.T) {
	// Given: a normal success ack built the existing way, never touching the
	// new field.
	tuple := control.Tuple{ProtocolVersion: 1, Transport: "uds", Codec: "proto", Features: map[string]bool{}}
	ack := control.TupleToHelloAck(tuple, 0xDEADBEEF, control.PluginIdentity{}, nil, control.Offer{})

	// When
	reason, pluginOffer, rejected := control.HelloAckIncompatible(ack)

	// Then
	require.False(t, rejected)
	require.Empty(t, reason)
	require.Equal(t, control.Offer{}, pluginOffer)
}
