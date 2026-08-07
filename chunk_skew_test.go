package styx_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/stretchr/testify/require"
)

// featureChecksum is the handshake name of the CRC32C payload-checksum feature
// (shm-abi.md §5's allowed_flags mask). No production offer in this module
// lists it, which is what the last test in this file pins; there is no exported
// constant for it precisely because nothing offers it.
const featureChecksum = "checksum"

// featureStreamingName is the handshake name of the streaming feature. It has no
// exported constant either — internal/supervisor and pluginserver.go each hold
// their own unexported one — so a test outside both spells it out.
const featureStreamingName = "streaming"

// attachChunkFieldTag is AttachRegion field 7 (chunk_max_payload) with the
// varint wire type: (7 << 3) | 0. A test appends it by hand to put the field on
// the wire carrying an explicit zero, which marshaling a proto3 scalar never
// produces on its own.
const attachChunkFieldTag byte = 0x38

// chunkInstanceCount reports how many fixture processes one host has spawned.
// testdata/chunkplugin appends its pid on startup, so the line count is the
// generation count: a restart — which is what a poisoned region forces — shows
// up as a second line.
func chunkInstanceCount(t *testing.T, files chunkHost) int {
	t.Helper()

	return len(strings.Fields(exitReport(t, files.pids)))
}

// chunkObservedOffers runs one real cross-process handshake that is guaranteed to
// fail on the service axis, and returns the two offers that handshake actually
// carried. A failed negotiation is the one place the public API surfaces both
// sides' real offers, so this is how a test outside the packages that build them
// reads what a live host and a live plugin process offered, instead of restating
// it.
//
// ceiling is stamped on the spec, so the host's offer is the one a host with —
// or without — a configured chunk ceiling really makes.
func chunkObservedOffers(t *testing.T, ceiling uint32) (host, plugin styx.HandshakeOffer) {
	t.Helper()

	spec := styx.PluginSpec{
		Name:      "chunk-skew-offers",
		Path:      fixtureVersionedPlugin,
		Args:      []string{"1"},
		Transport: styx.TransportAuto,
		Services: []styx.ServiceRequirement{
			{Service: "versiontest.Versioned", MinVersion: 2, MaxVersion: 2},
		},
	}
	styx.SetChunkMaxPayloadForTest(&spec, ceiling)

	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	var incompatible *styx.IncompatibleError
	require.ErrorAs(t, h.Start(t.Context()), &incompatible,
		"the probe must fail on the service axis: that failure is what makes both real offers observable")

	return incompatible.HostOffer, incompatible.PluginOffer
}

// chunkControlOffer rebuilds a negotiable control.Offer from the public
// projection a failed handshake reports. Two axes do not survive that projection
// and are restored here: the per-flag required bit and the shared-memory
// layout-version set, a single value this build fixes. Services is deliberately
// dropped: the probe handshake failed on that axis, and the rows built from these
// offers are about the feature axis.
//
// The required bit is set to false by construction rather than recovered, so
// these rows ASSUME rather than prove that the chunking flag is offered
// optional. That is a deliberate split, not an oversight: the bit is pinned at
// its source, by TestHostOffer_OffersChunking_FromConfig in
// internal/supervisor, which reads it straight off the real host offer, and by
// pluginserver_test.go's own offer assertions on the plugin side. What these
// rows add is what NEGOTIATION does with the flag's presence or absence, which
// the bit does not affect for an optional flag either side may omit. A required
// offer takes the ordinary fail-closed path (stream-protocol.md §13.1) and is
// not a skew case.
func chunkControlOffer(o styx.HandshakeOffer) control.Offer {
	transports := make([]string, len(o.Transports))
	for i, tr := range o.Transports {
		transports[i] = string(tr)
	}

	features := make([]control.FeatureFlag, len(o.Features))
	for i, name := range o.Features {
		features[i] = control.FeatureFlag{Name: name}
	}

	return control.Offer{
		ProtocolMin:    o.ProtocolMin,
		ProtocolMax:    o.ProtocolMax,
		Transports:     transports,
		Codecs:         o.Codecs,
		Features:       features,
		LayoutVersions: []uint32{control.ShmLayoutVersion},
	}
}

// chunkFeatureNames lists the flag names an offer carries, which is the axis
// every skew row asserts on.
func chunkFeatureNames(o control.Offer) []string {
	names := make([]string, len(o.Features))
	for i, f := range o.Features {
		names[i] = f.Name
	}

	return names
}

// chunkFrozenPluginOffer is peer as a build that predates stream-chunking would
// have offered it: identical in every other negotiated axis, minus the one flag.
// Deriving it from a real offer rather than writing one out keeps the row about
// the feature and not about some other axis drifting.
func chunkFrozenPluginOffer(peer control.Offer) control.Offer {
	frozen := peer
	frozen.Features = nil
	for _, f := range peer.Features {
		if f.Name != control.FeatureStreamChunking {
			frozen.Features = append(frozen.Features, f)
		}
	}

	return frozen
}

// Test that a host which announces no chunk ceiling behaves exactly as it did
// before stream-chunking existed, against a real plugin process that does offer
// the flag: the feature never activates, an oversize send is refused before the
// wire, every size at or below the inline limit is untouched, and frame kind 9
// stays unassigned on the connection (stream-protocol.md §13.1, shm-abi.md §5).
//
// A host with no ceiling omits the flag from its offer entirely, so two of
// §13.1's three conjuncts fail at once: the flag resolves false and the
// announced ceiling is zero. One host serves every row, so a per-row teardown
// cannot hide a connection that degraded partway through.
func TestHostStream_BehaveAsBeforeChunking_WhenTheHostAnnouncesNoCeiling(t *testing.T) {
	// Given a dormant shared-memory connection to a real fixture process.
	files := startChunkHostSpec(t, 0, chunkGeometry(), styx.TransportSHM, nil)
	oversize := int(chunkHostToPluginTop) + 1

	t.Run("an oversize message is refused before the wire", func(t *testing.T) {
		// Given / When one byte past the direction's inline limit is sent.
		got, err := echoRoundTrip(t, files.conn, oversize)

		// Then the send fails with the definitive error the pre-feature build gave.
		require.ErrorIs(t, err, styx.ErrPayloadTooLarge)
		require.Nil(t, got)

		// And nothing of that size ever reached the peer: the fixture reports every
		// message it receives with its byte count, so a delivery would be visible.
		require.NotContains(t, chunkProgress(t, files),
			fmt.Sprintf("%s %d bytes", chunkEchoReceived, oversize),
			"a refused oversize send delivers no message to the peer")
	})

	t.Run("sizes at and below the inline limit are unchanged", func(t *testing.T) {
		for _, size := range []int{1, 255, 256, int(chunkHostToPluginTop) - 1, int(chunkHostToPluginTop)} {
			t.Run(strconv.Itoa(size), func(t *testing.T) {
				// Given / When
				got, err := echoRoundTrip(t, files.conn, size)

				// Then it round-trips byte for byte, as it did before the feature.
				require.NoError(t, err)
				require.Len(t, got, size)
				require.True(t, bytes.Equal(chunkPattern(size), got))
			})
		}
	})

	t.Run("the region was never poisoned and the instance never replaced", func(t *testing.T) {
		// Given the refused oversize send above — the only thing on this connection
		// that could have tempted the sender into emitting a fragment train.
		//
		// A kind-9 frame reaching a receiver whose connection did not activate
		// chunking is an unassigned frame kind: the receiver poisons the region
		// (shm-abi.md §5, restated as stream-protocol.md §13.7's last row), which is
		// connection-fatal and forces the host to replace the instance. So a
		// connection that still serves, on a first and only fixture process, is the
		// observable consequence of no kind 9 having been written.

		// When the connection is used again.
		got, err := echoRoundTrip(t, files.conn, 256)

		// Then it still serves, exactly one fixture process ever ran, and nothing
		// arrived at the peer malformed.
		require.NoError(t, err)
		require.True(t, bytes.Equal(chunkPattern(256), got))
		require.Equal(t, 1, chunkInstanceCount(t, files),
			"a poisoned region fails the connection and forces a restart, which never happened")
		require.NotContains(t, chunkProgress(t, files), chunkEchoCorrupt)
	})
}

// Test the stream-chunking flag reaching a real host's handshake offer from the
// ceiling its PluginSpec announces and from nothing else, and a real plugin
// process offering the flag whatever the host does. Both facts are read off a
// live handshake, and every skew row in this file depends on them
// (stream-protocol.md §13.1).
func TestHandshakeOffer_OfferChunking_OnlyWhenTheSpecAnnouncesACeiling(t *testing.T) {
	// Given two real handshakes: one from a host that announced a ceiling, one
	// from a host that announced none.
	withCeiling, pluginAgainstCeiling := chunkObservedOffers(t, chunkCeiling)
	withoutCeiling, pluginAgainstNoCeiling := chunkObservedOffers(t, 0)

	// When / Then the host offers the flag exactly when it announced a ceiling.
	require.Contains(t, withCeiling.Features, control.FeatureStreamChunking)
	require.NotContains(t, withoutCeiling.Features, control.FeatureStreamChunking,
		"a host with no ceiling must not ask a plugin to support a feature it could not activate")

	// And the plugin offers it either way: the ceiling is the host's alone, so a
	// plugin has nothing to configure and nothing to withhold.
	require.Contains(t, pluginAgainstCeiling.Features, control.FeatureStreamChunking)
	require.Contains(t, pluginAgainstNoCeiling.Features, control.FeatureStreamChunking)
}

// Test that a plugin which predates stream-chunking leaves the feature dormant
// on a host that has a real, non-zero ceiling configured: the flag resolves true
// only when both sides offer it, so a ceiling alone activates nothing
// (stream-protocol.md §13.1).
//
// This orientation is proven at the negotiation seam rather than across a
// process boundary because nothing mutes a plugin's offered features — every
// plugin this module builds offers stream-chunking unconditionally. Negotiation
// is where the row is actually decided, and both offers going into it are the
// ones a live handshake carried.
func TestNegotiate_LeaveChunkingDormant_WhenThePluginPredatesTheFeature(t *testing.T) {
	// Given the real offers of a host with a ceiling and of a current plugin, plus
	// that same plugin offer as the previous release shipped it.
	observedHost, observedPlugin := chunkObservedOffers(t, chunkCeiling)
	hostOffer := chunkControlOffer(observedHost)
	current := chunkControlOffer(observedPlugin)
	frozen := chunkFrozenPluginOffer(current)

	require.Contains(t, chunkFeatureNames(hostOffer), control.FeatureStreamChunking,
		"this host offers the feature: the row below is about the OTHER peer")
	require.ElementsMatch(t, []string{featureStreamingName, control.FeatureBurst}, chunkFeatureNames(frozen),
		"the frozen offer is the shape the previous release shipped: streaming and burst, no stream-chunking")

	// When the frozen plugin offer is negotiated against the host's.
	tuple, err := control.Negotiate(hostOffer, frozen, nil)

	// Then the handshake succeeds — the flag is optional on both sides — the
	// transport still resolves, and the feature does not.
	require.NoError(t, err, "an absent optional feature must not fail the handshake")
	require.Equal(t, control.TransportSHM, tuple.Transport,
		"an absent optional feature must not change which transport is selected")
	require.False(t, tuple.Features[control.FeatureStreamChunking],
		"a feature only one side offers resolves false")
	require.False(t, control.ChunkingActive(tuple, chunkCeiling),
		"a configured ceiling activates nothing on its own")

	// And the same host offer against the current plugin offer resolves the flag
	// true, so the assertion above is about the missing flag and not vacuous.
	live, err := control.Negotiate(hostOffer, current, nil)
	require.NoError(t, err)
	require.True(t, live.Features[control.FeatureStreamChunking])
	require.True(t, control.ChunkingActive(live, chunkCeiling))
}

// Test the three shapes AttachRegion field 7 can take on the wire — absent,
// present carrying zero, present carrying a real ceiling — activating chunking
// only in the last case, on a tuple that already resolved the flag over shared
// memory (stream-protocol.md §13.1's third conjunct, §13.6).
//
// The absent case is built by marshaling a message without the field, so its
// absence is a property of the bytes rather than of a Go field left at zero: an
// attach from a peer that predates the feature carries no field 7 at all, and
// that case must resolve the same way as an explicit zero, not differently.
func TestChunkingActive_RequireANonZeroCeiling_AcrossAttachFieldPresence(t *testing.T) {
	// Given a tuple that resolved the flag over shared memory, so the announced
	// ceiling is the only conjunct still in play.
	tuple := control.Tuple{
		Transport: control.TransportSHM,
		Features:  map[string]bool{control.FeatureStreamChunking: true},
	}

	absent, err := (&controlpb.AttachRegion{Generation: 7}).MarshalVT()
	require.NoError(t, err)
	require.False(t, bytes.Contains(absent, []byte{attachChunkFieldTag}),
		"the field must be genuinely off the wire, not merely zero on the Go struct")

	// The present-zero encoding is that same message plus an explicit field 7 of
	// zero, which is the only way to put a present zero on the wire for a proto3
	// scalar.
	presentZero := append(slices.Clone(absent), attachChunkFieldTag, 0x00)

	presentCeiling, err := (&controlpb.AttachRegion{Generation: 7, ChunkMaxPayload: chunkCeiling}).MarshalVT()
	require.NoError(t, err)

	cases := []struct {
		name    string
		wire    []byte
		ceiling uint32
		active  bool
	}{
		{name: "absent", wire: absent},
		{name: "present-zero", wire: presentZero},
		{name: "present-nonzero", wire: presentCeiling, ceiling: chunkCeiling, active: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given one wire form of the attach message.
			var got controlpb.AttachRegion

			// When it is decoded and the activation conjunction evaluated on it.
			require.NoError(t, got.UnmarshalVT(tc.wire))

			// Then the ceiling and the activation both follow the bytes.
			require.Equal(t, tc.ceiling, got.GetChunkMaxPayload())
			require.Equal(t, tc.active, control.ChunkingActive(tuple, got.GetChunkMaxPayload()))
		})
	}
}

// Test the transport conjunct of the activation rule under the default transport
// preference: with both peers offering the flag, what auto negotiates is what
// decides whether chunking runs (stream-protocol.md §13.1). Each row spawns its
// own fixture, because the transport is fixed for a connection's life.
func TestHostStream_ResolveChunkingFromTheNegotiatedTransport_OverAuto(t *testing.T) {
	t.Run("shared memory with a ceiling activates chunking", func(t *testing.T) {
		// Given the ordinary fixture, which advertises both transports, and a host
		// that announces a ceiling.
		files := startChunkHostSpec(t, chunkCeiling, chunkGeometry(), styx.TransportAuto, nil)
		oversize := 2*int(chunkHostToPluginTop) + 1

		// When a message well past the inline limit and one past the ceiling are echoed.
		got, err := echoRoundTrip(t, files.conn, oversize)
		_, pastCeiling := echoRoundTrip(t, files.conn, int(chunkCeiling)+1)

		// Then the first round-trips and the second is refused. The pair is what
		// makes this decisive about the negotiated transport: uds would have carried
		// both, since its frame cap is a megabyte and no chunk ceiling governs it,
		// and a dormant shared-memory attach would have refused both.
		require.NoError(t, err)
		require.Len(t, got, oversize)
		require.True(t, bytes.Equal(chunkPattern(oversize), got))
		require.ErrorIs(t, pastCeiling, styx.ErrPayloadTooLarge)
	})

	t.Run("a uds-only plugin leaves chunking inactive", func(t *testing.T) {
		// Given a fixture that advertises only uds, against a host that offers both
		// and announces a ceiling — so both sides offer the flag, a real ceiling is
		// announced, and only the transport conjunct is missing.
		files := startChunkHostSpec(t, chunkCeiling, chunkGeometry(), styx.TransportAuto,
			func(spec *styx.PluginSpec, _ chunkHost) {
				spec.Env = append(spec.Env, "STYX_CHUNK_UDS_ONLY=1")
			})
		size := int(chunkCeiling) + 1

		// When a message one byte past the announced ceiling is echoed.
		got, err := echoRoundTrip(t, files.conn, size)

		// Then it round-trips. This size is what distinguishes the two data planes:
		// it is past the chunk ceiling, which an active-chunking connection refuses,
		// and far past the ladder's top slab, which a dormant shared-memory attach
		// refuses — but well inside the uds frame cap of a megabyte, which is
		// unrelated to the slab ladder and to the chunk ceiling both.
		require.NoError(t, err)
		require.Len(t, got, size)
		require.True(t, bytes.Equal(chunkPattern(size), got))
	})

	t.Run("no ceiling leaves shared memory dormant", func(t *testing.T) {
		// Given the ordinary fixture and a host that announces no ceiling.
		files := startChunkHostSpec(t, 0, chunkGeometry(), styx.TransportAuto, nil)

		// When a message one byte past the ladder's top slab is echoed.
		_, err := echoRoundTrip(t, files.conn, int(chunkHostToPluginTop)+1)

		// Then it is refused, and the refusal itself proves shared memory was
		// negotiated: uds carries this size without complaint, so a silent downgrade
		// would have succeeded here instead.
		require.ErrorIs(t, err, styx.ErrPayloadTooLarge)
	})
}

// Test that neither side of a real handshake offers a checksum feature.
//
// The cross-process matrix depends on it silently, so it is checked here rather
// than assumed: with no CRC32C trailer negotiated, nothing is appended to a
// payload slab (shm-abi.md §5), so a direction's inline limit is its top slab
// size exactly — which is why every boundary size in this suite is written
// against chunkHostToPluginTop and chunkPluginToHostTop directly instead of
// against a limit computed from them. Chunking with the checksum feature ON is
// covered where it can be reached at all — over an in-process shared-memory pair
// built with the feature forced on, by stream_shm_test.go's
// TestChunkPolicy_CarryEachEndsOwnInlineLimits_OnAnAsymmetricShmPair (the four
// exact limits, four bytes lower per direction) and
// TestStreamChunk_ReassembleTheExactBytes_OverARealShmPair (a train surviving the
// trailer path).
func TestHandshakeOffers_OmitChecksum_OnBothSidesOfARealHandshake(t *testing.T) {
	// Given a real host and a real plugin process exchanging offers.
	host, plugin := chunkObservedOffers(t, chunkCeiling)

	// When / Then neither side lists it, so it can never resolve true.
	require.NotContains(t, host.Features, featureChecksum,
		"a host that offered checksum would put a CRC trailer in every slab and lower every inline limit")
	require.NotContains(t, plugin.Features, featureChecksum)
}
