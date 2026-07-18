package control

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"slices"
	"unicode/utf8"

	"github.com/arloliu/styx/internal/control/controlpb"
)

// Offer is one side's declared capabilities for handshake negotiation.
// Services is populated only on the host's offer (its required version
// range per service, from generated-code metadata); the plugin's offer
// leaves it nil and instead supplies pluginServices to Negotiate.
type Offer struct {
	ProtocolMin, ProtocolMax uint32
	Features                 []FeatureFlag
	Transports               []string
	Codecs                   []string
	Services                 []ServiceRequirement // host-only
}

// FeatureFlag is a named, independently versioned capability: most protocol
// evolution happens here instead of a protocol-version bump.
// Required is per-side: each side marks which flags IT requires; Negotiate
// fails closed if either side requires a flag the other side doesn't
// support. A side "supports" a flag by listing it in its own Offer.Features
// — the flag's presence in the offer is the support signal (there is no
// separate Supported field in the pure Offer model; the controlpb wire type
// carries a Supported echo bit that HelloToOffer collapses into presence).
type FeatureFlag struct {
	Name     string
	Required bool
}

// ServiceRequirement is the host's declared acceptable version range for one
// service it intends to call, sourced from generated-code metadata (the code
// generator embeds the generator/runtime ABI version and each service's
// version in the generated Register<Service>Server call).
type ServiceRequirement struct {
	Service                string
	MinVersion, MaxVersion uint32
}

// ServiceVersion is one service's actual version, as advertised by the
// plugin (the version it was compiled against, from the same generated
// metadata on the plugin side).
type ServiceVersion struct {
	Service string
	Version uint32
}

// Tuple is the fully negotiated compatibility tuple: protocol version,
// transport, layout version (0 unless transport == "shm" — not yet
// populated, since only "uds" is currently negotiated), the resolved
// feature set (name -> whether both sides will use it), and codec. Both
// sides acknowledge the identical Tuple before any region is attached, so
// an untested combination of individually-valid versions can never run.
type Tuple struct {
	ProtocolVersion uint32
	Transport       string
	LayoutVersion   uint32
	Features        map[string]bool
	Codec           string
}

// IncompatibleError is internal/control's negotiation-failure type. It is
// NOT styx.IncompatibleError (the public type): internal/control must not
// import the styx package (a layering violation and an import cycle). The
// lifecycle code at the public-API boundary catches *IncompatibleError via
// errors.As and constructs a *styx.IncompatibleError with the equivalent
// styx.HandshakeOffer values.
type IncompatibleError struct {
	HostOffer   Offer
	PluginOffer Offer
	Reason      string
}

func (e *IncompatibleError) Error() string {
	return "control: incompatible handshake: " + e.Reason
}

// Negotiate computes the compatibility tuple from the host's Offer, the
// plugin's Offer, and the plugin's advertised service versions.
//
// On success, Tuple.ProtocolVersion is the highest common protocol version
// (the top of the range intersection — the two sides speak the highest
// common version); Tuple.Features contains every flag name either side
// offered, mapped to true only if BOTH sides support it (an optional flag
// that only one side supports is simply false, not an error);
// Tuple.Transport and Tuple.Codec are each the lexicographically first
// common entry (deterministic tie-break, so a future multi-option list has
// defined behavior); Tuple.LayoutVersion is 0 for "uds" (the only
// transport currently supported).
//
// Every failure returns a *IncompatibleError carrying both offers and a
// distinct Reason naming the offending axis.
func Negotiate(host, plugin Offer, pluginServices []ServiceVersion) (Tuple, error) {
	fail := func(reason string) (Tuple, error) {
		return Tuple{}, &IncompatibleError{HostOffer: host, PluginOffer: plugin, Reason: reason}
	}

	// Axis 1: protocol version — range intersection, highest common wins.
	lo := max(host.ProtocolMin, plugin.ProtocolMin)
	hi := min(host.ProtocolMax, plugin.ProtocolMax)
	if lo > hi {
		return fail("protocol range: empty intersection")
	}
	protocolVersion := hi

	// Transport — lexicographically-first common entry.
	transport, ok := firstCommon(host.Transports, plugin.Transports)
	if !ok {
		return fail("transport: no common transport")
	}

	// Codec — lexicographically-first common entry.
	codec, ok := firstCommon(host.Codecs, plugin.Codecs)
	if !ok {
		return fail("codec: no common codec")
	}

	// Feature flags — fail closed on a REQUIRED flag the other side does not
	// support; resolve every offered flag to whether BOTH sides support it.
	features, err := negotiateFeatures(host, plugin)
	if err != nil {
		return fail(err.Error())
	}

	// Per-service version requirements (host declares, plugin satisfies).
	if reason, ok := checkServices(host.Services, pluginServices); !ok {
		return fail(reason)
	}

	return Tuple{
		ProtocolVersion: protocolVersion,
		Transport:       transport,
		LayoutVersion:   0, // only "shm" populates a non-zero layout version (not yet implemented)
		Features:        features,
		Codec:           codec,
	}, nil
}

// negotiateFeatures resolves the feature axis. A side supports a flag if the
// flag name appears in that side's Offer.Features. A flag either side marks
// Required must be supported by the other side, else the handshake fails
// closed. The returned map covers every flag name either side offered,
// true only when both sides support it.
func negotiateFeatures(host, plugin Offer) (map[string]bool, error) {
	hostSupports := featureNames(host.Features)
	pluginSupports := featureNames(plugin.Features)

	for _, f := range host.Features {
		if f.Required && !pluginSupports[f.Name] {
			return nil, fmt.Errorf("feature %s: required by host, not supported by plugin", f.Name)
		}
	}
	for _, f := range plugin.Features {
		if f.Required && !hostSupports[f.Name] {
			return nil, fmt.Errorf("feature %s: required by plugin, not supported by host", f.Name)
		}
	}

	resolved := make(map[string]bool, len(hostSupports)+len(pluginSupports))
	for name := range hostSupports {
		resolved[name] = pluginSupports[name]
	}
	for name := range pluginSupports {
		if _, seen := resolved[name]; !seen {
			resolved[name] = false // host does not support it → false, not an error
		}
	}

	return resolved, nil
}

// checkServices verifies every host ServiceRequirement is satisfied by the
// plugin's advertised versions. A required service that is absent from the
// plugin, or present outside [Min,Max], fails the handshake with the service
// named in the reason. The two failure modes get distinct reasons: an absent
// service says "not advertised" (a zero version would otherwise masquerade as
// a real "version 0" the plugin never claimed), a present-but-out-of-range
// one names the actual version.
func checkServices(required []ServiceRequirement, advertised []ServiceVersion) (string, bool) {
	have := make(map[string]uint32, len(advertised))
	for _, sv := range advertised {
		have[sv.Service] = sv.Version
	}
	for _, req := range required {
		v, ok := have[req.Service]
		if !ok {
			return fmt.Sprintf("service %s: not advertised by plugin (required range [%d,%d])",
				req.Service, req.MinVersion, req.MaxVersion), false
		}
		if v < req.MinVersion || v > req.MaxVersion {
			return fmt.Sprintf("service %s: version %d outside required range [%d,%d]",
				req.Service, v, req.MinVersion, req.MaxVersion), false
		}
	}

	return "", true
}

// firstCommon returns the lexicographically-first entry present in both
// lists, and false if the intersection is empty.
func firstCommon(a, b []string) (string, bool) {
	set := make(map[string]struct{}, len(b))
	for _, s := range b {
		set[s] = struct{}{}
	}
	common := make([]string, 0, len(a))
	for _, s := range a {
		if _, ok := set[s]; ok {
			common = append(common, s)
		}
	}
	if len(common) == 0 {
		return "", false
	}
	slices.Sort(common)

	return common[0], true
}

// featureNames indexes a feature list by name (its support set).
func featureNames(flags []FeatureFlag) map[string]bool {
	names := make(map[string]bool, len(flags))
	for _, f := range flags {
		names[f.Name] = true
	}

	return names
}

// VerifyNonce checks that a HelloAck echoed the exact nonce the host's Hello
// sent (the per-launch nonce guards against attaching to a stale or foreign
// process). A mismatch is a control-protocol violation.
func VerifyNonce(sent, got uint64) error {
	if sent != got {
		return fmt.Errorf("control: nonce mismatch (sent %#x, echoed %#x): %w", sent, got, ErrProtocolViolation)
	}

	return nil
}

// VerifyBinaryIdentity checks the plugin binary at path against a pinned
// SHA-256 digest. Pinning is optional: a nil pin is a no-op (path is not
// even opened). Otherwise the file is streamed through
// crypto/sha256 (never slurped) and compared; a mismatch returns a
// *IncompatibleError naming the expected and actual hex digests. The host
// runs this against the binary IT spawned — identity is never trusted from
// the child's self-reported PluginIdentity.
func VerifyBinaryIdentity(path string, pinned []byte) error {
	if pinned == nil {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("control: open plugin binary for identity check: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("control: hash plugin binary for identity check: %w", err)
	}
	actual := h.Sum(nil)

	if !bytes.Equal(pinned, actual) {
		return &IncompatibleError{
			Reason: fmt.Sprintf("binary identity: expected %s, got %s",
				hex.EncodeToString(pinned), hex.EncodeToString(actual)),
		}
	}

	return nil
}

// OfferToHello builds a controlpb.Hello from an Offer and a per-launch nonce.
// Services is host-only; a plugin's Offer leaves it nil, yielding an empty
// repeated field.
func OfferToHello(o Offer, nonce uint64) *controlpb.Hello {
	h := &controlpb.Hello{
		ProtocolMin: o.ProtocolMin,
		ProtocolMax: o.ProtocolMax,
		Transports:  o.Transports,
		Codecs:      o.Codecs,
		Nonce:       nonce,
	}
	for _, f := range o.Features {
		h.Features = append(h.Features, &controlpb.FeatureFlag{Name: f.Name, Required: f.Required})
	}
	for _, s := range o.Services {
		h.Services = append(h.Services, &controlpb.ServiceRequirement{
			Service:    s.Service,
			MinVersion: s.MinVersion,
			MaxVersion: s.MaxVersion,
		})
	}

	return h
}

// HelloToOffer projects a controlpb.Hello back into an Offer. The wire
// FeatureFlag's Supported echo bit is not part of the Offer model — support
// is expressed by a flag's presence in the list — so it is dropped here.
func HelloToOffer(h *controlpb.Hello) Offer {
	o := Offer{
		ProtocolMin: h.GetProtocolMin(),
		ProtocolMax: h.GetProtocolMax(),
		Transports:  h.GetTransports(),
		Codecs:      h.GetCodecs(),
	}
	for _, f := range h.GetFeatures() {
		o.Features = append(o.Features, FeatureFlag{Name: f.GetName(), Required: f.GetRequired()})
	}
	for _, s := range h.GetServices() {
		o.Services = append(o.Services, ServiceRequirement{
			Service:    s.GetService(),
			MinVersion: s.GetMinVersion(),
			MaxVersion: s.GetMaxVersion(),
		})
	}

	return o
}

// TupleToHelloAck builds a controlpb.HelloAck from a negotiated Tuple, the
// echoed nonce, and the plugin's self-reported identity/service versions. The
// resolved feature set is emitted with the Supported bit set to the tuple's
// resolved value so the peer sees exactly which flags are active.
func TupleToHelloAck(t Tuple, nonce uint64, identity PluginIdentity, services []ServiceVersion) *controlpb.HelloAck {
	ack := &controlpb.HelloAck{
		ProtocolVersion: t.ProtocolVersion,
		Transport:       t.Transport,
		LayoutVersion:   t.LayoutVersion,
		Codec:           t.Codec,
		Nonce:           nonce,
		PluginName:      identity.Name,
		PluginSemver:    identity.SemVer,
		BinarySha256:    identity.BinarySHA256,
	}
	names := make([]string, 0, len(t.Features))
	for name := range t.Features {
		names = append(names, name)
	}
	slices.Sort(names) // deterministic wire order, independent of map iteration
	for _, name := range names {
		ack.Features = append(ack.Features, &controlpb.FeatureFlag{Name: name, Supported: t.Features[name]})
	}
	for _, s := range services {
		ack.Services = append(ack.Services, &controlpb.ServiceVersion{Service: s.Service, Version: s.Version})
	}

	return ack
}

// HelloAckToTuple projects a controlpb.HelloAck back into the acknowledged
// Tuple. Both sides must arrive at the identical Tuple; this is the
// receiving side's reconstruction of what the sender acknowledged.
func HelloAckToTuple(ack *controlpb.HelloAck) Tuple {
	t := Tuple{
		ProtocolVersion: ack.GetProtocolVersion(),
		Transport:       ack.GetTransport(),
		LayoutVersion:   ack.GetLayoutVersion(),
		Codec:           ack.GetCodec(),
		Features:        make(map[string]bool, len(ack.GetFeatures())),
	}
	for _, f := range ack.GetFeatures() {
		t.Features[f.GetName()] = f.GetSupported()
	}

	return t
}

// MaxIncompatibleReasonBytes bounds IncompatibleToHelloAck's reason field
// (MSG-size discipline: control.Conn caps any single encoded
// ControlMessage at MaxMessageSize, and a reason string is the one
// caller-influenced, unbounded-in-principle payload a rejection ack
// carries). Comfortably larger than any real Negotiate failure message
// (Negotiate's Reason strings are short, fixed-shape sentences) while
// leaving ample room under MaxMessageSize for the ack's other fields.
const MaxIncompatibleReasonBytes = 1024

// genericIncompatibleReason is IncompatibleToHelloAck's fallback when
// called with an empty reason, so a rejection ack can never carry an empty
// reason indistinguishable from a malformed or truncated message.
const genericIncompatibleReason = "control: incompatible handshake (no reason reported)"

// IncompatibleToHelloAck builds a rejection HelloAck for a plugin-side
// Negotiate failure. nonce is echoed (so VerifyNonce still passes on the
// host). offer is the plugin's own (currently fixed) Offer and services its
// own advertised ServiceVersions — both carried through the SAME fields
// TupleToHelloAck already populates on a success ack (protocol_version,
// transport, codec, features, services), so the host reconstructs a
// structured PluginOffer (IncompatibleError carries both sides' offers,
// not prose in Reason alone) via HelloAckIncompatible rather than adding
// new wire fields.
//
// The current plugin offer (pluginserver.go's m1PluginOffer) is
// single-valued on every axis — exactly one protocol version (ProtocolMin
// == ProtocolMax), one transport, one codec — so collapsing offer's
// ranges/lists into HelloAck's singular protocol_version/transport/codec
// fields (the same fields a SUCCESS ack uses for the negotiated, also
// single-valued, Tuple) is lossless today. A genuinely multi-valued plugin
// offer in the future would need dedicated range/list fields here instead
// of reusing the success-ack shape.
//
// reason is bounded to MaxIncompatibleReasonBytes (truncated on a valid
// UTF-8 boundary) and, if empty, replaced with genericIncompatibleReason.
func IncompatibleToHelloAck(offer Offer, services []ServiceVersion, reason string, nonce uint64) *controlpb.HelloAck {
	if reason == "" {
		reason = genericIncompatibleReason
	}
	reason = truncateUTF8(reason, MaxIncompatibleReasonBytes)

	ack := &controlpb.HelloAck{
		ProtocolVersion:    offer.ProtocolMax,
		Nonce:              nonce,
		IncompatibleReason: reason,
	}
	if len(offer.Transports) > 0 {
		ack.Transport = offer.Transports[0]
	}
	if len(offer.Codecs) > 0 {
		ack.Codec = offer.Codecs[0]
	}
	for _, f := range offer.Features {
		ack.Features = append(ack.Features, &controlpb.FeatureFlag{Name: f.Name, Required: f.Required})
	}
	for _, s := range services {
		ack.Services = append(ack.Services, &controlpb.ServiceVersion{Service: s.Service, Version: s.Version})
	}

	return ack
}

// truncateUTF8 shortens s to at most maxBytes bytes, trimming further if
// needed so the result never splits a multi-byte rune at the cut point
// (producing invalid UTF-8).
func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}

	s = s[:maxBytes]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}

	return s
}

// HelloAckIncompatible reports whether ack is a rejection reply built by
// IncompatibleToHelloAck (IncompatibleReason non-empty), returning that
// reason plus the plugin's own Offer reconstructed from the ack's fields
// (reversing the collapse IncompatibleToHelloAck's doc describes). An
// ordinary success ack (TupleToHelloAck's output) always leaves
// IncompatibleReason empty, so rejected is false for it and pluginOffer is
// the zero Offer.
func HelloAckIncompatible(ack *controlpb.HelloAck) (reason string, pluginOffer Offer, rejected bool) {
	reason = ack.GetIncompatibleReason()
	if reason == "" {
		return "", Offer{}, false
	}

	pluginOffer = Offer{ProtocolMin: ack.GetProtocolVersion(), ProtocolMax: ack.GetProtocolVersion()}
	if t := ack.GetTransport(); t != "" {
		pluginOffer.Transports = []string{t}
	}
	if c := ack.GetCodec(); c != "" {
		pluginOffer.Codecs = []string{c}
	}
	for _, f := range ack.GetFeatures() {
		pluginOffer.Features = append(pluginOffer.Features, FeatureFlag{Name: f.GetName(), Required: f.GetRequired()})
	}
	for _, s := range ack.GetServices() {
		pluginOffer.Services = append(pluginOffer.Services, ServiceRequirement{
			Service: s.GetService(), MinVersion: s.GetVersion(), MaxVersion: s.GetVersion(),
		})
	}

	return reason, pluginOffer, true
}

// PluginIdentity is the plugin's self-reported identity, surfaced to the host
// for logging/metrics/compatibility policy. BinarySHA256 is the child's
// self-report only; the host verifies identity against its own pin via
// VerifyBinaryIdentity and never trusts this field for enforcement.
type PluginIdentity struct {
	Name         string
	SemVer       string
	BinarySHA256 string
}
