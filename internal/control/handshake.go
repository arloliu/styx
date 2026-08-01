package control

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"unicode/utf8"

	"github.com/arloliu/styx/internal/control/controlpb"
)

// TransportSHM and TransportUDS name the two negotiable data-plane transports.
// The shared-memory transport carries a versioned region layout and so
// participates in layout-version negotiation; the Unix-domain-socket transport
// carries no region and fixes the negotiated layout version at 0.
const (
	TransportSHM = "shm"
	TransportUDS = "uds"
)

// ShmLayoutVersion is the shared-memory region layout version this build speaks.
// An Offer that lists the shared-memory transport advertises this value; Negotiate
// selects the highest version common to both sides.
// A future breaking layout ships as a new version the set would additionally advertise.
const ShmLayoutVersion uint32 = 1

// Offer is one side's declared capabilities for handshake negotiation.
// Services (required version range per service) is populated only on the host's offer,
// sourced from generated-code metadata; the plugin's offer leaves it nil.
// LayoutVersions is the set of shared-memory region layout versions this side can speak,
// populated only when the offer lists the shared-memory transport; left nil otherwise.
type Offer struct {
	ProtocolMin, ProtocolMax uint32
	Features                 []FeatureFlag
	Transports               []string
	Codecs                   []string
	Services                 []ServiceRequirement // host-only
	LayoutVersions           []uint32
}

// FeatureFlag is a named, independently versioned capability.
// Most protocol evolution happens here instead of via a protocol-version bump.
// Required is per-side: each side marks which flags it requires; Negotiate fails
// closed if either side requires a flag the other does not support.
// A side supports a flag by listing it in its Offer.Features; presence in the offer is the support signal.
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

// Tuple is the fully negotiated compatibility tuple: protocol version, transport,
// layout version, resolved feature set, and codec.
// Layout version is 0 for "uds" transport, or the highest version common to both
// sides for "shm" transport.
// Both sides acknowledge the identical Tuple before any region is attached,
// preventing untested combinations from running.
type Tuple struct {
	ProtocolVersion uint32
	Transport       string
	LayoutVersion   uint32
	Features        map[string]bool
	Codec           string
}

// IncompatibleError is the internal/control negotiation-failure type, distinct from
// the public styx.IncompatibleError to avoid an import cycle.
// Lifecycle code at the public-API boundary catches *IncompatibleError and constructs
// a *styx.IncompatibleError with equivalent values.
type IncompatibleError struct {
	HostOffer   Offer
	PluginOffer Offer
	Reason      string

	// Kind discriminates why negotiation failed; see IncompatibleKind.
	// The zero value, IncompatibleHandshake, is correct for any
	// construction that predates this field.
	Kind IncompatibleKind
}

func (e *IncompatibleError) Error() string {
	return "control: incompatible handshake: " + e.Reason
}

// IncompatibleKind discriminates the two reasons Negotiate/VerifyBinaryIdentity
// return *IncompatibleError. It mirrors the public styx.IncompatibleKind;
// internal/control cannot import styx, so the API boundary translation
// (host.go's translateIncompatible) maps between the two by value, not by cast.
type IncompatibleKind int

const (
	// IncompatibleHandshake reports an ordinary handshake negotiation failure.
	// This is the zero value, so every existing IncompatibleError construction
	// defaults to it correctly.
	IncompatibleHandshake IncompatibleKind = iota
	// IncompatibleBinaryIdentity reports that VerifyBinaryIdentity found the
	// plugin binary on disk does not match the pinned SHA-256 digest.
	IncompatibleBinaryIdentity

	// IncompatibleKindCount marks the end of the enum. It carries no meaning
	// of its own; it exists so a test can iterate every value this enum
	// currently defines and check it against the public-kind mapping in
	// host.go's toIncompatibleKind. Without it, a value added here without
	// also updating that mapping would compile and misclassify silently
	// instead of failing a test.
	IncompatibleKindCount
)

// Negotiate computes the compatibility tuple from the host's Offer, the plugin's
// Offer, and the plugin's advertised service versions.
//
// On success:
// - ProtocolVersion is the highest common protocol version (intersection of the two ranges)
// - Features maps every flag name either side offered to whether BOTH sides support it
// - Transport and Codec are the lexicographically first common entry (deterministic tie-break)
// - LayoutVersion is 0 for "uds", or the highest version common to both sides for "shm"
//
// Every failure returns a *IncompatibleError carrying both offers and a reason naming the offending axis.
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

	// Layout version — negotiated only for the shared-memory transport, where a
	// versioned region layout is mapped (shm-abi.md §19). Both sides advertise a
	// set; the single highest common version is selected, or the handshake fails
	// closed. The uds transport carries no region and fixes layout 0.
	layoutVersion := uint32(0)
	if transport == TransportSHM {
		v, common := highestCommonLayout(host.LayoutVersions, plugin.LayoutVersions)
		if !common {
			return fail("layout_version: no common shared-memory layout version")
		}
		layoutVersion = v
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
		LayoutVersion:   layoutVersion,
		Features:        features,
		Codec:           codec,
	}, nil
}

// highestCommonLayout returns the highest layout version present in both sets,
// and false when the intersection is empty (shm-abi.md §19: exact-match axis,
// highest common version selected). Order-independent — it scans a for any
// member of b, tracking the maximum — so the two sides need not advertise their
// sets in any particular order.
func highestCommonLayout(a, b []uint32) (uint32, bool) {
	set := make(map[uint32]struct{}, len(b))
	for _, v := range b {
		set[v] = struct{}{}
	}
	best, found := uint32(0), false
	for _, v := range a {
		if _, ok := set[v]; ok && (!found || v > best) {
			best, found = v, true
		}
	}

	return best, found
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
			Kind: IncompatibleBinaryIdentity,
		}
	}

	return nil
}

// OfferToHello builds a controlpb.Hello from an Offer and a per-launch nonce.
// Services is host-only; a plugin's Offer leaves it nil, yielding an empty
// repeated field.
func OfferToHello(o Offer, nonce uint64) *controlpb.Hello {
	h := &controlpb.Hello{
		ProtocolMin:    o.ProtocolMin,
		ProtocolMax:    o.ProtocolMax,
		Transports:     o.Transports,
		Codecs:         o.Codecs,
		Nonce:          nonce,
		LayoutVersions: o.LayoutVersions,
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
		ProtocolMin:    h.GetProtocolMin(),
		ProtocolMax:    h.GetProtocolMax(),
		Transports:     h.GetTransports(),
		Codecs:         h.GetCodecs(),
		LayoutVersions: h.GetLayoutVersions(),
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

// TupleToHelloAck builds a controlpb.HelloAck from a negotiated Tuple, echoed nonce,
// plugin identity, advertised service versions, and plugin offer.
// The resolved feature set is emitted with the Supported bit set so the peer sees
// exactly which flags are active.
// The plugin offer travels in plugin_offer so the host can recompute Negotiate and
// validate the acknowledged tuple before creating any region fd.
func TupleToHelloAck(
	t Tuple, nonce uint64, identity PluginIdentity, services []ServiceVersion, pluginOffer Offer,
) *controlpb.HelloAck {
	ack := &controlpb.HelloAck{
		ProtocolVersion: t.ProtocolVersion,
		Transport:       t.Transport,
		LayoutVersion:   t.LayoutVersion,
		Codec:           t.Codec,
		Nonce:           nonce,
		PluginName:      identity.Name,
		PluginSemver:    identity.SemVer,
		BinarySha256:    identity.BinarySHA256,
		PluginOffer:     OfferToHello(pluginOffer, 0),
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

// ValidateAcknowledgedTuple recomputes the negotiation from the host's offer and the
// plugin offer echoed in the ack, then rejects the ack unless every acknowledged axis
// matches that recomputation.
// The host cannot trust the acknowledged tuple on the plugin's word alone — a buggy or
// hostile plugin could acknowledge a transport, protocol, codec, or feature resolution
// neither side would compute.
// This function reruns Negotiate and compares protocol version, transport, codec,
// layout version, and the resolved feature set field-by-field.
// On success it returns the validated Tuple; on any mismatch, a *IncompatibleError,
// preventing region creation against an unverified tuple.
func ValidateAcknowledgedTuple(hostOffer Offer, ack *controlpb.HelloAck) (Tuple, error) {
	acked := HelloAckToTuple(ack)
	pluginOffer := HelloToOffer(ack.GetPluginOffer())
	pluginServices := serviceVersionsFromAck(ack)

	// Reject a duplicate service advertisement before any fd is created: a service
	// must appear at most once. Negotiate's checkServices indexes the advertised
	// versions into a map, so a forged ack repeating a service name would otherwise
	// be silently collapsed (a later entry overwriting an earlier one) and pass the
	// recompute-and-compare below.
	if dup, ok := firstDuplicateService(pluginServices); ok {
		return Tuple{}, &IncompatibleError{
			HostOffer:   hostOffer,
			PluginOffer: pluginOffer,
			Reason:      fmt.Sprintf("acknowledged tuple: service %q advertised more than once", dup),
		}
	}

	recomputed, err := Negotiate(hostOffer, pluginOffer, pluginServices)
	if err != nil {
		var ie *IncompatibleError
		if errors.As(err, &ie) {
			return Tuple{}, ie
		}

		return Tuple{}, &IncompatibleError{HostOffer: hostOffer, PluginOffer: pluginOffer, Reason: err.Error()}
	}

	if !tupleEqual(recomputed, acked) {
		return Tuple{}, &IncompatibleError{
			HostOffer:   hostOffer,
			PluginOffer: pluginOffer,
			Reason: fmt.Sprintf(
				"acknowledged tuple (transport=%q codec=%q protocol=%d layout=%d) does not match "+
					"host recomputation (transport=%q codec=%q protocol=%d layout=%d)",
				acked.Transport, acked.Codec, acked.ProtocolVersion, acked.LayoutVersion,
				recomputed.Transport, recomputed.Codec, recomputed.ProtocolVersion, recomputed.LayoutVersion,
			),
		}
	}

	return recomputed, nil
}

// tupleEqual reports whether two negotiated tuples agree on every axis: protocol
// version, transport, layout version, codec, and the resolved feature set.
func tupleEqual(a, b Tuple) bool {
	return a.ProtocolVersion == b.ProtocolVersion &&
		a.Transport == b.Transport &&
		a.LayoutVersion == b.LayoutVersion &&
		a.Codec == b.Codec &&
		maps.Equal(a.Features, b.Features)
}

// firstDuplicateService returns the first service name that appears more than
// once in svs, and whether one was found. It preserves the pre-collapse view a
// map cannot: checkServices later indexes these into a map, so a duplicate would
// otherwise be invisible to the recompute-and-compare.
func firstDuplicateService(svs []ServiceVersion) (string, bool) {
	seen := make(map[string]struct{}, len(svs))
	for _, sv := range svs {
		if _, dup := seen[sv.Service]; dup {
			return sv.Service, true
		}
		seen[sv.Service] = struct{}{}
	}

	return "", false
}

// serviceVersionsFromAck projects the plugin's advertised service versions off a
// HelloAck (the same field a success and a rejection ack both carry), for the
// host's recompute-and-compare.
func serviceVersionsFromAck(ack *controlpb.HelloAck) []ServiceVersion {
	svs := ack.GetServices()
	if len(svs) == 0 {
		return nil
	}
	out := make([]ServiceVersion, len(svs))
	for i, s := range svs {
		out[i] = ServiceVersion{Service: s.GetService(), Version: s.GetVersion()}
	}

	return out
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

// IncompatibleToHelloAck builds a rejection HelloAck for a plugin-side Negotiate failure.
// nonce is echoed so VerifyNonce still passes on the host.
// The plugin's full Offer travels in plugin_offer so the host can reconstruct the
// plugin's multi-valued offer via HelloAckIncompatible without loss.
// The singular success-ack fields (transport, codec, resolved feature list) stay
// unset on rejection.
// reason is bounded to MaxIncompatibleReasonBytes (truncated on a UTF-8 boundary)
// and replaced with a generic message if empty.
func IncompatibleToHelloAck(offer Offer, services []ServiceVersion, reason string, nonce uint64) *controlpb.HelloAck {
	if reason == "" {
		reason = genericIncompatibleReason
	}
	reason = truncateUTF8(reason, MaxIncompatibleReasonBytes)

	ack := &controlpb.HelloAck{
		Nonce:              nonce,
		IncompatibleReason: reason,
		PluginOffer:        OfferToHello(offer, 0),
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

// HelloAckIncompatible reports whether ack is a rejection reply (IncompatibleReason
// non-empty), returning the reason, the plugin's Offer reconstructed from
// plugin_offer, and whether rejection occurred.
// An ordinary success ack always leaves IncompatibleReason empty, so rejected is
// false and pluginOffer is the zero Offer.
func HelloAckIncompatible(ack *controlpb.HelloAck) (reason string, pluginOffer Offer, rejected bool) {
	reason = ack.GetIncompatibleReason()
	if reason == "" {
		return "", Offer{}, false
	}

	pluginOffer = HelloToOffer(ack.GetPluginOffer())
	for _, s := range ack.GetServices() {
		pluginOffer.Services = append(pluginOffer.Services, ServiceRequirement{
			Service: s.GetService(), MinVersion: s.GetVersion(), MaxVersion: s.GetVersion(),
		})
	}

	return reason, pluginOffer, true
}

// PluginIdentity is the plugin's self-reported identity, surfaced to the host for
// logging, metrics, and compatibility policy.
// BinarySHA256 is the child's self-report; the host verifies identity against its
// own pin via VerifyBinaryIdentity and never trusts this field for enforcement.
type PluginIdentity struct {
	Name         string
	SemVer       string
	BinarySHA256 string
}
