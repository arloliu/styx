package styx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
)

// Negotiation constants: a single protocol version over the uds transport
// with the proto codec. internal/supervisor's own handshake orchestration
// and pluginserver.go's plugin-side negotiation both resolve against this
// fixed offer.
const (
	firstGeneration   uint64 = 1
	m1ProtocolVersion uint32 = 1
	transportUDS             = "uds"
	codecProto               = "proto"
)

// HostConfig configures a Host before Start.
type HostConfig struct {
	Plugins []PluginSpec
}

// PluginSpec declares one plugin the Host spawns and supervises.
type PluginSpec struct {
	Name    string
	Path    string
	Args    []string
	Env     []string // additional vars merged onto the sanitized base env
	Restart RestartPolicy

	// BinarySHA256 optionally pins the plugin binary's identity. When
	// non-nil, Start verifies it host-side, before any
	// supervisor is created for this plugin, and fails the plugin with
	// *IncompatibleError on mismatch. nil disables pinning.
	BinarySHA256 []byte

	// Services optionally declares, per service this Host intends to call
	// on this plugin, the version range the plugin must satisfy — the host
	// declares a required version range per service it intends to call,
	// and a plugin that cannot satisfy it fails handshake with the
	// offending service named in the error. Typically populated from a
	// generated `<Service>Requirement()` value rather than constructed by
	// hand. Start sends this on the Hello offer; the plugin's own
	// control.Negotiate call enforces it against its advertised
	// ServiceVersions, and a violation surfaces as *IncompatibleError
	// naming the offending service. nil (the default) declares no
	// requirements — every service version is accepted.
	Services []ServiceRequirement
}

// ServiceRequirement is the host's declared acceptable version range for
// one service it intends to call on a plugin, fed to PluginSpec.Services.
// A generated `<Service>Requirement()` returns the exact-version form
// (MinVersion == MaxVersion == the generated service's own version) most
// callers should pass; a wider range is a hand-authored option this type
// permits but that no generator output currently produces.
type ServiceRequirement struct {
	Service                string
	MinVersion, MaxVersion uint32
}

// Host manages the plugins declared in a HostConfig end to end: spawning,
// handshake, supervision, and teardown. Generated client stubs reach a
// plugin through the *ClientConn Plugin returns. Unexported fields hold
// the configuration and the live plugin routing table Start populates.
type Host struct {
	mu       sync.Mutex
	cfg      HostConfig
	plugins  map[string]*ClientConn
	runtimes []*pluginRuntime

	// bus fans every plugin's relayed events into the one channel
	// Events() exposes, using internal/supervisor.Bus's own
	// drop-oldest-informational / coalesce-latest-critical semantics — the
	// identical contract each plugin's own internal/supervisor.EventBus
	// already applies one level down, now
	// also applied to Host's fan-IN from potentially many plugins so a
	// GaveUp from one plugin can never be silently lost behind a burst of
	// events from others, even with an unread Events() channel.
	bus    *supervisor.Bus[Event]
	events <-chan Event
}

// pluginRuntime is the host-side handle to one supervised plugin: the
// internal/supervisor.Supervisor driving its spawn/heartbeat/restart
// lifecycle, and the plumbing Stop needs to end its background relay
// goroutine cleanly.
type pluginRuntime struct {
	name      string
	sup       *supervisor.Supervisor
	unsub     func()
	stopRelay chan struct{}
	relayDone chan struct{}
}

// hostCriticalEventKinds are the lifecycle-critical Host-level event
// kinds: Crashed and GaveUp (there is no Poisoned kind in the six-symbol
// event stream yet, see event.go). Mirrors
// internal/supervisor.EventKind.isCritical exactly, one layer up.
func hostEventIsCritical(e Event) bool {
	return e.Kind == EventCrashed || e.Kind == EventGaveUp
}

// NewHost creates a Host from the given configuration but does not start it.
func NewHost(cfg HostConfig) *Host {
	bus := supervisor.NewBus(hostEventIsCritical)
	events, _ := bus.Subscribe() // never unsubscribed: this is Events()'s channel for the Host's whole life.

	return &Host{
		cfg:     cfg,
		plugins: make(map[string]*ClientConn),
		bus:     bus,
		events:  events,
	}
}

// Start spawns every configured plugin, completes its handshake, and
// begins supervisor heartbeat monitoring for each — one
// internal/supervisor.Supervisor per PluginSpec, running in its
// own goroutine for the plugin's whole life. Start blocks per plugin only
// until that plugin's FIRST attempt either reaches Ready or gives up
// (Config.Restart.Max exceeded on the first attempt already, e.g. the
// zero-value RestartPolicy{}); once Ready, ongoing heartbeat monitoring
// and any later restarts continue in the background and are reported only
// via Events. A single plugin's failure does not abort the others; it is
// reported via Events and Start's returned error is the combined
// (errors.Join) set of any that failed.
func (h *Host) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	var errs []error
	for _, spec := range h.cfg.Plugins {
		if err := h.startOne(ctx, spec); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// startOne verifies spec's optional binary pin, then creates and launches
// a Supervisor for it, blocking until its first attempt reaches Ready or
// gives up. On success it installs the plugin's routing into h.plugins and
// records its pluginRuntime for Stop; on any failure it leaves no
// goroutine, subscription, or running Supervisor behind.
func (h *Host) startOne(ctx context.Context, spec PluginSpec) error {
	if spec.BinarySHA256 != nil {
		if verr := control.VerifyBinaryIdentity(spec.Path, spec.BinarySHA256); verr != nil {
			err := translateIncompatible(verr)
			h.publish(Event{Plugin: spec.Name, Kind: EventStarting, Time: time.Now()})
			h.publish(Event{Plugin: spec.Name, Kind: EventCrashed, Time: time.Now(), Err: err})

			return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
		}
	}

	cc := newUnavailableClientConn(spec.Name)
	bus := supervisor.NewEventBus()
	cfg := supervisor.Config{
		Spec:     lifecycle.Spec{Path: spec.Path, Args: spec.Args, Env: spec.Env},
		Restart:  spec.Restart,
		Services: toControlServiceRequirements(spec.Services),
		OnReady:  func(inst supervisor.Instance) supervisor.ReadyHooks { return wireConnState(cc, inst) },
	}
	sup := supervisor.New(cfg, bus)

	events, unsub := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)

	go h.relayEvents(spec.Name, events, stopRelay, relayDone, firstOutcome)
	//nolint:gosec // sup.Run's lifetime is host-scoped (until sup.Stop, e.g. on
	// Host shutdown), not scoped to ctx, which only bounds this call's wait for
	// the first Ready/GaveUp outcome below.
	go sup.Run(context.Background())

	var startErr error
	var fromSupervisor bool
	select {
	case err := <-firstOutcome:
		startErr, fromSupervisor = err, true
	case <-ctx.Done():
		startErr = ctx.Err()
	}

	if startErr != nil {
		_ = sup.Stop(context.Background())
		close(stopRelay)
		<-relayDone
		unsub()

		// Only translate an error that actually came from
		// internal/supervisor (a GaveUp reason, possibly wrapping a bare
		// *supervisor.CrashInfo — see translateEventErr's doc); ctx.Err()
		// is already a public stdlib error and must keep its identity so
		// errors.Is(err, context.Canceled/DeadlineExceeded) still works.
		if fromSupervisor {
			startErr = translateEventErr(spec.Name, startErr)
		}

		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, startErr)
	}

	h.plugins[spec.Name] = cc
	h.runtimes = append(h.runtimes, &pluginRuntime{
		name: spec.Name, sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
	})

	return nil
}

// relayEvents forwards every internal/supervisor.Event this plugin's bus
// publishes onto Host's own Events() channel (translated to styx.Event)
// for as long as the plugin runs, until stopRelay is closed. It also
// reports this plugin's FIRST Ready or GaveUp on firstOutcome (nil for
// Ready, the GaveUp reason otherwise), non-blockingly — startOne only
// waits for the first one.
func (h *Host) relayEvents(
	name string, events <-chan supervisor.Event, stopRelay, relayDone chan struct{}, firstOutcome chan<- error,
) {
	defer close(relayDone)

	for {
		select {
		case ev := <-events:
			h.publish(translateEvent(name, ev))

			//exhaustive:ignore -- only Ready/GaveUp affect firstOutcome; every
			// other kind is already handled above via h.publish.
			switch ev.Kind {
			case supervisor.EventReady:
				select {
				case firstOutcome <- nil:
				default:
				}
			case supervisor.EventGaveUp:
				select {
				case firstOutcome <- ev.Err:
				default:
				}
			}
		case <-stopRelay:
			return
		}
	}
}

// Stop drains and shuts down every plugin via its Supervisor's own Stop
// (which runs the normative teardown machine) and blocks until
// every child has been reaped.
func (h *Host) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	runtimes := h.runtimes
	h.runtimes = nil
	h.plugins = make(map[string]*ClientConn)
	h.mu.Unlock()

	var errs []error
	for _, rt := range runtimes {
		if err := rt.sup.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("styx: stop plugin %q: %w", rt.name, err))
		}
		close(rt.stopRelay)
		<-rt.relayDone
		rt.unsub()
	}

	return errors.Join(errs...)
}

// Plugin returns the named plugin's client connection, or a ClientConn
// that fails every call with ErrPluginUnavailable if the plugin isn't
// running — generated constructors accept this return value directly,
// mirroring grpc.ClientConnInterface.
func (h *Host) Plugin(name string) *ClientConn {
	h.mu.Lock()
	cc, ok := h.plugins[name]
	h.mu.Unlock()

	if ok {
		return cc
	}

	return newUnavailableClientConn(name)
}

// Events returns a channel of supervisor lifecycle events for every plugin
// this Host manages (the Starting/Ready/Unhealthy/Crashed/Restarting/
// GaveUp event stream). Delivery follows the same non-blocking, bounded,
// two-class contract internal/supervisor.EventBus documents:
// informational events (Starting, Ready, Unhealthy, Restarting) drop the
// oldest queued one once the reader falls behind; Crashed and GaveUp
// coalesce to the latest instead of ever being silently dropped, even
// under a sustained burst from many plugins with no reader at all.
func (h *Host) Events() <-chan Event {
	return h.events
}

// publish delivers e to every Events() subscriber (today, always exactly
// one — the channel Events() itself returns) via h.bus, which applies the
// real drop-oldest/coalesce-to-latest policy described on Events(). It
// never blocks regardless of whether anything is reading Events().
func (h *Host) publish(e Event) {
	h.bus.Publish(e)
}

// wireConnState builds a fresh connState from a newly-Ready
// supervisor.Instance and installs it onto cc, starting its read loop. It
// is the restart-time analogue of newClientConn: newClientConn wires a
// brand-new ClientConn at first start, while wireConnState re-wires the
// SAME *ClientConn identity Host.Plugin already handed callers, on every
// Ready after a restart, so a caller holding that pointer transparently
// sees the new instance. The returned ReadyHooks are internal/supervisor's
// caller-specific Teardown callbacks (StopAdmission/FailInFlight/
// JoinGoroutines) for tearing this exact wiring back down.
//
// On a crash-restart, a successor is never promoted Ready while its
// predecessor is still alive or unreaped — internal/supervisor.Supervisor.Run
// never spawns a new instance (and so never calls this again) until the
// previous one's Teardown, built from the ReadyHooks returned here, has
// fully completed. A hot-reload promotion does not go through that
// machinery: it swaps cc's routing to the successor's state and only
// afterward tears the predecessor down, so a predecessor's Teardown can run
// after a later generation already owns cc's routing. StopAdmission
// therefore checks ownership before acting (see its own comment below)
// rather than assuming it is always the most recent instance.
func wireConnState(cc *ClientConn, inst supervisor.Instance) supervisor.ReadyHooks {
	state := &connState{
		table:        rpcruntime.NewTable(inst.Generation),
		tr:           inst.Transport,
		codec:        codec.Proto{},
		readLoopDone: make(chan struct{}),
	}
	cc.state.Store(state)
	cc.admission.Open()
	go func() {
		defer close(state.readLoopDone)
		runReadLoop(state)
	}()

	return supervisor.ReadyHooks{
		// CompareAndSwap only tears down routing this exact instance still
		// owns: on a plain crash-restart cc.state still holds state, so the
		// swap succeeds and this behaves as an unconditional close always
		// did. After a hot-reload has promoted a later generation, cc.state
		// holds the successor's state instead, the swap fails, and this
		// becomes a no-op — a predecessor's teardown must never close
		// admission or null out routing out from under a successor that is
		// already serving.
		StopAdmission: func() {
			if cc.state.CompareAndSwap(state, nil) {
				cc.admission.Close()
			}
		},
		// Same split-by-dispatch-state reasoning as the earlier teardown
		// wiring: a call whose request frame was never
		// published is provably not-dispatched (retryable); a published
		// call's outcome is unknown (not retryable). Run always passes
		// lifecycle.ErrTornDown, which is intentionally not used here —
		// FailAll's own two errors are the caller-facing ones.
		FailInFlight: func(error) {
			state.table.FailAll(ErrOutcomeUnknown, ErrPluginUnavailable)
		},
		// Closing the transport is what unblocks the read loop's Recv;
		// there is no separate writer goroutine in this inline-send design.
		JoinGoroutines: func() {
			_ = state.tr.Close()
			<-state.readLoopDone
		},
	}
}

// translateEvent converts one internal/supervisor.Event into the public
// styx.Event, attaching the plugin name (internal/supervisor.Event has no
// such field — one Supervisor only ever supervises one plugin, so its
// events carry no name of their own) and translating Err at the boundary.
func translateEvent(name string, ev supervisor.Event) Event {
	return Event{Plugin: name, Kind: translateEventKind(ev.Kind), Time: ev.Time, Err: translateEventErr(name, ev.Err)}
}

// translateEventErr converts an internal/supervisor error into a public
// one. This is the same translate-at-boundary rule used elsewhere for
// IncompatibleError/HandshakeOffer: a bare *supervisor.CrashInfo or
// *control.IncompatibleError (or any other internal-package-originated
// error) can never cross onto Event.Err as-is — internal/ packages are not
// importable outside this module, so an external caller could never even
// name the type to errors.As into it.
//
// *control.IncompatibleError is checked FIRST, ahead of *CrashInfo: a
// handshake rejection (the control.IncompatibleToHelloAck path —
// pluginHandshake, then internal/supervisor.handshakeAndAttach) always
// reaches here wrapped inside a *CrashInfo too (runOneInstance's
// crashReason treats every pre-attach failure uniformly), so errors.As
// would find *CrashInfo first if it ran first; checking IncompatibleError
// first ensures a negotiation failure is always reported through
// supervisor events rather than a silent fallback, surfacing as the more
// specific *IncompatibleError, not a generic *PluginCrashError that
// discards the offending service/reason. Otherwise, a
// *supervisor.CrashInfo becomes a *PluginCrashError, carrying the real
// ExitStatus/ExitStatusKnown through when internal/lifecycle.Teardown
// reaped the process (see CrashInfo's own doc for when it did not);
// anything else keeps its message but not its internal identity.
func translateEventErr(name string, err error) error {
	if err == nil {
		return nil
	}

	var ie *control.IncompatibleError
	if errors.As(err, &ie) {
		return translateIncompatible(err)
	}

	var ci *supervisor.CrashInfo
	if errors.As(err, &ci) {
		// Dispatched is intentionally always false at the event level here —
		// see PluginCrashError.Dispatched's field doc for why it cannot be
		// derived here and why per-call errors, not this event, carry the
		// authoritative dispatch truth.
		return &PluginCrashError{
			Plugin: name, ExitStatus: ci.ExitStatus, ExitStatusKnown: ci.ExitStatusKnown,
			Reason: err.Error(), Dispatched: false,
		}
	}

	return errors.New(err.Error())
}

// toControlServiceRequirements projects PluginSpec.Services into
// internal/supervisor.Config's own type: a pure, allocation-free-when-
// empty mapping with no other logic — enforcement lives entirely in
// control.Negotiate, this only carries the requirement across the
// styx/internal/supervisor boundary. nil in, nil out. export_test.go's
// ToControlServiceRequirements deliberately re-exports this for tests
// outside the package; the case-only difference is the intended shape of
// that pattern, not a naming accident.
//
//nolint:revive // see doc above
func toControlServiceRequirements(reqs []ServiceRequirement) []control.ServiceRequirement {
	if len(reqs) == 0 {
		return nil
	}

	out := make([]control.ServiceRequirement, len(reqs))
	for i, r := range reqs {
		out[i] = control.ServiceRequirement{Service: r.Service, MinVersion: r.MinVersion, MaxVersion: r.MaxVersion}
	}

	return out
}

// translateEventKind maps internal/supervisor.EventKind to styx.EventKind.
// Written as an explicit switch rather than a same-index int conversion so
// the two enums are never silently allowed to drift out of sync.
// EventCrashed intentionally falls through to default (same return value)
// rather than an explicit case, to avoid identical-switch-branches — see
// default's comment below.
func translateEventKind(k supervisor.EventKind) EventKind {
	//exhaustive:ignore -- see doc above
	switch k {
	case supervisor.EventStarting:
		return EventStarting
	case supervisor.EventReady:
		return EventReady
	case supervisor.EventUnhealthy:
		return EventUnhealthy
	case supervisor.EventRestarting:
		return EventRestarting
	case supervisor.EventGaveUp:
		return EventGaveUp
	default:
		// Also covers supervisor.EventCrashed: same branch as an unrecognized
		// kind, so revive's identical-switch-branches doesn't flag a
		// duplicate case that returns the same value as default.
		return EventCrashed
	}
}

// translateIncompatible maps an internal *control.IncompatibleError to the
// public *styx.IncompatibleError at the API boundary, leaving any other
// error unchanged. internal/control must not import styx, so the
// translation lives here.
func translateIncompatible(err error) error {
	var ie *control.IncompatibleError
	if errors.As(err, &ie) {
		return &IncompatibleError{
			HostOffer:   toHandshakeOffer(ie.HostOffer),
			PluginOffer: toHandshakeOffer(ie.PluginOffer),
			Reason:      ie.Reason,
		}
	}

	return err
}

// toHandshakeOffer projects an internal control.Offer into the public,
// stable, printable HandshakeOffer (names only for features; required/
// optional detail lives in the Reason string). Services carries through
// structurally — o.Services is already []control.ServiceRequirement on
// both HostOffer (the host's real
// requirements) and, when reconstructed from a handshake rejection ack
// (control.HelloAckIncompatible), PluginOffer (the plugin's own advertised
// versions, reported as a degenerate exact-version requirement — see
// HandshakeOffer.Services's doc).
func toHandshakeOffer(o control.Offer) HandshakeOffer {
	names := make([]string, 0, len(o.Features))
	for _, f := range o.Features {
		names = append(names, f.Name)
	}

	var services []ServiceRequirement
	if len(o.Services) > 0 {
		services = make([]ServiceRequirement, len(o.Services))
		for i, s := range o.Services {
			services[i] = ServiceRequirement{Service: s.Service, MinVersion: s.MinVersion, MaxVersion: s.MaxVersion}
		}
	}

	return HandshakeOffer{
		ProtocolMin: o.ProtocolMin,
		ProtocolMax: o.ProtocolMax,
		Transports:  o.Transports,
		Codecs:      o.Codecs,
		Features:    names,
		Services:    services,
	}
}
