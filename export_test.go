package styx

import (
	"context"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	"golang.org/x/sys/unix"
)

// InProcessStreamPairForTest wires a *ClientConn (client end) to s's registered
// stream handlers (plugin end) over an in-process streaming socketpair, and
// returns the client conn plus a stop func. It lets an external test — including
// the generated-code streaming round-trip in package styx_test, which cannot
// reach package styx's own in-process helpers — exercise a GENERATED
// New<Service>Client against a GENERATED Register<Service>Server end to end
// without a spawned plugin process. stop closes both transports, joins the serve
// loop, and tears the streaming half down in the same order runServing does.
func InProcessStreamPairForTest(s *PluginServer) (*ClientConn, func(), error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	clientTr, err := transport.NewUDSTransport(fds[0], true)
	if err != nil {
		return nil, nil, err
	}
	pluginTr, err := transport.NewUDSTransport(fds[1], true)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	handlers := make(map[streamKey]streamHandlerReg, len(s.streamHandlers))
	for k, h := range s.streamHandlers {
		handlers[k] = h
	}
	s.mu.Unlock()

	srv := newStreamServer(pluginTr, handlers, codec.Proto{})
	dispatcher := rpcruntime.NewDispatcher()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pluginTr, dispatcher, srv)
	}()

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	stop := func() {
		_ = clientTr.Close()
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	}

	return cc, stop, nil
}

// HeartbeatIntervalEnv re-exports heartbeatIntervalEnv so pluginserver_test
// (external test package) can shorten or lengthen the heartbeat send interval
// to make the serving-control dispatch tests deterministic.
const HeartbeatIntervalEnv = heartbeatIntervalEnv

// RunServingControlForTest re-exports runServingControl for pluginserver_test
// (external test package): it drives the plugin's control-plane serving phase
// (successor restore, heartbeats, and the reload/shutdown dispatch loop) over a
// caller-supplied control.Conn, so the reload wiring can be exercised in-process
// against a scripted host without a real spawned child.
func (s *PluginServer) RunServingControlForTest(ctx context.Context, conn *control.Conn) error {
	return s.runServingControl(ctx, conn)
}

// RunServingForTest re-exports runServing for pluginserver_test (external test
// package): it drives the whole serving phase — the successor restore, the
// data-plane reader launch, and the control-plane serving loop — over a
// caller-supplied control.Conn and data-plane Transport, so the ordering
// between a successor's restore and the data-plane reader can be exercised
// in-process without a real spawned child.
func (s *PluginServer) RunServingForTest(
	ctx context.Context, conn *control.Conn, tr transport.Transport, streaming bool,
) error {
	return s.runServing(ctx, conn, tr, streaming)
}

// AddRuntimeForTest registers a pluginRuntime backed by sup under name, the way
// startOne does after a successful start, so a test can drive Host.Reload
// against a supervisor in a chosen state (e.g. one that has already given up)
// without spawning a real child process.
func (h *Host) AddRuntimeForTest(name string, sup *supervisor.Supervisor) {
	h.mu.Lock()
	h.runtimes = append(h.runtimes, &pluginRuntime{name: name, sup: sup})
	h.mu.Unlock()
}

// DroppedInformationalEventCounts re-exports h.bus's informational-event
// drop counters for host_test (external test package): it lets a test
// assert directly that Host's own fan-in actually counted a drop under a
// burst, not just infer it from which events survived.
func (h *Host) DroppedInformationalEventCounts() []uint64 {
	return h.bus.DroppedInformationalCounts()
}

// ToControlServiceRequirements re-exports toControlServiceRequirements for
// host_test (external test package): the pure PluginSpec.Services ->
// internal/supervisor.Config.Services translation, exercised directly
// here without needing a real spawn. The case-only difference from the
// unexported original is the intended re-export pattern, not a naming accident.
//
//nolint:revive // confusing-naming: intentional case-only re-export, see doc above
func ToControlServiceRequirements(reqs []ServiceRequirement) []control.ServiceRequirement {
	return toControlServiceRequirements(reqs)
}

// IncompatibleReason re-exports incompatibleReason for pluginserver_test
// (external test package): pluginHandshake's rejection-ack reason
// selection, exercised directly here since pluginHandshake itself cannot
// be driven without a real spawned child (see pluginserver_test.go's own
// doc). The case-only difference from the unexported original is the intended
// re-export pattern, not a naming accident.
//
//nolint:revive // confusing-naming: intentional case-only re-export, see doc above
func IncompatibleReason(err error) string {
	return incompatibleReason(err)
}
