// Command echo-crashy-plugin deliberately misbehaves to test crash-timing,
// restart policies, hot-reload, wedge detection, and panic handling — all
// scenarios the normal example avoids by design. It registers the same
// EchoServer and EchoStreamServer so well-behaved calls round-trip normally,
// but argv[1] selects which misbehavior to trigger:
//
//   - "slow": Say synchronizes on the STYX_ECHO_CRASHY_FIFO checkpoint (see
//     syncFIFO) if set, then sleeps for STYX_ECHO_CRASHY_DELAY (default 1s)
//     before responding normally — it never crashes. Gives a client-side
//     deadline or cancellation something genuinely in-flight to race against.
//   - "midcall": Say synchronizes on the checkpoint (required) and then
//     os.Exit(1)s — the client's request-table entry is provably already
//     StatePublished, so the crash always lands in the "may have been
//     dispatched" window.
//   - "startup": a goroutine blocks on the checkpoint (required) before Serve
//     begins, then os.Exit(1)s — crashes a plugin that has already reached
//     Ready without needing any RPC.
//   - "status": Say returns a *styx.Status application error instead of a
//     normal response — exercises the end-to-end status-framing path.
//   - "echo": Say echoes its request back immediately. With STYX_ECHO_PID_TAG
//     set it prefixes its own process id ("<pid>:<message>") so a caller can
//     tell which instance served each call across a hot-reload. The single
//     reloadGateMessage request is instead parked mid-handler (signalling entry
//     on STYX_ECHO_ENTER_FIFO, then blocking on STYX_ECHO_RELEASE_FIFO) so a
//     test can prove an in-flight call finishes on the predecessor.
//   - "panic": Say panics on the sentinel request "panic" and echoes any other
//     request, so the panic policy can be exercised while the restarted or
//     still-serving instance still answers a following call.
//   - "wedge": Say synchronizes on the checkpoint (required, to prove the
//     handler was entered) and then blocks forever, but only for the
//     wedgeBlockMessage sentinel request; any other request echoes immediately
//     (pid-tagged when STYX_ECHO_PID_TAG is set), so a restarted successor can
//     be observed serving. Because a unary handler runs inline on the serve
//     loop, a wedged one freezes the plugin's inbound consume counter; a second
//     request queued behind it then makes the plugin report inbound work still
//     readable, which is the transport-wedge the supervisor restarts on. With
//     no second request the same blocked handler stays healthy — its lease keeps
//     renewing — which is the owed-response-with-live-lease case the classifier
//     must NOT wedge on.
//
// Independent of the mode, two environment switches shape hot-reload and panic
// behavior:
//
//   - STYX_ECHO_RESTORE_FAIL: registers a hot-reload state restorer that always
//     fails. A restorer runs only on a freshly spawned reload successor, so the
//     first-start instance serves normally; a reload then fails in its restore-
//     and-validate phase and rolls back onto this still-serving instance.
//   - STYX_ECHO_CONTINUE_AFTER_PANIC: opts the server into continuing to serve
//     after a handler panic, instead of the default taint-and-restart.
//
// STYX_ECHO_STREAM selects the server-streaming Feed behavior: "block" streams
// one response and then waits on the stream context (so a caller can cancel or
// let a deadline expire mid-stream), "panic" panics from the handler; unset
// streams a fixed deterministic sequence.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// reloadGateMessage is the sentinel request the "echo" mode parks mid-handler
// so a hot-reload test can prove an already-executing call completes on this
// (predecessor) instance. Every other request echoes immediately, so ordinary
// load traffic runs unaffected while the single gated call is held.
const reloadGateMessage = "gated"

// wedgeBlockMessage is the sentinel request the "wedge" mode blocks forever on.
// Only that one request wedges the serve loop; every other request echoes, so a
// caller can still get an answer out of a wedge-mode instance — which is how a
// test tells a restarted successor apart from the wedged predecessor that could
// answer nothing. A mode that blocked on every request would make the process
// serving after a restart indistinguishable from the one before it.
const wedgeBlockMessage = "wedge"

type crashyServer struct {
	mode string
}

// crashyStreamServer registers the three streaming shapes. Collect and Chat are
// deterministic; Feed's behavior is switched by STYX_ECHO_STREAM so a test can
// cancel or time out mid-stream, or observe a streaming-handler panic.
type crashyStreamServer struct{}

// failingRestorer is a hot-reload state restorer that always refuses the
// snapshot. Because a restorer runs only on a freshly spawned reload successor,
// registering it makes every reload roll back in its restore-and-validate phase
// while leaving the first-start instance serving normally.
type failingRestorer struct{}

func (s crashyServer) Say(_ context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	switch s.mode {
	case "midcall":
		syncFIFO()
		os.Exit(1)
	case "slow":
		syncFIFO()
		time.Sleep(crashyDelay(time.Second))
	case "status":
		return nil, &styx.Status{
			Code:    styx.CodeInvalidArgument,
			Message: "rejected: " + req.GetMessage(),
		}
	case "echo":
		// Ordinary echo, except the reload-gate sentinel request is parked
		// mid-handler so a hot-reload test can prove it completes on this
		// instance. Only that one request blocks; all other traffic echoes now.
		gateInFlightReloadCall(req.GetMessage())
	case "panic":
		// Panic only on a sentinel request, so a restarted (default policy) or
		// still-serving (continue-after-panic) instance echoes any other request
		// normally — which is how a test confirms the next call succeeds.
		if req.GetMessage() == "panic" {
			panic("styx echo test: deliberate unary handler panic")
		}
	case "wedge":
		// The sentinel request proves the handler was entered (the serve loop is now
		// inside it and no longer consuming inbound frames), then blocks forever.
		// Teardown reaps the process; the handler never returns on its own. Any other
		// request falls through and echoes, so a successor spawned after the wedged
		// instance is torn down can be observed answering.
		if req.GetMessage() == wedgeBlockMessage {
			syncFIFO()
			select {}
		}
	}

	msg := req.GetMessage()
	if envSet("STYX_ECHO_PID_TAG") {
		msg = strconv.Itoa(os.Getpid()) + ":" + msg
	}

	return &echopb.SayResponse{Message: msg}, nil
}

// Blob echoes the payload back unchanged. None of the misbehavior modes above
// apply here — they exist to exercise Say's crash-timing and status-framing
// paths, which Blob does not need its own copy of.
func (crashyServer) Blob(_ context.Context, req *echopb.BlobRequest) (*echopb.BlobResponse, error) {
	return &echopb.BlobResponse{Payload: req.GetPayload()}, nil
}

func (crashyStreamServer) Collect(stream echopb.EchoStream_CollectServer) error {
	var all string
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		all += req.GetMessage()
	}

	return stream.SendAndClose(&echopb.SayResponse{Message: "collected:" + all})
}

func (crashyStreamServer) Feed(req *echopb.SayRequest, stream echopb.EchoStream_FeedServer) error {
	switch os.Getenv("STYX_ECHO_STREAM") {
	case "panic":
		panic("styx echo test: deliberate streaming handler panic")
	case "block":
		// Stream one response so the caller has a delivered frame to observe, then
		// wait on the stream context: the caller cancels it or lets a deadline
		// expire, and the handler returns that context error.
		if err := stream.Send(&echopb.SayResponse{Message: req.GetMessage() + "-0"}); err != nil {
			return err
		}
		<-stream.Context().Done()

		return stream.Context().Err()
	}

	for i := range 3 {
		if err := stream.Send(&echopb.SayResponse{Message: fmt.Sprintf("%s-%d", req.GetMessage(), i)}); err != nil {
			return err
		}
	}

	return nil
}

func (crashyStreamServer) Chat(stream echopb.EchoStream_ChatServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echopb.SayResponse{Message: "chat:" + req.GetMessage()}); err != nil {
			return err
		}
	}
}

func (failingRestorer) RestoreState(_ context.Context, _ uint32, _ []byte) error {
	return errors.New("styx echo test: deliberate restore failure")
}

// syncFIFO opens the STYX_ECHO_CRASHY_FIFO checkpoint for reading, if set,
// blocking until the test process opens the same path for writing. It is a
// no-op if the env var is unset.
func syncFIFO() { syncFIFOPath(os.Getenv("STYX_ECHO_CRASHY_FIFO")) }

// syncFIFOPath opens path for reading, if non-empty, blocking until the test
// process opens the same path for writing, then closes it. Opening a fifo for
// read blocks until a writer arrives, so pairing proves both sides reached the
// checkpoint with no timer or polling. A no-op for an empty path.
func syncFIFOPath(path string) {
	if path == "" {
		return
	}

	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err == nil {
		_ = f.Close()
	}
}

// gateInFlightReloadCall parks the reload-gate sentinel call inside the handler
// so a test can prove a hot-reload completes an already-executing call on this
// instance: it signals entry on the enter fifo, then blocks on the release fifo
// until the test lets it finish. Both fifos must be configured; any other
// request, or an unconfigured gate, returns immediately.
func gateInFlightReloadCall(message string) {
	if message != reloadGateMessage {
		return
	}

	enter := os.Getenv("STYX_ECHO_ENTER_FIFO")
	release := os.Getenv("STYX_ECHO_RELEASE_FIFO")
	if enter == "" || release == "" {
		return
	}

	syncFIFOPath(enter)   // signal that the handler has entered
	syncFIFOPath(release) // park until the test releases the call
}

// crashyDelay resolves STYX_ECHO_CRASHY_DELAY, or def if unset/invalid.
func crashyDelay(def time.Duration) time.Duration {
	if v := os.Getenv("STYX_ECHO_CRASHY_DELAY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}

	return def
}

// envSet reports whether name is present and non-empty.
func envSet(name string) bool { return os.Getenv(name) != "" }

func main() {
	mode := "slow"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	if mode == "startup" {
		go func() {
			syncFIFO()
			os.Exit(1)
		}()
	}

	srv := styx.NewPluginServer(styx.PluginServerConfig{
		ContinueAfterPanic: envSet("STYX_ECHO_CONTINUE_AFTER_PANIC"),
	})
	if envSet("STYX_ECHO_RESTORE_FAIL") {
		srv.RegisterStateRestorer(failingRestorer{})
	}
	echopb.RegisterEchoServer(srv, crashyServer{mode: mode})
	echopb.RegisterEchoStreamServer(srv, crashyStreamServer{})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
