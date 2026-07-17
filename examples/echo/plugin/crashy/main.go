// Command echo-crashy-plugin is examples/echo/plugin's deliberately
// misbehaving twin, used only by tests/integration/echo_test.go to drive
// Styx's crash-timing and restart-policy error taxonomy — scenarios the
// real examples/echo/plugin, by design, never triggers. It
// still registers the real generated EchoServer (so a well-behaved call
// round-trips normally), but argv[1] selects one additional misbehavior:
//
//   - "slow": Say synchronizes on the STYX_ECHO_CRASHY_FIFO checkpoint
//     (see syncFIFO below) if set, then sleeps for
//     STYX_ECHO_CRASHY_DELAY (default 1s) before responding normally —
//     it never crashes. Gives a client-side deadline or cancellation
//     something genuinely in-flight to race against, deterministically:
//     the checkpoint proves the handler was entered before the test
//     acts, with no sleep-and-hope polling on the test's side.
//   - "midcall": Say synchronizes on the STYX_ECHO_CRASHY_FIFO checkpoint
//     (required for this mode) and then os.Exit(1)s — by the time this
//     runs, the client's request-table entry is provably already
//     StatePublished (Publish always precedes Send in ClientConn.Invoke),
//     so the crash always lands in the "may have been dispatched" window.
//   - "startup": spawns a goroutine that blocks on the
//     STYX_ECHO_CRASHY_FIFO checkpoint (required for this mode) before
//     Serve begins, then os.Exit(1)s — used to crash a plugin that has
//     already reached Ready without needing any RPC at all, once the
//     test itself is ready for it (again, no sleep-and-hope: the test
//     awaits the real EventReady before releasing this checkpoint).
//   - "status": Say returns a *styx.Status application error
//     (CodeInvalidArgument, message echoing the request) instead of a
//     normal response — exercises the end-to-end status-framing path: the
//     client must errors.As-recover an equal *styx.Status.
//
// The checkpoint is a single named pipe (FIFO) whose path is passed via
// STYX_ECHO_CRASHY_FIFO: opening it for read blocks until a peer opens it
// for write (and vice versa) — true blocking synchronization with the
// test process, not a timer.
package main

import (
	"context"
	"os"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// syncFIFO opens the STYX_ECHO_CRASHY_FIFO checkpoint for reading, if set,
// blocking until the test process opens the same path for writing. It is
// a no-op if the env var is unset (the "slow" mode's deadline-propagation
// use does not need a checkpoint at all).
func syncFIFO() {
	fifo := os.Getenv("STYX_ECHO_CRASHY_FIFO")
	if fifo == "" {
		return
	}

	f, err := os.OpenFile(fifo, os.O_RDONLY, 0)
	if err == nil {
		_ = f.Close()
	}
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

type crashyServer struct {
	mode string
}

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
	}

	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

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

	srv := styx.NewPluginServer()
	echopb.RegisterEchoServer(srv, crashyServer{mode: mode})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
