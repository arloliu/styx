package lifecycle

import (
	"context"
	"io"
	"os"

	"github.com/arloliu/styx/internal/control"
	"golang.org/x/sys/unix"
)

// initialPPID is this process's parent PID, captured at package
// initialization — as early as a pure-Go program can observe it, before
// main runs and before InstallDeathSignal is called. InstallDeathSignal
// compares it against a fresh unix.Getppid() to detect that the original
// parent already died (the process was reparented) in the window between
// process start and the PR_SET_PDEATHSIG install, which PR_SET_PDEATHSIG
// alone cannot cover: PDEATHSIG only fires on a parent death that happens
// AFTER it is armed.
var initialPPID = os.Getppid()

// InstallDeathSignal enforces the "a plugin never outlives its host"
// invariant on the plugin side. It is meant to be the very first statement
// of PluginServer.Serve, before any other setup.
//
// It arms PR_SET_PDEATHSIG(SIGKILL) as a backstop (the kernel SIGKILLs this
// process if its parent later dies), then immediately re-checks
// unix.Getppid(): if the parent no longer matches initialPPID — the original
// host died and this process was reparented to a subreaper or to init/PID 1 —
// the process is orphaned and must not keep running. In that case it calls
// os.Exit(1) and never returns; otherwise it returns normally. Arming the
// signal is best-effort: a Prctl failure does not by itself orphan the
// process, so it is not treated as fatal — the getppid re-check is the
// authoritative orphan test.
func InstallDeathSignal() {
	// Best-effort backstop; the getppid re-check below is authoritative.
	_ = unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0)

	if orphaned(unix.Getppid(), initialPPID) {
		// Original parent already gone (reparented). PR_SET_PDEATHSIG will
		// never fire for a death that already happened, so exit explicitly.
		os.Exit(1)
	}
}

// orphaned reports whether the process was reparented away from its original
// parent — the host died in the fork→PR_SET_PDEATHSIG-install window, which
// the signal alone cannot cover. A plain getppid mismatch is the entire test:
// deliberately NOT "currentPPID == 1", because a host that legitimately runs
// as PID 1 (a container with no init shim — eqp-hub ships host and plugins in
// one container) has a live parent whose pid IS 1, and initialPPID
// captured it as 1; a "== 1" clause would then wrongly kill every plugin. Real
// reparenting to init is already caught by the mismatch (1 != the host's pid).
func orphaned(currentPPID, originalPPID int) bool {
	return currentPPID != originalPPID
}

// AwaitHostDisconnect drives the plugin's control socket during the serving
// phase and blocks until the host connection ends, returning what ended it:
//
//   - nil — the host sent a graceful Shutdown message. The caller
//     (PluginServer.Serve) is responsible for acknowledging it, running any
//     cleanup, and exiting 0.
//   - a non-nil error — the host process crashed or closed its end (control
//     socket EOF), or ctx was canceled. The caller runs cleanup and exits.
//
// It is the single reader of conn during serving: control.Conn permits only
// one in-flight Recv, so there is exactly one control-message loop, and this
// is it. Currently the only control message the host itself sends after
// handshake is Shutdown (from Host.Stop's teardown) or nothing at all (a
// crash, seen as EOF); the host's HeartbeatAck replies (to the plugin's own
// periodic Heartbeat sends — see pluginserver.go's runHeartbeatSender) also
// arrive here and are likewise ignored — the plugin does not itself need to
// observe the host's acks, only send the heartbeats.
//
// NOTE (deviation from the brief's godoc sketch): the brief described a
// separate "ordinary control-message loop" handling Shutdown while this
// function only detected EOF. control.Conn's one-in-flight-Recv contract
// makes two concurrent control readers illegal, so the two are folded into
// this single loop; the graceful-vs-crash distinction is carried by the
// nil-vs-error return instead of by a separately-canceled ctx.
func AwaitHostDisconnect(ctx context.Context, conn *control.Conn) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		msg, err := conn.Recv(ctx)
		if err != nil {
			return err // EOF / peer close / recv error: host is gone.
		}

		kind, ok := control.KindOf(msg)
		if !ok {
			// An empty datagram is how a closed SOCK_SEQPACKET peer reports
			// EOF: recvmsg returns zero bytes, which unmarshals to a
			// body-less ControlMessage. Treat it as host disconnect.
			return io.EOF
		}

		if kind == control.KindShutdown {
			return nil // graceful shutdown; caller acks + cleans up + exits 0.
		}

		// Any other kind (e.g. a future heartbeat) is currently ignored — keep
		// reading until Shutdown or disconnect.
	}
}
