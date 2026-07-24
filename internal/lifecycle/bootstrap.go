package lifecycle

import (
	"os"

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

// InstallDeathSignal enforces that a plugin process does not outlive its host.
// It should be called at the start of PluginServer.Serve, before any other setup.
//
// It arms PR_SET_PDEATHSIG(SIGKILL) to ensure the kernel kills this process if
// its parent dies. It then immediately re-checks unix.Getppid(): if the parent
// no longer matches the initial parent captured at package initialization, the
// original host died and this process was reparented to a subreaper or init/PID 1,
// meaning it is orphaned. The process exits with code 1 in that case.
// Otherwise it returns normally. Arming the signal is best-effort: a Prctl failure
// does not by itself make the process orphaned, so it is not treated as fatal —
// the getppid re-check is the authoritative orphan test.
// This function unconditionally calls os.Exit(1) on orphan detection, enforcing
// the invariant that an orphaned plugin never continues running.
//
//nolint:revive // deep-exit: enforces the orphan-detection invariant
func InstallDeathSignal() {
	// Best-effort backstop; the getppid re-check below is authoritative.
	_ = unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0)

	if orphaned(unix.Getppid(), initialPPID) {
		// Original parent already gone (reparented). PR_SET_PDEATHSIG will
		// never fire for a death that already happened, so exit explicitly.
		os.Exit(1)
	}
}

// orphaned reports whether the process was reparented away from its original parent.
// This detects the fork to PR_SET_PDEATHSIG install window, which the signal alone
// cannot cover. The test is a plain getppid mismatch: deliberately not "currentPPID == 1",
// because a host running as PID 1 (e.g. in a container with no init shim) has a live
// parent whose PID is 1, and this would have captured that as the original.
// A "== 1" clause would then wrongly kill every plugin, whereas real reparenting to
// init is already caught by the mismatch (1 != the original host's PID).
func orphaned(currentPPID, originalPPID int) bool {
	return currentPPID != originalPPID
}
