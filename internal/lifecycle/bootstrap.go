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
//
// The os.Exit below IS the safety contract this function exists to provide
// — a caller that forgot to check a returned bool would reintroduce the
// exact "orphaned plugin keeps running" failure this guards against.
// Covered end to end by
// TestInstallDeathSignal_ExitsChild_WhenOriginalParentDiesBeforeInstall via
// testdata/deathsig_helper, a real spawned child process.
//
//nolint:revive // deep-exit: see doc above
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
// as PID 1 (a container with no init shim — some deployments ship host and
// plugins in one container) has a live parent whose pid IS 1, and initialPPID
// captured it as 1; a "== 1" clause would then wrongly kill every plugin. Real
// reparenting to init is already caught by the mismatch (1 != the host's pid).
func orphaned(currentPPID, originalPPID int) bool {
	return currentPPID != originalPPID
}
