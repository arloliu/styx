// Command deathsig_helper is a test fixture for
// lifecycle.InstallDeathSignal's getppid reparent-detection. It coordinates
// with its intermediary parent so the test is deterministic rather than
// racing the fork→install window:
//
//  1. lifecycle.initialPPID is captured at package init while the
//     intermediary parent is still alive (the intermediary blocks until this
//     helper signals ready), so it captures the REAL parent pid, not a
//     post-reparent value.
//  2. The helper writes $STYX_READY_FILE to release the intermediary, which
//     then exits and reparents this process.
//  3. The helper waits until getppid actually changes (the reparent
//     completed), then calls InstallDeathSignal, which must os.Exit(1)
//     because getppid no longer matches initialPPID.
//
// If InstallDeathSignal fails to detect the reparent it falls through to
// print "alive" and block — which the test detects as a survived orphan.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/arloliu/styx/internal/lifecycle"
)

func main() {
	// Captured while the intermediary parent is still alive (it waits for our
	// ready signal below), so this is the real parent pid — matching
	// lifecycle.initialPPID captured at package init.
	originalPPID := os.Getppid()

	// Release the intermediary; it exits and we get reparented.
	if ready := os.Getenv("STYX_READY_FILE"); ready != "" {
		_ = os.WriteFile(ready, []byte("ok"), 0o600)
	}

	// Wait for the reparent to actually happen so InstallDeathSignal runs
	// against a genuinely orphaned state (bounded, so a stuck test still ends).
	deadline := time.Now().Add(2 * time.Second)
	for os.Getppid() == originalPPID && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	lifecycle.InstallDeathSignal()

	// Reached only if InstallDeathSignal wrongly continued past a reparent.
	fmt.Fprintln(os.Stdout, "alive")

	select {}
}
