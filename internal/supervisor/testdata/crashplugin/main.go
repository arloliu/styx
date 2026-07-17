// Command crashplugin is a test fixture for Supervisor's restart-policy
// path: it writes a fixed line to stderr (so crash-reason stderr-tail
// capture has something to observe) and exits immediately with a non-zero
// status, before ever attempting the control handshake. Every restart
// attempt fails identically and deterministically — no timing races with
// a real handshake, unlike a fixture that tried to crash shortly after
// becoming Ready.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "crashplugin: simulated crash")
	os.Exit(1)
}
