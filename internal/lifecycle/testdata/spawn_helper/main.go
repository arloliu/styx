// Command spawn_helper is a test fixture for lifecycle.Spawn. It inspects
// the environment Spawn handed it — the inherited control fd (fd 3), the
// PR_SET_PDEATHSIG armed by SysProcAttr, and the sanitized env — and writes
// what it found as key=value lines to the file named by $STYX_REPORT (so
// the parent can read the result without depending on how Spawn wires the
// child's stdio), then exits 0.
package main

import (
	"fmt"
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// controlFD is the first (and, for now, only) intentionally inherited
// bootstrap fd: the child end of the control socketpair, per
// lifecycle.Spawn.
const controlFD = 3

func main() {
	var b strings.Builder
	fmt.Fprintf(&b, "fd3_type=%d\n", fdSockType(controlFD))
	fmt.Fprintf(&b, "pdeathsig=%d\n", getPdeathsig())
	fmt.Fprintf(&b, "leak_canary=%s\n", os.Getenv("STYX_LEAK_CANARY"))
	fmt.Fprintf(&b, "path_present=%t\n", os.Getenv("PATH") != "")
	fmt.Fprintf(&b, "extra=%s\n", os.Getenv("STYX_EXTRA"))

	report := os.Getenv("STYX_REPORT")
	if report == "" {
		fmt.Fprint(os.Stdout, b.String())
		return
	}
	if err := os.WriteFile(report, []byte(b.String()), 0o600); err != nil {
		os.Exit(2)
	}
}

// fdSockType returns fd's SO_TYPE (e.g. unix.SOCK_SEQPACKET), or -1 if fd is
// not a socket / not open.
func fdSockType(fd int) int {
	t, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil {
		return -1
	}

	return t
}

// getPdeathsig returns the current PR_SET_PDEATHSIG signal number (0 if none
// is armed), read back via PR_GET_PDEATHSIG.
func getPdeathsig() int {
	var sig int
	if err := unix.Prctl(unix.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&sig)), 0, 0, 0); err != nil {
		return -1
	}

	return sig
}
