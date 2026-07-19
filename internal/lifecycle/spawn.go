package lifecycle

import (
	"fmt"
	"os"
	"os/exec"

	"golang.org/x/sys/unix"
)

// baseEnvKeys are the only host environment variables that pass through to a
// spawned plugin's sanitized base environment. Everything else from the
// host's environment is dropped; spec.Env is merged on top of this base.
var baseEnvKeys = []string{"PATH", "HOME", "TZ"}

// Spec declares one plugin process to spawn: the binary path, args, and
// additional env vars merged onto the sanitized base environment.
type Spec struct {
	Path string
	Args []string
	Env  []string
	// CaptureStdio requests that Spawn wire the child's stdout/stderr to
	// pipes this process owns the read end of (Process.Stdout/Stderr),
	// instead of the previous default of /dev/null. False (the
	// zero value) reproduces that exact prior behavior for every existing
	// caller; internal/supervisor is the only caller that sets it true, so
	// it can drain the child's stdout/stderr into bounded buffers.
	CaptureStdio bool
}

// Process is a live handle to a spawned plugin: its PID, the host-side
// control fd (SOCK_SEQPACKET, wrapped by internal/control.Conn), and the
// os.Process for signaling/waiting.
type Process struct {
	PID       int
	ControlFD int
	// Stdout and Stderr are the host-owned read ends of the child's
	// stdout/stderr pipes, non-nil only when Spec.CaptureStdio was true.
	// The caller is responsible for draining and eventually closing them
	// (Kill closes them itself for the startup-abort path; a caller using
	// the full internal/lifecycle.Teardown machine closes them from its
	// own CloseFDs step, alongside the control fd).
	Stdout    *os.File
	Stderr    *os.File
	osProcess *os.Process
}

// Spawn starts spec.Path with a sanitized environment and the child end of a
// freshly created AF_UNIX/SOCK_SEQPACKET control socketpair, inherited as fd
// 3. The host-side end is returned as Process.ControlFD (CLOEXEC, so it does
// not leak into any later spawn). Every other fd the host holds is CLOEXEC
// already (Go's os/exec sets CLOEXEC for fds not explicitly listed in
// ExtraFiles, and this package's own fd discipline guarantees it for
// received fds).
//
// SysProcAttr.Pdeathsig is set to SIGKILL as defense-in-depth (it is not
// sufficient alone — see InstallDeathSignal — because the window between
// fork and the child's own prctl call is real). SysProcAttr.Setpgid puts the
// child in its own process group so teardown can SIGKILL the whole group,
// reaching any grandchildren the plugin itself forked (the go-plugin fork
// learned this the hard way in commit 8bf442e). See teardown's signalGroup.
func Spawn(spec Spec) (*Process, error) {
	// SOCK_CLOEXEC on both ends: the host end must not leak into a later
	// spawn, and os/exec clears CLOEXEC on the child's inherited copy when it
	// dup2s the ExtraFile into place, so the child still receives an
	// inheritable fd 3.
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: spawn: socketpair: %w", err)
	}
	hostFD, childFD := fds[0], fds[1]

	childFile := os.NewFile(uintptr(childFD), "styx-control-child")

	// spec.Path/Args are the host's own plugin configuration, not externally
	// supplied input — spawning the configured plugin binary is Spawn's job.
	// Deliberately not exec.CommandContext: the child's lifetime is owned by
	// internal/lifecycle.Teardown's graceful sequence (StopAdmission ->
	// FailInFlight -> JoinGoroutines -> ShutdownDeadline -> SIGKILL
	// fallback), not by a context — binding it to a ctx would let a ctx
	// cancellation hard-kill the child, bypassing that teardown entirely.
	//nolint:gosec,noctx // see comment above
	cmd := exec.Command(spec.Path, spec.Args...)
	cmd.Env = sanitizedEnv(spec.Env)
	cmd.ExtraFiles = []*os.File{childFile} // becomes fd 3 in the child
	cmd.SysProcAttr = &unix.SysProcAttr{
		Pdeathsig: unix.SIGKILL,
		Setpgid:   true,
	}

	var stdoutR, stdoutW, stderrR, stderrW *os.File
	if spec.CaptureStdio {
		var err error
		stdoutR, stdoutW, err = os.Pipe()
		if err != nil {
			_ = childFile.Close()
			_ = unix.Close(hostFD)

			return nil, fmt.Errorf("lifecycle: spawn: stdout pipe: %w", err)
		}
		stderrR, stderrW, err = os.Pipe()
		if err != nil {
			_ = stdoutR.Close()
			_ = stdoutW.Close()
			_ = childFile.Close()
			_ = unix.Close(hostFD)

			return nil, fmt.Errorf("lifecycle: spawn: stderr pipe: %w", err)
		}
		cmd.Stdout = stdoutW
		cmd.Stderr = stderrW
	}

	if err := cmd.Start(); err != nil {
		_ = childFile.Close()
		_ = unix.Close(hostFD)
		closeIfNonNil(stdoutR, stdoutW, stderrR, stderrW)

		return nil, fmt.Errorf("lifecycle: spawn %q: %w", spec.Path, err)
	}

	// The child holds its own dup of the control socket now; the host keeps
	// only hostFD. Close our copy of the child end so the host's side sees
	// EOF if the child dies.
	_ = childFile.Close()

	// Same reasoning for the pipe write ends: the child holds its own dup
	// (dup2'd into place by os/exec since cmd.Stdout/Stderr are *os.File),
	// so the host's copy of the write end must close for the host's read
	// end to see EOF once the child exits.
	if spec.CaptureStdio {
		_ = stdoutW.Close()
		_ = stderrW.Close()
	}

	return &Process{
		PID:       cmd.Process.Pid,
		ControlFD: hostFD,
		Stdout:    stdoutR,
		Stderr:    stderrR,
		osProcess: cmd.Process,
	}, nil
}

// closeIfNonNil closes every non-nil file in files, ignoring individual
// close errors — used on Spawn's abort path where the files are being
// discarded anyway.
func closeIfNonNil(files ...*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

// sanitizedEnv builds the child's environment: the whitelisted base keys
// taken from the host's current environment, then extra appended on top
// (extra wins on a duplicate key, since exec uses the last value).
func sanitizedEnv(extra []string) []string {
	env := make([]string, 0, len(baseEnvKeys)+len(extra))
	for _, k := range baseEnvKeys {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}

	return append(env, extra...)
}

// Wait blocks until the process exits and reaps it (waitpid), returning its
// exit state. It may be called at most once per Process (os.Process.Wait's
// contract). Teardown's step 5 and the startup-abort path both funnel their
// reap through here so the reap always happens exactly once.
func (p *Process) Wait() (*os.ProcessState, error) {
	return p.osProcess.Wait()
}

// Kill force-terminates the process group and reaps the child (waitpid),
// returning the reaped *os.ProcessState so the caller can recover a real exit
// status. It is the startup-abort reap: when handshake fails before a
// ClientConn is wired there is no Teardown to run, but a spawned child must
// still never be left as a zombie. Teardown.Run's step 5 is the normal-path
// reap (it keeps its own *os.ProcessState on Teardown.Reaped); this is its
// abort-path analogue and, like it, always ends in a waitpid, now surfacing
// the same kind of state instead of discarding it. It also closes
// Stdout/Stderr if Spec.CaptureStdio was set (a no-op, since they are nil,
// for every caller that left it false).
func (p *Process) Kill() (*os.ProcessState, error) {
	_ = p.signal(unix.SIGKILL)
	state, err := p.Wait()
	closeIfNonNil(p.Stdout, p.Stderr)

	return state, err
}

// signal sends sig to the process's whole process group (Spawn set Setpgid,
// so the group id equals the child PID). Group-directed signaling reaches
// any subprocess the plugin itself forked into the same group. If the
// group-directed kill fails (e.g. the group is already gone), it falls back
// to signaling the single PID.
func (p *Process) signal(sig unix.Signal) error {
	if err := unix.Kill(-p.PID, sig); err != nil {
		return unix.Kill(p.PID, sig)
	}

	return nil
}
