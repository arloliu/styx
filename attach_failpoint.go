//go:build failpoint

package styx

// SetPluginAttachSHMFailAt installs a per-step hook fired after each named
// construction step of the plugin-side shared-memory attach (pluginAttachSHM):
// recv-fds, hp-wrap, ph-wrap, attach, and ack-send. A non-nil return aborts the
// attach at that step.
//
// It exists only under -tags failpoint so a crash-window plugin fixture can install
// a hook that os.Exit()s at a chosen step — modeling a plugin process dying
// mid-attach across a real process boundary, so the host's spawn/attach-failure
// classification and its fd/region cleanup can be asserted. The default build
// compiles this setter out; the seam it drives stays nil there, a cold-path no-op on
// the attach path.
func SetPluginAttachSHMFailAt(f func(step string) error) {
	pluginAttachSHMFailAt = f
}
