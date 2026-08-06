//go:build failpoint

package styx

// SetPluginAttachSHMFailAt installs a test hook that fires after each named
// construction step of plugin-side shared-memory attach: recv-fds, hp-wrap,
// ph-wrap, burst-wrap (only reached when the burst path is active), attach,
// ack-send (each before the ack reaches the host), and post-ack (after the ack
// is sent). A non-nil return aborts the attach at that step.
//
// This is available only under -tags failpoint so a test can model a plugin
// process dying mid-attach, verifying the host's spawn/attach-failure
// classification and its fd/region cleanup. The default build compiles this
// setter out; the seam stays nil, a cold-path no-op on the attach path.
func SetPluginAttachSHMFailAt(f func(step string) error) {
	pluginAttachSHMFailAt = f
}
