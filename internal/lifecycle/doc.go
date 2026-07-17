// Package lifecycle implements the process-level primitives of host- and
// plugin-side lifecycle: spawning a plugin child with a sanitized
// environment and the inherited control fd (Spawn), the plugin's
// "never outlive the host" death-signal bootstrap (InstallDeathSignal), host-
// disconnect detection during serving (AwaitHostDisconnect), and the
// normative 6-step teardown state machine (Teardown) whose ordering — stop
// admission, fail in-flight, join goroutines, unmap, terminate-and-reap,
// close fds — is fixed and never reordered.
//
// It deliberately does NOT import the public styx package: styx imports
// lifecycle (Host.Start/Stop and PluginServer.Serve drive these primitives),
// so the reverse would cycle. The handshake message orchestration and the
// translation between internal negotiation types (internal/control.Offer,
// *control.IncompatibleError) and the public stable types
// (styx.HandshakeOffer, *styx.IncompatibleError) therefore live in the styx
// package at the public-API boundary, not here.
package lifecycle
