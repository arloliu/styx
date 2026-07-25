// Package goplugin compares the arloliu/go-plugin fork against Styx's own
// shm and uds transports, driven through Styx's public API. It lives in its
// own Go module (see go.mod) so go-plugin's dependency graph never touches
// the root styx module -- the root module's benchmark suites keep comparing
// Styx against gRPC/net-rpc/raw-UDS baselines; this module is the go-plugin
// comparison's home. Output is advisory, like bench/rpc; it is not part of
// the bench-compare regression gate.
package goplugin
