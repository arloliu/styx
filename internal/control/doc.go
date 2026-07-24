// Package control implements the Styx control-plane protocol.
// It carries connection handshake, file descriptor passing, liveness signals, drain
// coordination, and shutdown via framed protobuf messages over a SOCK_SEQPACKET socketpair.
package control
