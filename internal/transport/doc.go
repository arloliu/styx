// Package transport defines the message-oriented transport abstraction used by
// the Styx RPC runtime. It provides the Send/Recv interface over two
// implementations: the Unix-domain socket transport (uds) and the shared-memory
// transport (shm). The uds transport also serves as the correctness oracle for
// differential testing and remains available as a fallback.
package transport
