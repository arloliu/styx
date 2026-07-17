// Package transport defines the transport interface and its uds and shm
// implementations. uds serves as the fallback data-plane transport, a
// differential-testing oracle, and the vehicle for building the framework
// before the shared-memory transport exists.
package transport
