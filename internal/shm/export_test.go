package shm

// WriteLayoutPageForTest re-exports writeLayoutPage so shm_test (black-box
// tests) can hand-build a layout page CreateRegion's own input validation
// would never let a caller construct — needed to exercise OpenRegion's
// Phase 2 attach-time defenses against a corrupt or malicious peer.
// Production code never calls this; only *_test.go files do.
func WriteLayoutPageForTest(data []byte, l Layout) { writeLayoutPage(data, l) }

// DataForTest exposes r's raw mapped bytes so shm_test can inspect actual
// memory protection (e.g. via /proc/self/maps) around a call to
// RemapLayoutReadOnly — behavior no accessor on the public API needs to
// expose, since callers only ever consume the cached, decoded Layout.
func (r *Region) DataForTest() []byte { return r.data }

// NewRegionForTest constructs a Region directly from a caller-supplied fd
// and data slice, bypassing CreateRegion/OpenRegion's validation entirely.
// Needed only to exercise Close's fd-not-leaked-when-Munmap-fails path: a
// state the public API can never produce, since every real Region's data
// always comes from a successful, page-aligned mmap of a nonzero,
// PageSize-multiple length, which unix.Munmap never rejects. Production
// code never calls this; only *_test.go files do.
func NewRegionForTest(fd int, data []byte) *Region { return &Region{fd: fd, data: data} }
