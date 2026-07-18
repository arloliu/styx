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
