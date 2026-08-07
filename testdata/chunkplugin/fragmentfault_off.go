//go:build !failpoint

package main

// installFragmentFault arms nothing in a normal build: the fragment-boundary
// seam it uses exists only under -tags failpoint, so the fixture compiles and
// behaves identically without the tag. See fragmentfault.go for the tagged
// implementation and the environment variable it reads.
func installFragmentFault() {}
