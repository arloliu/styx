package styx

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// freshIdentityID returns an ID that no earlier execution in this test binary
// has registered. The registry is process-global and a collision poisons an
// ID permanently and deliberately, so a test that poisons a literal ID would
// start its second execution under -count=2 already poisoned, failing before
// it reached the behavior it means to check.
var identityIDSeq atomic.Uint64

func freshIdentityID(t *testing.T) uint64 {
	t.Helper()

	return fnv64a(fmt.Sprintf("%s#%d", t.Name(), identityIDSeq.Add(1)))
}

// Test identityForID returning the name RegisterIdentityName recorded for an
// ID, proving the registry is actually consulted rather than the function
// always falling through to hexIdentity
func TestIdentityForID_ReturnsRegisteredName_WhenIDIsRegistered(t *testing.T) {
	// Given
	id := fnv64a("idregistry.Registered")
	RegisterIdentityName(id, "idregistry.Registered")

	// When
	got := identityForID(id)

	// Then: the registered name comes back, not the hex hash a test that left
	// the registry empty would still pass with.
	require.Equal(t, "idregistry.Registered", got)
	require.NotEqual(t, hexIdentity(id), got)
}

// Test identityForID falling back to hexIdentity for an ID that was never
// registered — the path a hand-called precomputed-ID call, the difftest
// harness, or a plugin built with an older generator all still take
func TestIdentityForID_FallsBackToHex_WhenIDIsUnregistered(t *testing.T) {
	// Given: an ID with no corresponding RegisterIdentityName call anywhere
	// in this process.
	id := fnv64a("idregistry.NeverRegistered")

	// When
	got := identityForID(id)

	// Then: the hex rendering, never empty and never the literal "." bug
	// this whole identity feature exists to remove.
	require.Equal(t, hexIdentity(id), got)
	require.NotEmpty(t, got)
}

// Test RegisterIdentityName treating a repeated identical (id, name) pair as
// a no-op, not an error — the case that legitimately arrives whenever two
// generated packages describe the same service and both run their init
func TestRegisterIdentityName_IsNoOp_OnDuplicateRegistration(t *testing.T) {
	// Given
	id := fnv64a("idregistry.Duplicated")
	RegisterIdentityName(id, "idregistry.Duplicated")

	// When: the identical pair registers again, as it would from a second
	// generated package's init function.
	require.NotPanics(t, func() {
		RegisterIdentityName(id, "idregistry.Duplicated")
		RegisterIdentityName(id, "idregistry.Duplicated")
	})

	// Then: the name still resolves — the duplicate registration neither
	// poisoned the entry nor changed it.
	require.Equal(t, "idregistry.Duplicated", identityForID(id))
}

// Test RegisterIdentityName falling back to the hex rendering once an ID is
// claimed by two DIFFERENT names — a genuine FNV-1a-64 collision. Neither
// claimant is trustworthy once that happens, so the registry must not keep
// serving whichever name registered first.
func TestRegisterIdentityName_FallsBackToHex_OnConflictingNames(t *testing.T) {
	// Given: an ID first registered under one name.
	id := freshIdentityID(t)
	RegisterIdentityName(id, "first.Name")
	require.Equal(t, "first.Name", identityForID(id), "sanity: first registration must resolve before the collision")

	// When: the SAME id is registered again under a different name.
	require.NotPanics(t, func() {
		RegisterIdentityName(id, "second.Name")
	})

	// Then: the id is no longer trusted at all — not the first name, not the
	// second, just the hex rendering hexIdentity would produce for an
	// unregistered id.
	got := identityForID(id)
	require.Equal(t, hexIdentity(id), got)
	require.NotEqual(t, "first.Name", got)
	require.NotEqual(t, "second.Name", got)

	// And: a THIRD registration under the original name does not un-poison
	// the id — the collision is permanent, not resolved by whichever name
	// happens to register most recently.
	RegisterIdentityName(id, "first.Name")
	require.Equal(t, hexIdentity(id), identityForID(id))
}
