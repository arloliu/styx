package lifecycle

import (
	"crypto/sha256"
	"testing"

	"github.com/arloliu/styx/internal/shm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Test verifySnapshot accepting a fully sealed snapshot and computing its own checksum
func TestVerifySnapshot_Accept_ForFullySealedPayload(t *testing.T) {
	// Given
	payload := []byte("device gateway session state")
	fd, declaredLen, _, err := shm.BuildSnapshot(payload, shm.MaxSnapshotBytes)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	// When
	snap, err := verifySnapshot(fd, declaredLen, 1)

	// Then
	require.NoError(t, err)
	want := sha256.Sum256(payload)
	require.Equal(t, want[:], snap.checksum)
}

// Test verifySnapshot wrapping both the shm sentinel and lifecycle's own
// ErrSnapshotRejected on an under-sealed snapshot, so a caller can match
// either without the two verifiers (host and successor) ever disagreeing on
// what a rejection means.
func TestVerifySnapshot_WrapBothShmAndLifecycleSentinels_OnUnsealedSnapshot(t *testing.T) {
	// Given: a memfd missing F_SEAL_WRITE, the same rejection shm.VerifySealedSnapshot enforces
	fd, err := unix.MemfdCreate("styx-test-under-sealed", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	payload := []byte("data")
	_, err = unix.Write(fd, payload)
	require.NoError(t, err)

	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	require.NoError(t, err)

	// When
	_, err = verifySnapshot(fd, uint64(len(payload)), 1)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, shm.ErrUnsealedSnapshot)
	require.ErrorIs(t, err, ErrSnapshotRejected)
}

// Test verifySnapshot wrapping both sentinels on a declared length that
// disagrees with the memfd's real size.
func TestVerifySnapshot_WrapBothShmAndLifecycleSentinels_OnLengthMismatch(t *testing.T) {
	// Given
	fd, declaredLen, _, err := shm.BuildSnapshot([]byte("real data"), shm.MaxSnapshotBytes)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	// When
	_, err = verifySnapshot(fd, declaredLen+1, 1)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, shm.ErrSnapshotLengthMismatch)
	require.ErrorIs(t, err, ErrSnapshotRejected)
}
