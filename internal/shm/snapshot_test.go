package shm_test

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/shm"
)

// Test BuildSnapshot producing a fully sealed memfd for a valid payload
func TestBuildSnapshot_ProduceFullySealedMemfd_ForValidPayload(t *testing.T) {
	// Given
	payload := []byte(`{"counter":42}`)

	// When
	fd, declaredLen, checksum, err := shm.BuildSnapshot(payload, 1<<20)

	// Then
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	require.NoError(t, err)
	require.Equal(t, unix.F_SEAL_SEAL|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_WRITE, seals)
	require.EqualValues(t, len(payload), declaredLen)
	want := sha256.Sum256(payload)
	require.Equal(t, want, checksum)
}

// Test BuildSnapshot rejecting a payload larger than the caller's own bound
func TestBuildSnapshot_Reject_WhenPayloadExceedsCallerBound(t *testing.T) {
	// Given
	payload := []byte("this payload is definitely bigger than the tiny bound below")

	// When
	_, _, _, err := shm.BuildSnapshot(payload, 4)

	// Then
	require.ErrorIs(t, err, shm.ErrSnapshotTooLarge)
}

// Test VerifySealedSnapshot round-tripping a snapshot BuildSnapshot produced,
// returning the same bytes and the same self-computed checksum
func TestVerifySealedSnapshot_ReturnDataAndChecksum_ForValidSnapshot(t *testing.T) {
	// Given
	payload := []byte("device gateway session state")
	fd, declaredLen, wantChecksum, err := shm.BuildSnapshot(payload, 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	// When
	data, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)

	// Then
	require.NoError(t, err)
	require.Equal(t, payload, data)
	require.Equal(t, wantChecksum, checksum)
	require.NoError(t, unix.Munmap(data))
}

// Test VerifySealedSnapshot rejecting a memfd as a protocol violation when the seal set is incomplete
func TestVerifySealedSnapshot_RejectAsProtocolViolation_WhenSealSetIncomplete(t *testing.T) {
	// Given: a memfd sealed with F_SEAL_SHRINK|F_SEAL_GROW only (write NOT sealed —
	// a snapshot the producer could still mutate is itself the violation)
	fd := newPartiallySealedMemfd(t, []byte("data"), unix.F_SEAL_WRITE)

	// When
	_, _, err := shm.VerifySealedSnapshot(fd, 4)

	// Then
	require.ErrorIs(t, err, shm.ErrUnsealedSnapshot)
}

// Test VerifySealedSnapshot rejecting a fully sealed memfd whose declared length disagrees with its real size
func TestVerifySealedSnapshot_RejectAsLengthMismatch_WhenDeclaredLengthDisagreesWithRealSize(t *testing.T) {
	// Given
	fd, declaredLen, _, err := shm.BuildSnapshot([]byte("real data"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	// When
	_, _, err = shm.VerifySealedSnapshot(fd, declaredLen+1)

	// Then
	require.ErrorIs(t, err, shm.ErrSnapshotLengthMismatch)
}

// Test VerifySealedSnapshot rejecting an internally-consistent but oversized snapshot
func TestVerifySealedSnapshot_RejectAsTooLarge_WhenSizeExceedsCeiling(t *testing.T) {
	// Given: a sparse fd one byte past the package's size ceiling — a memfd is
	// tmpfs-backed, so growing it with Ftruncate alone leaves the range
	// unwritten, avoiding an actual gigabyte-sized write just to trip this check.
	size := int64(shm.MaxSnapshotBytes) + 1
	fd := sparseSealedMemfd(t, size)

	// When
	_, _, err := shm.VerifySealedSnapshot(fd, uint64(size))

	// Then
	require.ErrorIs(t, err, shm.ErrSnapshotTooLarge)
}

// newPartiallySealedMemfd creates a memfd holding payload, sealed with the
// full snapshot seal set minus omitSeal, so the returned fd is deliberately
// under-sealed for a rejection test.
func newPartiallySealedMemfd(t *testing.T, payload []byte, omitSeal int) int {
	t.Helper()

	fd, err := unix.MemfdCreate("styx-test-snapshot", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	if len(payload) > 0 {
		_, err = unix.Write(fd, payload)
		require.NoError(t, err)
	}

	seals := (unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL) &^ omitSeal
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals)
	require.NoError(t, err)

	return fd
}

// sparseSealedMemfd creates a fully sealed memfd of size bytes without
// writing any of them.
func sparseSealedMemfd(t *testing.T, size int64) int {
	t.Helper()

	fd, err := unix.MemfdCreate("styx-test-snapshot-oversized", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	require.NoError(t, unix.Ftruncate(fd, size))

	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals)
	require.NoError(t, err)

	return fd
}
