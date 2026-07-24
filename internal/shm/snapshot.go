package shm

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// SnapshotFormatVersion is bumped whenever the on-wire snapshot envelope
// (not the caller's own payload bytes) changes shape.
const SnapshotFormatVersion = 1

// MaxSnapshotBytes bounds a snapshot either side of a hot-reload is willing to
// map (1 GiB).
// The declared length arrives from the peer (the untrusted side of this
// boundary), so it is validated against this fixed ceiling before being used as
// a mapping length.
const MaxSnapshotBytes = 1 << 30

// snapshotSeals is the full seal set a hot-reload snapshot memfd must carry
// before it is handed across the wire.
// Unlike the live shared-memory region (wantSeals in region.go), a snapshot
// is immutable by contract: a producer that could still write to, grow, or
// shrink it after handing it over could change the bytes out from under the
// receiver between verification and use.
// Anything short of the complete set is a protocol violation rather than a
// warning.
const snapshotSeals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

// ErrUnsealedSnapshot marks a snapshot memfd whose seal set is missing at
// least one of the required bits.
var ErrUnsealedSnapshot = errors.New("shm: snapshot missing required seal flags")

// ErrSnapshotLengthMismatch marks a snapshot whose declared length
// disagrees with the memfd's real, kernel-reported size.
var ErrSnapshotLengthMismatch = errors.New("shm: snapshot declared length does not match memfd size")

// ErrSnapshotTooLarge marks a snapshot whose size is past the size this
// package is willing to accept, either the caller's own bound (BuildSnapshot)
// or MaxSnapshotBytes (VerifySealedSnapshot).
var ErrSnapshotTooLarge = errors.New("shm: snapshot exceeds the maximum allowed size")

// BuildSnapshot writes payload into a freshly created memfd and seals it fully
// before returning.
// The fd it hands back is already the immutable artifact the wire protocol
// requires — never a writable one a caller might forget to seal.
// maxLen bounds payload; a caller wiring this to the wire protocol passes
// MaxSnapshotBytes unless it has a tighter bound.
// The returned checksum is SHA-256 of payload, which a caller returns to its
// peer as a receipt (e.g. SaveStateAck's checksum) but which the peer never
// compares against — see VerifySealedSnapshot.
//
// fd, declaredLen, and checksum are the three fields the wire's SaveState/
// SaveStateAck pair needs from a build.
// A caller wants all three together.
// Wrapping them in a struct would only relocate the same four return values
// (three data fields plus err) behind an extra allocation.
//
//nolint:revive // function-result-limit: see the paragraph above
func BuildSnapshot(payload []byte, maxLen uint64) (fd int, declaredLen uint64, checksum [32]byte, err error) {
	length := uint64(len(payload))
	if length > maxLen {
		return -1, 0, checksum, fmt.Errorf("shm: BuildSnapshot: payload of %d bytes exceeds the %d byte limit: %w",
			length, maxLen, ErrSnapshotTooLarge)
	}

	fd, err = unix.MemfdCreate("styx-snapshot", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return -1, 0, checksum, fmt.Errorf("shm: BuildSnapshot: memfd_create: %w", err)
	}

	// length <= maxLen, and every real caller's maxLen stays well below the
	// 63-bit range ftruncate/mmap accept on 64-bit targets.
	//nolint:gosec // bounded above by the maxLen check just performed
	if err := unix.Ftruncate(fd, int64(length)); err != nil {
		_ = unix.Close(fd)

		return -1, 0, checksum, fmt.Errorf("shm: BuildSnapshot: ftruncate: %w", err)
	}

	if length > 0 {
		//nolint:gosec // bounded above by the maxLen check just performed
		data, mmapErr := unix.Mmap(fd, 0, int(length), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if mmapErr != nil {
			_ = unix.Close(fd)

			return -1, 0, checksum, fmt.Errorf("shm: BuildSnapshot: mmap: %w", mmapErr)
		}

		copy(data, payload)

		if munmapErr := unix.Munmap(data); munmapErr != nil {
			_ = unix.Close(fd)

			return -1, 0, checksum, fmt.Errorf("shm: BuildSnapshot: munmap: %w", munmapErr)
		}
	}

	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, snapshotSeals); err != nil {
		_ = unix.Close(fd)

		return -1, 0, checksum, fmt.Errorf("shm: BuildSnapshot: fcntl(F_ADD_SEALS): %w", err)
	}

	return fd, length, sha256.Sum256(payload), nil
}

// VerifySealedSnapshot independently checks everything a receiver must not
// take on trust from the producer: complete seal set, declared length against
// the memfd's real size, and size within MaxSnapshotBytes.
// Then it maps the memfd read-only and returns the live mapping with its
// self-computed SHA-256.
// It never trusts declaredLen as a mapping length: the mapping is sized from
// fstat (which the kernel reports and the seals have frozen).
// It never repairs a rejected snapshot in place and never panics.
//
// Nothing on the wire carries a checksum to compare against — SaveStateAck's
// checksum travels host->plugin as a receipt, never as an expected value.
// This function only ever computes a checksum; it never checks one.
//
// The caller owns the returned mapping and MUST unix.Munmap(data) once done,
// except when the snapshot is empty (len(data) == 0): nothing was mapped in
// that case, and Munmap on an empty slice fails.
func VerifySealedSnapshot(fd int, declaredLen uint64) (data []byte, checksum [32]byte, err error) {
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: fcntl(F_GET_SEALS): %w", err)
	}
	if seals&snapshotSeals != snapshotSeals {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: seal set %#x missing %#x: %w",
			seals, snapshotSeals&^seals, ErrUnsealedSnapshot)
	}

	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: fstat: %w", err)
	}
	if st.Size < 0 {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: negative fstat size %d: %w",
			st.Size, ErrSnapshotLengthMismatch)
	}

	//nolint:gosec // st.Size was just checked non-negative above
	actual := uint64(st.Size)
	if actual != declaredLen {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: declared %d bytes but memfd is %d: %w",
			declaredLen, actual, ErrSnapshotLengthMismatch)
	}
	if actual > MaxSnapshotBytes {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: %d bytes exceeds the %d byte limit: %w",
			actual, uint64(MaxSnapshotBytes), ErrSnapshotTooLarge)
	}
	if actual == 0 {
		return []byte{}, sha256.Sum256(nil), nil
	}

	//nolint:gosec // bounded above by MaxSnapshotBytes, checked above
	data, err = unix.Mmap(fd, 0, int(actual), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, checksum, fmt.Errorf("shm: VerifySealedSnapshot: mmap: %w", err)
	}

	return data, sha256.Sum256(data), nil
}
