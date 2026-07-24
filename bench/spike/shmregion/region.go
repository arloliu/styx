package shmregion

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ErrBadMagic is returned by Attach when the region's layout page does not
// carry the spike magic value.
var ErrBadMagic = errors.New("shmregion: bad layout magic")

// Region is a memfd-backed shared-memory region mapped into this process.
type Region struct {
	fd   int
	data []byte // len == RegionSize
}

// Create allocates, seals, writes the layout page, and maps a fresh region.
// Called on the host side before the plugin is spawned.
func Create() (*Region, error) {
	fd, err := unix.MemfdCreate("styx-spike-region", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}
	if err := unix.Ftruncate(fd, RegionSize); err != nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("ftruncate: %w", err)
	}
	data, err := unix.Mmap(fd, 0, RegionSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("mmap: %w", err)
	}
	binary.LittleEndian.PutUint64(data[LayoutPageOffset:], layoutMagic)

	seals := unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
		_ = unix.Munmap(data)
		_ = unix.Close(fd)

		return nil, fmt.Errorf("fcntl(F_ADD_SEALS): %w", err)
	}

	return &Region{fd: fd, data: data}, nil
}

// Attach maps an already-created, already-sealed region by fd (the plugin
// side, after receiving fd via SCM_RIGHTS) and validates the layout magic.
//
// Attach duplicates fd and the returned Region owns only that duplicate;
// the caller retains ownership of the fd it passed in and is responsible
// for closing it. The duplicate is created with F_DUPFD_CLOEXEC so it
// inherits close-on-exec protection (plain dup(2) always clears
// FD_CLOEXEC on the new descriptor, which would otherwise silently undo
// the MFD_CLOEXEC that Create establishes and leak the region fd into
// exec'd children).
//
// Duplicating also means the returned Region owns an independent
// descriptor: in production the plugin process already has its own fd
// number (assigned by the kernel on SCM_RIGHTS receipt), but a caller
// attaching within the same process — e.g. tests — would otherwise share
// the host's fd number and double-close it when both Regions are closed.
func Attach(fd int) (*Region, error) {
	ownFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("fcntl(F_DUPFD_CLOEXEC): %w", err)
	}
	data, err := unix.Mmap(ownFD, 0, RegionSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Close(ownFD)

		return nil, fmt.Errorf("mmap: %w", err)
	}
	if got := binary.LittleEndian.Uint64(data[LayoutPageOffset:]); got != layoutMagic {
		_ = unix.Munmap(data)
		_ = unix.Close(ownFD)

		return nil, ErrBadMagic
	}

	return &Region{fd: ownFD, data: data}, nil
}

// FD returns the region's memfd, for passing over SCM_RIGHTS.
func (r *Region) FD() int { return r.fd }

// Close unmaps the region and closes the local fd.
// Safe to call once.
func (r *Region) Close() error {
	if err := unix.Munmap(r.data); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}

	return unix.Close(r.fd)
}

// TailHP returns a pointer to the host->plugin ring tail word for seq_cst atomics.
func (r *Region) TailHP() *uint64 { return r.wordU64(syncTailHPOffset) }

// HeadHP returns a pointer to the host->plugin ring head word for seq_cst atomics.
func (r *Region) HeadHP() *uint64 { return r.wordU64(syncHeadHPOffset) }

// TailPH returns a pointer to the plugin->host ring tail word for seq_cst atomics.
func (r *Region) TailPH() *uint64 { return r.wordU64(syncTailPHOffset) }

// HeadPH returns a pointer to the plugin->host ring head word for seq_cst atomics.
func (r *Region) HeadPH() *uint64 { return r.wordU64(syncHeadPHOffset) }

// ParkStateHP returns a pointer to the host->plugin direction's park-state word.
func (r *Region) ParkStateHP() *uint32 { return r.wordU32(syncParkStateHPOffset) }

// ParkStatePH returns a pointer to the plugin->host direction's park-state word.
func (r *Region) ParkStatePH() *uint32 { return r.wordU32(syncParkStatePHOffset) }

// Poison returns a pointer to the poison word in the sync page.
func (r *Region) Poison() *uint32 { return r.wordU32(syncPoisonOffset) }

// Generation returns a pointer to the generation word in the sync page.
func (r *Region) Generation() *uint32 { return r.wordU32(syncGenerationOffset) }

// RingHPBytes returns a byte slice over the host->plugin ring descriptor array.
func (r *Region) RingHPBytes() []byte { return r.data[RingHPOffset : RingHPOffset+RingBytesHP] }

// RingPHBytes returns a byte slice over the plugin->host ring descriptor array.
func (r *Region) RingPHBytes() []byte { return r.data[RingPHOffset : RingPHOffset+RingBytesPH] }

// ArenaHPBytes returns a byte slice over the host->plugin arena.
func (r *Region) ArenaHPBytes() []byte {
	return r.data[ArenaHPOffset : ArenaHPOffset+ArenaBytesPerDirection]
}

// ArenaPHBytes returns a byte slice over the plugin->host arena.
func (r *Region) ArenaPHBytes() []byte {
	return r.data[ArenaPHOffset : ArenaPHOffset+ArenaBytesPerDirection]
}

func (r *Region) wordU64(offset int) *uint64 {
	return (*uint64)(unsafe.Pointer(&r.data[offset])) //nolint:gosec // offsets are fixed, page-aligned, 8-byte aligned
}

func (r *Region) wordU32(offset int) *uint32 {
	return (*uint32)(unsafe.Pointer(&r.data[offset])) //nolint:gosec // offsets are fixed, page-aligned, 4-byte aligned
}
