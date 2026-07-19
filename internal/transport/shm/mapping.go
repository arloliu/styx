package shm

import (
	"unsafe"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
)

// Sync-page word offsets, relative to Layout.SyncPageOffset (shm-abi.md §3).
// Each word lives alone on its own 64-byte cache line to avoid false sharing
// between the producer-owned tail word and the consumer-owned head word, and
// between either ring's words and the park-state words. All little-endian,
// accessed only with sequentially-consistent atomics.
const (
	syncTailHP   = 0   // tail_hp:      host writes, plugin reads
	syncHeadHP   = 64  // head_hp:      plugin writes, host reads (reclaim gate)
	syncTailPH   = 128 // tail_ph:      plugin writes, host reads
	syncHeadPH   = 192 // head_ph:      host writes, plugin reads (reclaim gate)
	syncParkHP   = 256 // park_state_hp: plugin (H->P consumer) owns; host (producer) reads
	syncParkPH   = 320 // park_state_ph: host (P->H consumer) owns; plugin (producer) reads
	syncPoison   = 384 // poison: shared both directions
	syncShutdown = 448 // shutdown: shared both directions
)

// Per-direction sync-word offsets. A ring's tail/head and its consumer's
// park-state word flip by direction (shm-abi.md §3): H->P uses the *_hp words,
// P->H the *_ph words.
var (
	syncTailOffset = [2]uint64{shm.HostToPlugin: syncTailHP, shm.PluginToHost: syncTailPH}
	syncHeadOffset = [2]uint64{shm.HostToPlugin: syncHeadHP, shm.PluginToHost: syncHeadPH}
	syncParkOffset = [2]uint64{shm.HostToPlugin: syncParkHP, shm.PluginToHost: syncParkPH}
)

// directions maps a role to its outbound and inbound region directions
// (shm-abi.md §1): the host produces H->P and consumes P->H; the plugin is the
// mirror image.
func directions(role Role) (outbound, inbound shm.Direction) {
	if role == RoleHost {
		return shm.HostToPlugin, shm.PluginToHost
	}

	return shm.PluginToHost, shm.HostToPlugin
}

// carveRing overlays a *ring.Ring on the region mapping for one direction: its
// descriptor slots aliased over the ring span and its head/tail words resolved
// from the sync page (shm-abi.md §1/§3). The ring neither maps nor owns this
// memory — it is the caller's already-resolved view into the shared mapping,
// exactly the aliasing contract ring.New documents.
func carveRing(bytes []byte, layout shm.Layout, dir shm.Direction) (*ring.Ring, error) {
	capacity := uint64(layout.RingCapacity)
	ringOff := layout.Rings[dir].Offset

	// Ring spans are page-aligned and each 64-byte slot is naturally aligned
	// (shm-abi.md §1/§4), so the base pointer is aligned for ring.Descriptor;
	// ring.New's contract is that these slots alias the region mapping.
	//nolint:gosec // aliasing the mapped ring span per ring.New's contract; §4 alignment guaranteed
	slots := unsafe.Slice((*ring.Descriptor)(unsafe.Pointer(&bytes[ringOff])), capacity)

	syncBase := layout.SyncPageOffset
	head := regionU64(bytes, syncBase+syncHeadOffset[dir])
	tail := regionU64(bytes, syncBase+syncTailOffset[dir])

	return ring.New(slots, head, tail, capacity)
}

// arenaSpan returns a direction's payload-arena byte span within the region
// mapping (shm-abi.md §1/§6). The outbound side hands it to arena.New; the
// inbound side holds it raw for bounds-checked copy-out (it only reads payloads
// the peer allocated).
func arenaSpan(bytes []byte, layout shm.Layout, dir shm.Direction) []byte {
	off := layout.Arenas[dir].Offset
	end := off + layout.Arenas[dir].Bytes

	return bytes[off:end]
}

// parkWord resolves a direction's park-state word within the sync page
// (shm-abi.md §3). The direction's consumer is its sole writer; the paired
// producer reads it to decide whether to signal.
func parkWord(bytes []byte, layout shm.Layout, dir shm.Direction) *uint32 {
	return regionU32(bytes, layout.SyncPageOffset+syncParkOffset[dir])
}

// poisonWord and shutdownWord resolve the two sync-page words shared by both
// directions (shm-abi.md §3).
func poisonWord(bytes []byte, layout shm.Layout) *uint32 {
	return regionU32(bytes, layout.SyncPageOffset+syncPoison)
}

func shutdownWord(bytes []byte, layout shm.Layout) *uint32 {
	return regionU32(bytes, layout.SyncPageOffset+syncShutdown)
}

// regionU64 and regionU32 resolve a sequentially-consistent atomic word at a
// region byte offset. The sync-page words sit at 64-aligned offsets
// (shm-abi.md §3), so the resulting pointers satisfy the alignment the atomics
// require.
//
//nolint:gosec // resolving a sync-page word aliased over the mapping; §3 guarantees 64-byte alignment
func regionU64(bytes []byte, off uint64) *uint64 {
	return (*uint64)(unsafe.Pointer(&bytes[off]))
}

//nolint:gosec // resolving a sync-page word aliased over the mapping; §3 guarantees 64-byte alignment
func regionU32(bytes []byte, off uint64) *uint32 {
	return (*uint32)(unsafe.Pointer(&bytes[off]))
}
