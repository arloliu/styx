package shm

import "github.com/arloliu/styx/internal/transport"

var _ transport.ArenaCarryCounter = (*Transport)(nil)

// ArenaCarries reports, per outbound size class, how many payloads this side's
// writer set aside because that class had no free slab, and how many of those
// later obtained one (transport.ArenaCarryCounter).
//
// It reports what the arena-occupancy gauge structurally cannot: occupancy is
// sampled, so a class that exhausts and refills between two samples stalls real
// calls while every sample it takes shows room. These counts are advanced at the
// stall itself, so no stall is invisible to a reporter however coarse its
// sampling.
//
// The counts are this side's outbound direction only — the direction its own
// writer allocates from. A stall on the peer's outbound arena is the peer's to
// report; neither side can see the other's free lists (shm-abi.md §6).
func (t *Transport) ArenaCarries() []transport.ArenaCarry {
	if t.outbound == nil {
		return nil
	}

	return t.outbound.arenaCarries()
}
