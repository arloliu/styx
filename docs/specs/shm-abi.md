# Styx Shared-Memory ABI (`layout_version = 1`)

Byte-exact, endian-defined ABI contract for the Styx shared-memory data plane
(memfd region + SPSC descriptor rings + slab arena + eventfd hybrid wakeup).

This document is **normative and self-contained**. An implementation of
`internal/ring`, `internal/arena`, `internal/shm`, `internal/event`, or
`internal/transport/shm` MUST be derivable from this document alone: every byte
offset, field width, alignment, endianness, atomic access rule, initialization
value, and ordering guarantee it needs is stated here. A reader MUST NOT need to
open the design document
([`2026-07-16-styx-design.md`](2026-07-16-styx-design.md)) to learn any of them.
Where the design document is the source of a rule, the corresponding design
section is cited for provenance, not because it must be re-read.

Its existence is a gating deliverable: per the design document's message-frame /
descriptor-format section (§13), *"No SHM transport code merges before that
document exists and the implementation cross-references it."*

---

## §0 Front matter

### Normative language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, and **MAY** are used in the RFC 2119 / RFC 8174
sense. A **MUST** is an ABI conformance requirement: a peer that violates it is
non-conformant and the region is poisoned (§16) rather than trusted.

### Target architectures

| Target | Status | Endianness | Notes |
|---|---|---|---|
| `linux/amd64` | **primary** | little-endian | reference target; all offsets validated here |
| `linux/arm64` | best-effort | little-endian | same layout; compile-time assertions (§17) run in CI |

Both supported targets are **little-endian**. The ABI is **little-endian
throughout** — every multi-byte integer field named in this document is stored
in two's-complement little-endian byte order. This is stated once here and
repeated per field table. A big-endian target is out of scope; supporting one
would require an explicit byte-swap layer and a new `layout_version`, and is not
provided.

The ABI uses only fixed-width integer types (`uint64`, `int64`, `uint32`,
`uint16`, `uint8`) and explicit reserved padding. Consequently the in-memory
struct layout (offsets, sizes, alignment) is **identical on amd64 and arm64** —
the two targets differ in neither field alignment nor total size for any type
used here. Layout does not depend on `GOARCH`.

### Relationship to `layout_version`

`layout_version` is one of the three independently-negotiated axes of the
handshake compatibility tuple (design §10): protocol version, **SHM layout
version**, and plugin identity. This document defines `layout_version = 1`.

- The host writes `layout_version` into the immutable layout page (§2) before
  sealing and passing the region fd.
- Both sides negotiate a single agreed `layout_version` as part of the
  compatibility tuple **before any region is attached**. `layout_version` is
  matched **exactly**, never as a range: a memory layout cannot be partially
  compatible (§19).
- A peer that maps a region whose layout-page `layout_version` differs from the
  negotiated value MUST refuse to proceed and MUST poison the region (§16); it
  MUST NOT attempt to interpret fields under a different layout.

### Document status and change-versioning

Status: **normative, frozen for `layout_version = 1`.**

**The schema is the ABI; the geometry values are configuration.** Under
`layout_version = 1` the frozen contract is the **layout-page schema** — the field
offsets, widths, types, and the structural rules and invariants over them (§2) —
together with the descriptor schema (§4), the sync-page schema (§3), the
frame-kind/flag numbering (§5), and every protocol rule (§7–§16, §19). The
**geometry values** carried in those fields — ring capacity, per-direction arena
size classes and counts, arena byte totals, region size, and the lifecycle
reservation `R` (`lifecycle_reserve`) — are **chosen by the host at `CreateRegion`**
from a validated space (§1, §18), advertised over the control plane, and validated
structurally at attach (§1 Phase 2). Two geometries that both
satisfy the schema and its structural rules are both valid `layout_version = 1`
regions; they are not different layout versions.

Any change that alters an existing **schema** element — a field's offset, width,
type, or meaning; the region page order; the descriptor size; the sync-page
geometry; a frame-kind or flag-bit meaning — is a **breaking** change and requires
a new `layout_version` and a new document (§19). Choosing different **geometry
values** within the schema's structural rules is **not** a version change.
Additive changes that consume only reserved bits, reserved fields, reserved flag
bits, or reserved frame-kind values do **not** bump `layout_version` (§19).

---

## §1 Region layout overview

One shared-memory region per plugin (design §11), created by the host with
`memfd_create(MFD_CLOEXEC|MFD_ALLOW_SEALING)`, sized with `ftruncate`, mapped
`MAP_SHARED`, then sealed
`F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL` before the fd is passed to the
plugin over the control socket via `SCM_RIGHTS` (design §6, §11). The seal set
makes the region size immutable, so every offset below is fixed for the region's
life.

### Page and region order (fixed schema)

Regions are laid out as a strict sequence of contiguous spans, in this order,
with no gaps. The **order and roles are frozen schema**; the **sizes are
geometry** (host-chosen, §2/§18) except the two fixed-size pages:

| # | Span | Size (bytes) | Role |
|---|---|---|---|
| 0 | Layout page | `PageSize` (fixed, 4096) | **immutable** metadata (§2) |
| 1 | Sync page | `PageSize` (fixed, 4096) | **mutable** synchronization state (§3) |
| 2 | Ring H→P | `ring_capacity × DescriptorSize` | descriptor ring, host→plugin (requests) |
| 3 | Ring P→H | `ring_capacity × DescriptorSize` | descriptor ring, plugin→host (responses) |
| 4 | Arena H→P | `arena_bytes_hp` | payload slabs, host-owned (request payloads) |
| 5 | Arena P→H | `arena_bytes_ph` | payload slabs, plugin-owned (response payloads) |

Every span begins on a `PageSize` (4096-byte) boundary **by construction**: ring
spans are always page multiples (`ring_capacity` is a power of two `≥ 64`, so
`ring_capacity·64` is a multiple of 4096), and arena spans are page-aligned by the
roundup rule below (`arena_bytes_dir = roundup(class_total, PageSize)`). Cache-line
alignment (64 B) is therefore automatic for every field below. **Arenas are
independently sized per direction** (`arena_bytes_hp` MAY differ from
`arena_bytes_ph`, §2), so request and response payload pools can be asymmetric.

### Geometry: fixed schema constants vs. host-chosen values

**Fixed by schema (never configured):**

| Constant | Value | Meaning |
|---|--:|---|
| `PageSize` | 4096 | page granularity; every span offset is a multiple of this |
| `CacheLine` | 64 | false-sharing unit; sync-page words are one per line |
| `DescriptorSize` | 64 | one descriptor = one cache line (§4) |

**Host-chosen at `CreateRegion` (carried in the layout page, §2; validated §1
Phase 2 / §18):** `ring_capacity` (per-region, both rings); `lifecycle_reserve`
(the reservation `R`, §18); each direction's size-class table
(`slab_size`/`slab_count` per class) and thus `arena_bytes_hp` / `arena_bytes_ph`;
and the derived `region_size`.

Structural bounds every chosen geometry MUST satisfy (validated at create and at
attach). Let `class_total(dir) = class_base_offset[last] +
slab_size[last]·slab_count[last]` be the raw byte-extent of a direction's classes:

- `ring_capacity` is a power of two in `[64, 1<<20]` (§10 mask arithmetic needs
  power-of-two; the bounds keep the ring sane and `depth` well under `2⁶⁴`);
- `lifecycle_reserve` (the reservation `R`, §2/§18) satisfies `0 < R < ring_capacity`;
- **frozen slab bounds, each direction's class table:** `num_classes ≥ 1`; every
  `slab_size` is a **multiple of 64 and `≥ 64`**; `slab_size` is **strictly
  ascending**; `slab_size[last] ≥ 4096` (guarantees a positive `max_payload` after
  the ≤36 B worst-case overhead, §18); every `slab_count ≥ 1`. A single-class table
  is legal once these hold. Bases are contiguous and **unpadded within the arena**:
  `class_base_offset[0] == 0`, `class_base_offset[c+1] == class_base_offset[c] +
  slab_size[c]·slab_count[c]`, all overflow-safe (§2);
- **arena page-alignment via padding:** each direction's
  `arena_bytes_dir == roundup(class_total(dir), PageSize)`; the trailing pad bytes
  `[class_total(dir), arena_bytes_dir)` are **reserved-zero and never allocatable**
  (they exist only so the next span starts page-aligned); class base offsets
  themselves stay unpadded;
- each direction's `arena_bytes_dir ≤ 4 GiB − 1` (**`4294967295`**), because
  `payload_offset` is a `uint32` arena-relative offset (§4) and must be able to name
  any byte of the arena;
- the whole `region_size ≤ 1<<40` (1 TiB) hard ceiling.

### Recommended profiles (non-normative examples)

Two named geometries are documented as examples; a host MAY choose any geometry
meeting the bounds above.

**`default` profile (≤ 64 MiB total):** `ring_capacity = 4096`; per direction
classes {64 B ×4096, 4 KiB ×1024, 1 MiB ×26} ⇒ `arena_bytes_dir =
64·4096 + 4096·1024 + 1048576·26 = 262144 + 4194304 + 27262976 = 31719424`
(~30.25 MiB); `region_size = 2·4096 + 2·(4096·64) + 2·31719424 = 8192 + 524288 +
63438848 = 63971328` (~61.0 MiB). Asymmetric variants (e.g. a smaller P→H arena)
are allowed.

**`benchmark` profile (the spike-equivalent 146 MiB geometry — used by the Task 11
spike comparison and MUST remain exactly this geometry for comparability):**
`ring_capacity = 8192`; per direction classes {64 B ×8192, 4 KiB ×2048,
1 MiB ×64} ⇒ `arena_bytes_dir = 64·8192 + 4096·2048 + 1048576·64 = 524288 +
8388608 + 67108864 = 76021760`; `region_size = 2·4096 + 2·(8192·64) +
2·76021760 = 8192 + 1048576 + 152043520 = 153100288` (~146.0 MiB). Spans for this
profile: Layout 0, Sync 4096, Ring H→P 8192, Ring P→H 532480, Arena H→P 1056768,
Arena P→H 77078528, end 153100288.

### Span offsets (derived) and total region size

Given a chosen geometry, spans and total size are computed left-to-right:

```
layout_page_offset = 0
sync_page_offset   = PageSize                                  (= 4096)
ring_hp_offset     = sync_page_offset + PageSize               (= 8192)
ring_ph_offset     = ring_hp_offset + ring_capacity·DescriptorSize
arena_hp_offset    = ring_ph_offset + ring_capacity·DescriptorSize
arena_ph_offset    = arena_hp_offset + arena_bytes_hp
region_size        = arena_ph_offset + arena_bytes_ph
```

`internal/shm` MUST compute these with **overflow-safe unsigned 64-bit
arithmetic**: each partial sum accumulates in `uint64`; before each addition it
MUST verify no carry past `math.MaxUint64` (`sum+addend >= sum`), enforce the
per-arena `≤ 4 GiB − 1` and total `≤ 1<<40` ceilings above, and reject any
geometry that fails. No arithmetic on layout-page geometry may be performed with
`int` or with architecture-dependent width.

### Two-phase attach validation (normative, structural)

The region geometry is host-chosen (schema-valid, not a fixed constant), so attach
validates it **structurally and against the control-plane-declared geometry**, not
against hard-coded values. The negotiated geometry (or at minimum `region_size`)
is carried in the `AttachRegion` control message (design §6), so the plugin knows
the expected region size **before** mapping. Attach MUST proceed in two phases and
MUST NOT touch shared memory until the sync-page `poison` word is provably mapped
and writable:

**Phase 1 — size gate before mapping (no SHM access).**
1. `fstat` the received memfd and read its length `L`.
2. **Fixed-header minimum (before anything else):** require both the
   control-plane-declared region size **and** the actual `fstat` length to be
   `≥ 2·PageSize` (`8192`) — the fixed extent of the layout page + sync page. This
   guarantees the `poison` word (absolute offset 4480, below) is inside a region
   that, once mapped, is at least two pages long. An undersized declaration or
   memfd fails here, before mapping.
3. Require `L` to **exactly equal** the region size computed from the
   control-plane-declared geometry (overflow-safe, §1 formula). A truncated,
   oversized, or otherwise wrong-sized memfd fails here.
4. Verify the memfd's seal set via `F_GET_SEALS` includes
   `F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL` (the size cannot change under us).
5. Only after steps 1–4 pass may the region be mapped, and it MUST be mapped in
   full and writable:
   `mmap(NULL, region_size, PROT_READ|PROT_WRITE, MAP_SHARED, memfd, 0)`, where
   `region_size` is the exact `fstat`-checked length from step 3 (`= L`). This
   maps the **entire** validated region starting at offset 0, so every byte in
   `[0, region_size)` — including absolute offset 4480 — is mapped and writable.

The `poison` word is at **absolute region offset 4480** (= sync-page base `4096`
+ its `384`-byte in-page offset, §3) — **not** `sync_page_offset + 4480`, which
would land inside Ring H→P and corrupt a descriptor slot. Because
`sync_page_offset` is fixed at `4096` for every valid geometry (§2), offset 4480
always lies in the sync page; step 5 maps `[0, region_size)` (with
`region_size ≥ 8192` from step 2) at `PROT_READ|PROT_WRITE`, so the `poison` word
at 4480 is provably mapped and writable before Phase 2 runs. **Any Phase-1
failure is reported over the control plane as a handshake/attach rejection
(`ErrIncompatible` or an attach error) and MUST NOT write to shared memory** — the
region is not trustworthy enough to even locate its poison word, and a truncated
memfd could fault on the write.

**Phase 2 — structural geometry check after mapping (SHM read, poison-on-failure).**
Read the layout page **using the fixed v1 schema offsets from §2** (never offsets
derived from the page's own fields). The checks MUST run in this order, so that no
class-table entry is dereferenced before its address range is proven in-bounds:

1. **Header scalars.** `magic` = `STYXSHMR`; `layout_version` = 1;
   `page_size` = 4096; `descriptor_size` = 64; `shard_count` = 1; `reserved_u32`
   = 0.
2. **Ring / reservation.** `ring_capacity` is a power of two in `[64, 1<<20]`;
   `0 < lifecycle_reserve (R) < ring_capacity` (§18).
3. **Class-table address ranges (BEFORE any entry dereference).** For each
   direction `d` with count `n_d` and table offset `o_d`
   (`class_table_hp_offset` / `class_table_ph_offset`): `1 ≤ n_d`;
   `o_d ∈ [256, 4096)`; `o_d` is 4-byte aligned (entry fields are `uint32`);
   `o_d + 16·n_d ≤ 4096` (whole table within the layout page, computed
   overflow-safe); and the two tables `[o_hp, o_hp+16·n_hp)` and
   `[o_ph, o_ph+16·n_ph)` **do not overlap** each other or the header
   (`[0, 256)`). Only after these pass may entries be read.
4. **Per-direction class rules (§2 frozen slab bounds; numeric only).** For each
   direction: every `slab_size` a multiple of 64 and `≥ 64`; `slab_size` strictly
   ascending; `slab_size[last] ≥ 4096`; every `slab_count ≥ 1`;
   `class_base_offset[0] == 0` and `class_base_offset[c+1] == class_base_offset[c]
   + slab_size[c]·slab_count[c]` (contiguous, overflow-safe). (The entries live in
   the layout page, already mapped and range-proven in step 3.)
5. **Arena totals (numeric only, no byte inspection).** For each direction,
   `class_total = class_base_offset[last] + slab_size[last]·slab_count[last]`;
   `arena_bytes_dir == roundup(class_total, PageSize)` and `≤ 4 GiB − 1` (§1).
6. **Spans + total coverage (numeric).** span offsets (`sync_page_offset` = 4096,
   `ring_hp_offset`, `ring_ph_offset`, `arena_hp_offset`, `arena_ph_offset`) each
   equal the value recomputed from the §1 formula, and `region_size` equals both
   the control-plane-declared size **and** the §1-recomputed size — i.e. spans are
   contiguous, page-aligned, non-overlapping, and cover the whole region with no
   gaps. **All span byte-ranges are now proven to lie inside the validated,
   mapped region.**
7. **Byte-content / reserved-zero checks (only AFTER step 6 proves the spans).**
   Now that each span's byte-range is validated in-bounds, inspect reserved bytes:
   - **arena pad range** `[class_total, arena_bytes_dir)` (within each arena span,
     now proven safe by step 6) MUST be reserved-zero and is never allocated;
   - **feature-scoped reserved-zero (§0/§19):** `header_flags`, `reserved_hdr`,
     the sync-page reserved tail, **and each size-class entry's `reserved`
     `uint32`** MUST be zero for every bit/field whose governing feature is **not**
     in the acknowledged tuple; a set reserved bit/field whose feature **was**
     negotiated is permitted and interpreted per that feature (a negotiated
     `layout_version = 1` feature MAY consume any of these, §19).

Any Phase-2 failure MUST poison the region (`POISON_BAD_GEOMETRY`, §16,
first-setter-wins CAS) — the poison word is known-addressable by construction
(Phase 1 passed). The bounds here are the *schema's* structural rules; a geometry
that satisfies them is a valid `layout_version = 1` region regardless of which
recommended profile (if any) it matches.

---

## §2 Layout page (immutable)

**Span:** offset 0, length 4096.
**Endianness:** little-endian for every integer field.
**Mutability:** written **once** by the host before sealing and fd-passing, then
**never modified**. After validating it at attach, each side MUST copy the
geometry into process-local variables and **never re-read the layout page**;
where the platform permits, the layout page SHOULD be remapped read-only after
validation (design §11). A peer that later scribbles on the layout page cannot
redirect the other side's accesses, because the other side no longer reads it.

**Atomicity:** all fields are **write-once-before-publish**. Because publication
happens via the fd-passing / seal boundary (a full control-plane round trip that
strictly happens-before the plugin's first mapping), no per-field atomic
primitive is REQUIRED — the fields are immutable by the time any reader exists.

### Header fields

The **offsets, widths, and types are frozen schema**; the `Init / value` column
shows schema-fixed constants where the value is fixed and "host-chosen" where the
host selects it at `CreateRegion` (with the `default` profile, §1, in parentheses
as a worked example). Every integer field is little-endian.

| Offset | Width | Field | Type | Init / value | Notes |
|--:|--:|---|---|---|---|
| 0 | 8 | `magic` | `[8]byte` | `S T Y X S H M R` (`0x53 54 59 58 53 48 4D 52`) | ASCII tag in byte order; endianness-independent. Mismatch ⇒ poison (§16). |
| 8 | 4 | `layout_version` | `uint32` | `1` (fixed) | see §0; matched exactly at handshake (§19). |
| 12 | 4 | `header_flags` | `uint32` | `0` unless a governing feature negotiated (§0, §19) | reserved header flags; MUST be 0 for every bit whose feature is not in the acknowledged tuple. |
| 16 | 8 | `generation` | `uint64` | host-assigned (≥1) | region incarnation; increments on every restart (§15). Descriptors carry a 32-bit view (§4). |
| 24 | 8 | `region_size` | `uint64` | host-derived (default `63971328`) | total region bytes; MUST equal the sealed memfd length and the §1 recomputed size. |
| 32 | 4 | `page_size` | `uint32` | `4096` (fixed) | MUST equal `PageSize`. |
| 36 | 4 | `descriptor_size` | `uint32` | `64` (fixed) | MUST equal `DescriptorSize`. |
| 40 | 4 | `ring_capacity` | `uint32` | host-chosen (default `4096`) | descriptors per ring; power of two in `[64, 1<<20]` (§1, §10). Same for both rings. |
| 44 | 4 | `num_classes_hp` | `uint32` | host-chosen ≥1 (default `3`) | number of size classes in the **H→P** arena's class table. |
| 48 | 4 | `num_classes_ph` | `uint32` | host-chosen ≥1 (default `3`) | number of size classes in the **P→H** arena's class table. |
| 52 | 4 | `reserved_u32` | `uint32` | `0` | header alignment padding; MUST be 0. |
| 56 | 8 | `sync_page_offset` | `uint64` | `4096` (fixed) | offset of the sync page (§3). |
| 64 | 8 | `ring_hp_offset` | `uint64` | host-derived (default `8192`) | offset of ring H→P (§1 formula). |
| 72 | 8 | `ring_ph_offset` | `uint64` | host-derived (default `270336`) | offset of ring P→H. |
| 80 | 8 | `arena_hp_offset` | `uint64` | host-derived (default `532480`) | offset of arena H→P. |
| 88 | 8 | `arena_ph_offset` | `uint64` | host-derived (default `32251904`) | offset of arena P→H. |
| 96 | 8 | `arena_bytes_hp` | `uint64` | host-derived (default `31719424`) | H→P arena total; MUST equal `roundup(H→P class_total, PageSize)` (§1) and be `≤ 4 GiB − 1`. |
| 104 | 8 | `arena_bytes_ph` | `uint64` | host-derived (default `31719424`) | P→H arena total; MUST equal `roundup(P→H class_total, PageSize)` (§1); independent of `arena_bytes_hp` (asymmetric allowed). |
| 112 | 8 | `class_table_hp_offset` | `uint64` | host-chosen (default `256`) | layout-page offset of the H→P class table; RECOMMENDED `256`. |
| 120 | 8 | `class_table_ph_offset` | `uint64` | host-chosen (default `304`) | layout-page offset of the P→H class table; RECOMMENDED `256 + 16·num_classes_hp`. |
| 128 | 8 | `shard_count` | `uint64` | `1` | **reserved for v2** per-CPU sharded rings (design §12). MUST be 1 in v1. |
| 136 | 4 | `lifecycle_reserve` | `uint32` | host-chosen (default `R = C/16`: 256 at default `C=4096`, 512 at benchmark `C=8192`) | the reservation `R` (§18); both producers read it here. MUST satisfy `0 < R < ring_capacity` (validated §1 Phase 2). |
| 140 | 116 | `reserved_hdr` | `[116]byte` | all `0` | reserved header padding to offset 256; MUST be 0 unless a governing feature negotiated (§0, §19). |

**Alignment and access (normative, every layout-page row incl. the size-class
tables below).** Every integer field is **naturally aligned**: its listed offset
is a multiple of its width (8-byte fields at 8-aligned offsets, 4-byte fields —
including each size-class entry's `reserved` `uint32` — at 4-aligned offsets),
guaranteed because the layout page starts at region offset 0 and every entry
offset is 4-aligned. The only byte-array fields are `magic` and `reserved_hdr`,
which have no alignment requirement beyond byte granularity. **Access discipline
for every row is write-once-before-publish** (§7): no atomic primitive is used or
required, because the fd-pass/seal boundary strictly happens-before any reader
exists. Endianness is little-endian for every integer field. `Init / value` is the
table's column.

### Size-class tables (one per direction)

Each direction has its **own** class table (B2 asymmetric arenas): an array of
16-byte entries. The H→P table has `num_classes_hp` entries starting at
`class_table_hp_offset`; the P→H table has `num_classes_ph` entries starting at
`class_table_ph_offset`. Both tables MUST lie within the layout page
`[256, 4096)`, MUST NOT overlap each other or the reserved header padding, and
each entry MUST lie fully within the page. `class_base_offset` in an entry is the
class's byte offset **within that direction's arena** (relative to
`arena_hp_offset` / `arena_ph_offset`).

Entry schema (16 bytes, LE):

| Field | Offset in entry | Type |
|---|--:|---|
| `slab_size` | +0 | `uint32` |
| `slab_count` | +4 | `uint32` |
| `class_base_offset` | +8 | `uint32` |
| `reserved` | +12 | `uint32` (MUST be 0 unless a governing feature is negotiated, §19) |

Per-direction structural rules the plugin MUST validate at attach (§1 Phase 2;
`POISON_BAD_GEOMETRY` on failure), with the class-table **address range validated
before any entry is dereferenced** (§1 Phase-2 order):

- `num_classes ≥ 1` (a single-class table is legal once the rest holds);
- **frozen slab bounds:** every `slab_size` is a multiple of 64 and `≥ 64`;
  `slab_size` strictly ascending; `slab_size[last] ≥ 4096` (keeps `max_payload`
  positive after the ≤36 B overhead, §18); every `slab_count ≥ 1`;
- `class_base_offset[0] == 0`;
- `class_base_offset[c+1] == class_base_offset[c] + slab_size[c]·slab_count[c]`
  (contiguous, **unpadded within the arena**);
- let `class_total = class_base_offset[last] + slab_size[last]·slab_count[last]`;
  `arena_bytes_dir == roundup(class_total, PageSize)` and `≤ 4 GiB − 1`, all
  overflow-safe (§1). The trailing pad `[class_total, arena_bytes_dir)` is
  **reserved-zero and never allocatable** — it exists only so the next span starts
  page-aligned.

**`default` profile example (both directions identical, symmetric):** 3 entries
each — {`slab_size` 64, `slab_count` 4096, `class_base_offset` 0}, {4096, 1024,
262144}, {1048576, 26, 4456448} — totalling `arena_bytes_dir = 31719424`. The
**`benchmark`** profile uses {64 ×8192, 4096 ×2048, 1048576 ×64} totalling
`76021760` (§1). A host MAY configure asymmetric tables (e.g. a response arena with
fewer 1 MiB slabs).

Bytes of the layout page not occupied by the header (through 255) or the two class
tables are reserved (`0`).

**Concrete decision (geometry is configuration).** The largest class's usable
capacity (minus worst-case negotiated overhead, §5/§18) is the per-direction
`max_payload`; there is no ABI-level fixed maximum frame size — the former M1 UDS
`transport.MaxFrameSize` (1 MiB) is **not** an ABI value and is referenced in this
document only as the M1 UDS default (which itself becomes host-configured; that Go
change is out of scope here). A payload larger than the largest class is rejected
with a typed error before allocation (design §19).

---

## §3 Sync page (mutable)

**Span:** offset 4096, length 4096 (`sync_page_offset`).
**Endianness:** little-endian for every integer field.
**Cache-line rule (design §12):** every mutable word lives alone on its own
64-byte cache line; the remaining bytes of each line are reserved padding
(MUST be 0 at init, ignored thereafter). This prevents false sharing between the
producer-owned tail word and the consumer-owned head word of the same ring, and
between either ring's words and the park-state words.
**Atomicity:** every field in this table is accessed **only** with
sequentially-consistent (seq_cst) atomics (Go `sync/atomic`, which is seq_cst) —
on both sides, in every binding (design §12, §15). No weaker ordering appears
anywhere in the protocol.

| Line | Offset | Width | Field | Type | Atomic | Init | Written by | Read by |
|--:|--:|--:|---|---|---|--:|---|---|
| 0 | 4096 | 8 | `tail_hp` | `uint64` LE | seq_cst store/load | 0 | host (producer of H→P) | plugin (consumer of H→P) |
| 1 | 4160 | 8 | `head_hp` | `uint64` LE | seq_cst store/load | 0 | plugin (consumer of H→P) | host (producer of H→P) |
| 2 | 4224 | 8 | `tail_ph` | `uint64` LE | seq_cst store/load | 0 | plugin (producer of P→H) | host (consumer of P→H) |
| 3 | 4288 | 8 | `head_ph` | `uint64` LE | seq_cst store/load | 0 | host (consumer of P→H) | plugin (producer of P→H) |
| 4 | 4352 | 4 | `park_state_hp` | `uint32` LE | seq_cst store/load | 0 (`AWAKE`) | plugin (consumer of H→P) | host (producer of H→P) |
| 5 | 4416 | 4 | `park_state_ph` | `uint32` LE | seq_cst store/load | 0 (`AWAKE`) | host (consumer of P→H) | plugin (producer of P→H) |
| 6 | 4480 | 4 | `poison` | `uint32` LE | seq_cst CAS/load | 0 | either side (first-setter-wins CAS) | both |
| 7 | 4544 | 4 | `shutdown` | `uint32` LE | seq_cst store/load | 0 | either side (teardown) | consumers (both waiters) |

Bytes from offset 4608 to 8191 of the sync page are reserved (`0`), available
for v2 (e.g. per-shard state).

### Head/tail words are the progress counters

`tail_*` and `head_*` are **monotonically increasing** 64-bit counters (never
reduced modulo capacity; the physical slot is `counter & (ring_capacity-1)`, §10).
Therefore they *are* the per-direction progress counters the design's heartbeat
progress contract (design §18) requires:

- **produced (H→P)** = `tail_hp`; **consumed (H→P)** = `head_hp`;
- **produced (P→H)** = `tail_ph`; **consumed (P→H)** = `head_ph`;
- **in-flight (direction)** = `tail − head` (unsigned, §10).

**Concrete decision.** No separate "progress counter" fields are defined: the
monotonic head/tail words already carry cumulative produced/consumed counts, and
duplicating them would introduce a second source of truth that could disagree with
the ring under a torn update. The supervisor reads `tail_*`/`head_*` directly via
seq_cst loads to build the heartbeat progress snapshot; arena occupancy, which is
producer-local (§6), is reported by the owning side inside the heartbeat control
message, not via the sync page.

### Park-state values

| Name | Value | Meaning |
|---|--:|---|
| `AWAKE` | 0 | consumer is running / not blocked on the eventfd |
| `PARKED` | 1 | consumer has armed and may be (or is about to be) blocked on the eventfd |

Values 2..(2³²−1) are reserved and MUST NOT be written under `layout_version = 1`.

### Poison values (frozen minimal cause enum)

`poison == 0` means healthy; any nonzero value means poisoned. The nonzero values
are a **frozen minimal cause enum** — part of the ABI, so both sides (and the
supervisor reading the word) agree on the reason:

| Value | Name | Meaning |
|--:|---|---|
| 0 | `POISON_NONE` | healthy (not poisoned) |
| 1 | `POISON_GENERIC` | unspecified / control-plane teardown decision |
| 2 | `POISON_BAD_GEOMETRY` | layout/geometry/attach (Phase 2) validation failure (§1, §2) |
| 3 | `POISON_BAD_FRAME` | out-of-range `kind`, flag bit outside `allowed_flags`, or descriptor bounds overrun (§4, §5) |
| 4 | `POISON_RING_CORRUPT` | ring `depth > capacity` (§8, §9) |
| 5 | `POISON_CHECKSUM` | CRC32C mismatch under the negotiated `checksum` feature (§5) |
| 6 | `POISON_STALE_STAMP` | reserved (not used in v1: generation mismatch is discarded, §15) |
| 7 | `POISON_PEER_CRASH` | peer death / crash-window teardown detected (§15) |
| 8 | `POISON_BAD_SYNC` | corrupt sync-page word — invalid park-state value (§12) |

Values 9..(2³²−1) are reserved and MUST NOT be written under `layout_version = 1`.
The first setter wins via a seq_cst CAS from `0` to the cause (§16).

**Concrete decision (frozen enum vs. control-plane cause).** The cause is frozen
into the shared word rather than carried only over the control plane because the
side that poisons the region may crash immediately after the CAS and never send a
control-plane message; a self-describing shared word gives the supervisor a
consistent, always-available reason. The enum is kept deliberately minimal
(coarse categories, not per-call detail); richer diagnostic context (offending
call ID, offset, etc.) still travels over the control plane when the setter
survives, but the coarse category is authoritative and process-crash-robust.

### Shutdown word

`shutdown == 0` normally; set to 1 by either side during teardown, paired with an
eventfd write, to unpark any current or future waiter so no goroutine sleeps
through region destruction (§14, design §15).

---

## §4 Descriptor format

**Size:** exactly 64 bytes = one cache line (`DescriptorSize`, design §12).
**Alignment:** each descriptor begins at a ring offset that is a multiple of 64
(the ring span is page-aligned and slots are 64 B), so every field is naturally
aligned. Ring slot `i` occupies bytes `[i·64, i·64+64)` within its ring span.
**Endianness:** little-endian for every integer field.
**Atomicity:** a descriptor is **written entirely by the producer before the
tail store publishes it**, and read by the consumer only after the seq_cst
tail load reveals it (§8, §9). Individual descriptor fields therefore need **no
per-field atomic** — the seq_cst tail store/load pair is the single
publication/observation edge that orders all descriptor bytes (design §12, §15).
A descriptor MUST NOT be mutated after its tail store; a late writer that mutates
a published slot is a conformance violation.
**Initialization:** every ring slot is zero-filled at region creation (via
`ftruncate`); no slot is "live" until the tail counter reaches it, so descriptor
fields have no static init value beyond zero — each field is written by the
producer per enqueue (§8) and is meaningful only for slots in `[head, tail)`.

| Offset | Width | Field | Type | Endian | Notes |
|--:|--:|---|---|---|---|
| 0 | 8 | `call_id` | `uint64` | LE | monotonic within a generation, never reused within it (design §14). Shared by unary calls and streams. |
| 8 | 8 | `service_id` | `uint64` | LE | FNV-1a-64 of the **full service name**. 0 for kinds that carry no service routing (`CANCEL`, `STREAM_ACK`, `STREAM_CLOSE`, `STREAM_ERR`, `UNARY_RESP`, `UNARY_ERR`). |
| 16 | 8 | `method_id` | `uint64` | LE | FNV-1a-64 of the **bare method name**, per-service scoped. 0 when `service_id` is 0. |
| 24 | 4 | `payload_offset` | `uint32` | LE | byte offset of the slab within the **producing direction's** arena (relative to `arena_hp_offset` for H→P, `arena_ph_offset` for P→H). Points at the start of the slab (trace block if present, else the payload). Valid iff `stored_length != 0` (§5); MUST be 0 when `stored_length == 0`. |
| 28 | 4 | `payload_length` | `uint32` | LE | length in bytes of the message payload only (excludes any trace prefix and CRC trailer, §5). MAY be 0 (empty message). MUST be 0 for descriptor-only kinds (`CANCEL`, `STREAM_ACK`). MUST be ≤ `max_payload` (geometry-derived per direction, §18) and, together with the §5 trace/CRC overhead, ≤ the serving slab's capacity. |
| 32 | 8 | `alloc_seq` | `uint64` | LE | per-arena allocation sequence of the referenced slab; diagnostic stamp with `generation` (§6, §15). Valid iff `stored_length != 0`; MUST be 0 when `stored_length == 0`. |
| 40 | 8 | `budget_ns` | `int64` | LE | remaining deadline budget in nanoseconds at send time (relative duration, never wall-clock; design §14). 0 = no deadline. MUST NOT be written negative. |
| 48 | 2 | `kind` | `uint16` | LE | frame kind (§5). Low byte mirrors `transport.FrameKind`; high byte reserved (0). |
| 50 | 2 | `flags` | `uint16` | LE | flag bits (§5). |
| 52 | 4 | `generation` | `uint32` | LE | low 32 bits of the layout-page `generation`; compared as `uint32(cached_generation)`. A mismatch is a stale late-write signal: the frame is **discarded** (skipped, head advanced, counted in a diagnostic counter), not dispatched and not poisoned (§15, design of record). |
| 56 | 8 | `reserved` | `uint64` | LE | reserved (v2: compact trace handle / sharded-ring routing). MUST be 0 on write; ignored on read in v1. |

`kind` and `flags` jointly form the 32-bit "kind/flags" word at offset 48
(design §12–§13 describe `kind` as part of `flags`).

**Concrete decision (field widths).** `call_id`, `service_id`, and `method_id`
are 64-bit to accommodate `internal/rpcruntime`'s 64-bit call IDs and FNV-1a-64
service/method IDs. `generation` is 32 bits in the descriptor (a truncated view of
the 64-bit layout-page generation) — 32 bits of restart counter is ample for
staleness detection within a live host, and it keeps the descriptor within its
64-byte budget. `alloc_seq` is 64-bit to make stamp wrap a non-concern.

**Concrete decision (trace context is out-of-line).** The design's observability
section (§21) says trace context "rides the descriptor's reserved trace field
(W3C trace-context binary form)". A W3C trace-context binary blob (version +
16-byte trace-id + 8-byte span-id + flags ≈ 26 bytes) **cannot** fit inline
alongside three 64-bit IDs, offsets, lengths, generation, `alloc_seq`, and budget
within a 64-byte cache line. This document therefore carries trace context
**out-of-line**: when the `TRACE_PRESENT` flag (§5) is set, a fixed 32-byte
W3C-binary trace-context block is prefixed to the payload slab (§5). The
descriptor's inline `reserved` field (offset 56) is retained for a future compact
trace handle but carries no trace data in v1. This is a deliberate, recorded
deviation from §21's "inline field" wording, forced by the one-cache-line
descriptor invariant (design §12); the *capability* §21 asks for (trace context
travels with the frame, addable later with no wire change) is preserved.

**Controller-authorized deviation (trace is a negotiated feature).** The design's
observability section (§21: *"Trace context rides the descriptor's reserved trace
field … so eqp-hub can add OTel later without wire changes"*, design line ~646)
frames trace context as an always-available base capability. This document instead
makes `trace` a **negotiated feature**, symmetric with `checksum` and
`compression` (§5 `allowed_flags`): the `TRACE_PRESENT` bit MAY be set only when
`trace` is in the acknowledged handshake tuple. Rationale — symmetry with the other
two payload-layout features; under the former always-on trace, §18's `overhead` term
counted the 32-byte trace prefix unconditionally, shrinking `max_payload` and the
admissible concurrency §18 derives from it for every deployment, even one that never
set `TRACE_PRESENT` on any descriptor. (The per-message size-class jump itself was
never universal: it only ever affected descriptors that actually set
`TRACE_PRESENT`, not every 64-byte message.) Making `trace` a negotiated feature
removes that always-on conservative overhead tax (§18) for deployments that don't
negotiate `trace` — such deployments no longer pay the sizing cost for a feature
they never use — while a deployment that does negotiate `trace` still incurs the
§18 overhead exactly as before, and still sees the class-jump only on descriptors
that actually set `TRACE_PRESENT`. This also makes trace symmetric with
`checksum`/`compression`. This does **not** weaken §21's promise: the
capability is still addable with no *layout* change (turning on the negotiated
`trace` feature needs no `layout_version` bump, §19), so OTel can be added later
exactly as §21 intends — it is simply gated on negotiation rather than always on.

---

## §5 Frame kind enumeration and flag bits

### Frame kinds (`kind`, offset 48)

Numeric values are **frozen** and identical to `internal/transport`'s
`FrameKind` at the wire level (`internal/transport/transport.go`); the SHM
`kind` mirrors `transport.FrameKind`. They MUST NOT be renumbered.

| Name | Value | Carries payload? | Notes |
|---|--:|---|---|
| `UNARY_REQ` | 0 | yes | unary request; `service_id`/`method_id` set. |
| `UNARY_RESP` | 1 | yes | unary success response. |
| `CANCEL` | 2 | no | data-plane cancellation, same ring/ordering as the request it cancels (design §14). Descriptor-only. |
| `STREAM_OPEN` | 3 | yes | reserved for streaming; wire value frozen now (design §14). |
| `STREAM_MSG` | 4 | yes | reserved for streaming. |
| `STREAM_ACK` | 5 | no | reserved; credit return. Descriptor-only. |
| `STREAM_CLOSE` | 6 | maybe | reserved; half-close. |
| `STREAM_ERR` | 7 | yes | reserved; stream error status. |
| `UNARY_ERR` | 8 | yes | error response carrying a status payload (code, message, details) instead of a normal payload (design §14, §17). |

Values 9..255 (low byte) are **reserved/unassigned** and MUST NOT be written
under `layout_version = 1`; a receiver that reads an unassigned `kind` MUST treat
the frame as a conformance violation and poison the region (§16). The high byte
of `kind` (bits 8..15) is reserved and MUST be 0.

**Note on the brief's checklist.** The plan's §5 checklist enumerates
`UNARY_REQ, UNARY_RESP, STREAM_OPEN, STREAM_MSG, STREAM_ACK, STREAM_CLOSE,
STREAM_ERR, CANCEL` and omits `UNARY_ERR`. `UNARY_ERR = 8` is included here
because the SHM `FrameKind` mirrors `transport.FrameKind` at the wire level, and
`transport.go` defines `FrameUnaryErr = 8`. Omitting it would desynchronize the
two wire numberings.

### Flag bits (`flags`, offset 50)

16-bit field; bit 0 is least significant.

| Bit | Mask | Name | Meaning |
|--:|--:|---|---|
| 0 | `0x0001` | `COMPRESSED` | payload is codec-compressed (negotiated). |
| 1 | `0x0002` | `TRACE_PRESENT` | a 32-byte W3C-binary trace-context block is prefixed to the payload slab (see below); negotiated `trace` feature, **off by default**. |
| 2 | `0x0004` | `CRC32C_PRESENT` | a CRC32C payload checksum trailer is present (negotiated `checksum` feature; **off by default**, design §13). |
| 3..15 | `0xFFF8` | reserved | unassigned in v1; governed by `allowed_flags` (below). |

### `allowed_flags` mask (normative, fail-closed)

A receiver MUST NOT silently ignore an unexpected flag bit: a future bit could
change payload prefix, trailer, compression, or validation semantics, so ignoring
it risks mis-parsing the slab. Instead, each side computes an `allowed_flags` mask
from the **acknowledged feature tuple** of the handshake (design §10):

```
allowed_flags = 0
if feature "trace"       negotiated: allowed_flags |= TRACE_PRESENT
if feature "compression" negotiated: allowed_flags |= COMPRESSED
if feature "checksum"     negotiated: allowed_flags |= CRC32C_PRESENT
// all other bits (3..15) remain 0 under layout_version = 1
```

All three payload-layout flags are **negotiated features**, symmetric: a bit MAY
be set on the wire only if its named feature (`trace` / `compression` / `checksum`)
is in the acknowledged handshake tuple (design §10). None is on by default. The
per-descriptor flag semantics and the `stored_length` / slab-prefix layout (below)
are identical regardless of negotiation — negotiation controls only *whether the
flag may be set*, not what it means when set.

On every received descriptor the consumer MUST check
`flags & ~allowed_flags == 0`. **Any set bit outside `allowed_flags` — a reserved
bit, or a feature bit whose feature was not negotiated — is a conformance
violation and MUST poison the region (`POISON_BAD_FRAME`, §16).** The producer
correspondingly MUST NOT set any bit outside its side's `allowed_flags`. This
resolves the earlier "MUST be 0 but ignored" ambiguity: reserved *flag* bits are
never ignored (they fail-closed); only reserved *bytes* whose interpretation
cannot affect parsing (e.g. `Descriptor.reserved` at offset 56, §4) are ignored on
read.

### Payload slab layout when flags are set

The slab referenced by `(payload_offset, payload_length)` is laid out as:

```
[ optional 32-byte trace block ][ payload (payload_length bytes) ][ optional 4-byte CRC32C trailer ]
```

- When `TRACE_PRESENT` is set, the first 32 bytes of the slab (starting at
  `payload_offset`) are the W3C trace-context binary form (version(1) +
  trace-id(16) + span-id(8) + trace-flags(1) = 26 bytes, zero-padded to 32).
  The message payload begins at `payload_offset + 32`. When clear, the payload
  begins at `payload_offset`.
- `payload_length` counts **message bytes only** — it excludes the 32-byte trace
  block and the 4-byte CRC trailer.
- When `CRC32C_PRESENT` is set, a 4-byte little-endian **CRC32C (Castagnoli,
  polynomial `0x1EDC6F41`)** checksum immediately follows the message payload
  (at `payload_offset + [32 if TRACE_PRESENT] + payload_length`). **Coverage:
  the message payload bytes only** — not the trace block, not the descriptor.
  The receiver, when the `checksum` feature is negotiated, MUST recompute and
  compare; a mismatch poisons the region (§16). When the feature is not
  negotiated the bit MUST NOT be set.
- The slab's size class MUST be large enough to hold `stored_length` (defined
  next); this reduces the usable message payload per class by up to 36 bytes (§18).

### Slab presence and `stored_length` (normative)

Define the total bytes physically stored in the slab:

```
trace_prefix = 32 if TRACE_PRESENT else 0
crc_trailer  = 4  if CRC32C_PRESENT else 0
stored_length = trace_prefix + payload_length + crc_trailer
```

- **Arena offset 0 permanently means "no slab" (reserved slab-zero).** Class 0,
  slab index 0 — which sits at `class_base_offset[0] == 0`, i.e. arena-relative
  offset 0 — is **permanently reserved and never allocated** in either direction.
  Consequently a `payload_offset` of 0 is **unambiguously** "no slab", and every
  **present** slab has `payload_offset > 0`. (Cost: one smallest-class slab per
  direction is unusable; negligible.)
- **Slab presence is keyed on `stored_length != 0`, not `payload_length != 0`.**
  A slab is allocated **iff `stored_length != 0`**, and then `payload_offset` and
  `alloc_seq` are both **nonzero** (presence markers — `payload_offset > 0` by the
  reserved-slab-zero rule, and `alloc_seq ≥ 1` by the counter's initialization,
  §6). This closes the empty-flagged-message hole: an empty message
  (`payload_length == 0`) that still carries a trace block and/or a CRC trailer has
  `stored_length ≥ 4` and therefore a real allocation at a nonzero offset with a
  nonzero stamp.
- When `stored_length == 0` (no flags and an empty message), **no slab is
  allocated**: `payload_offset == 0`, `alloc_seq == 0`, and the consumer reads
  zero payload bytes without touching the arena.
- The message payload always begins at `payload_offset + trace_prefix` and spans
  `payload_length` bytes; the CRC trailer, if present, begins at
  `payload_offset + trace_prefix + payload_length`.

### Descriptor-only kinds reject payload-layout flags (normative)

`CANCEL` (2) and `STREAM_ACK` (5) are **descriptor-only**: they MUST set
`payload_length == 0`, `payload_offset == 0`, `alloc_seq == 0`, and **MUST NOT set
any payload-layout flag** (`COMPRESSED`, `TRACE_PRESENT`, or `CRC32C_PRESENT`). A
received descriptor-only frame with any payload-layout flag set, or with a nonzero
`payload_offset`/`payload_length`/`alloc_seq`, is a conformance violation and MUST
poison the region (`POISON_BAD_FRAME`, §16).

### Enumerated empty-message cases (`payload_length == 0`)

| Case | Flags | `stored_length` | Slab? | `payload_offset` / `alloc_seq` | Legal for |
|---|---|--:|---|---|---|
| no flags | none | 0 | no | 0 / 0 | any data kind; and (required form) `CANCEL`/`STREAM_ACK` |
| trace only | `TRACE_PRESENT` | 32 | yes | valid nonzero | data kinds only (`trace` negotiated) |
| CRC only | `CRC32C_PRESENT` | 4 | yes | valid nonzero | data kinds only (`checksum` negotiated) |
| trace + CRC | both | 36 | yes | valid nonzero | data kinds only (`trace`+`checksum` negotiated) |
| invalid flagged lifecycle | any payload-layout flag on `CANCEL`/`STREAM_ACK` | — | — | — | **rejected → poison** |

**Concrete decision (checksum coverage/placement).** The design leaves the CRC's
coverage and placement open; both are frozen here (CRC32C/Castagnoli, 4-byte LE
trailer after the payload, covering the message payload only) even though
enforcement stays behind the negotiated `checksum` feature flag and is
implemented in the transport-assembly task. Freezing it now means the bit
position and byte layout never change when the feature is turned on.

---

## §6 Arena slab header / allocation record

**Span:** Arena H→P (host-owned, `arena_bytes_hp`) and Arena P→H (plugin-owned,
`arena_bytes_ph`) — independently sized, host-configured (§1, §2).
**Ownership (design §12, normative):** each direction's arena is allocated from
and freed to **only by that direction's single producer goroutine**. The free
lists are never touched by two processes.

### Size classes (per direction)

Each direction's classes come from **its own** §2 size-class table
(`class_table_hp_offset` / `class_table_ph_offset`); the two tables MAY differ
(asymmetric arenas, B2). A request of `stored_length` bytes (§5) is served by the
**smallest class in that direction whose `slab_size ≥ stored_length`**. There is
**no cross-class fallback** in v1: if that class is exhausted, allocation fails
with a backpressure error (design §12, §19) rather than promoting to a larger
class. Slab `i` of class `c` occupies bytes
`[class_base_offset[c] + i·slab_size[c], … + slab_size[c])` within the direction's
arena; that offset (arena-relative) is what the descriptor's `payload_offset`
carries.

**Reserved slab-zero.** Class 0, slab index 0 (arena offset 0) is **never
allocated** in either direction, so offset 0 unambiguously means "no slab" (§5).
The allocator initializes each direction's class-0 free list without index 0.

**Profile examples (non-normative):** the `default` profile's per-direction table
is {64 B ×4096, 4 KiB ×1024, 1 MiB ×26}; the `benchmark` profile's is
{64 B ×8192, 4 KiB ×2048, 1 MiB ×64} (§1/§2). The largest class fixes that
direction's `max_payload` (§18).

### `(generation, alloc_seq)` stamp — head-gated reclaim is the sole ABA proof

Every slab handed across the ring carries a `(generation, alloc_seq)` stamp
(design §12), which travels in the **descriptor** (`generation` at offset 52,
`alloc_seq` at offset 32 — §4), not in a separate in-band slab header (a duplicated
header would consume payload space and add a torn-update-prone copy).

**Concrete decision (honest ABA story).** The consumer has **no independent,
authoritative per-slab sequence value** to check `alloc_seq` against — the
descriptor and its stamp are the same untrusted record — so `alloc_seq` is
**demoted to a diagnostic field**, not a validated ABA backstop. It is carried and
logged (and cross-checked against producer-side records in tests), but the
consumer does **not** treat a particular `alloc_seq` value as proof of freshness.
**Head-gated reclamation (§6, below) is the sole normative proof against
use-after-free and ABA**, and it is airtight by construction: a producer reclaims a
slab only after the consumer's head has advanced past the referencing descriptor
and the payload has been copied out, so a slab can never be simultaneously
in-flight and reallocated. This is honest about what the ABI can actually
guarantee (the reviewer's requirement) and matches the retained `bench/spike`
allocator, which carries no per-slab stamp at all.

- The producer maintains a **process-local**, per-arena `alloc_seq` counter
  (`uint64`), **initialized to 1** at region attach; the **first emitted**
  `alloc_seq` is `1` (the value `0` is reserved to mean "no slab", §4/§5). It
  increments by 1 on each successful allocation. It is not stored in shared
  memory. **Overflow:** wrap of a `uint64` counter at one allocation per
  nanosecond takes ~584 years; nonetheless, because `0` is an ABI-reserved
  presence marker (a present slab MUST carry `alloc_seq != 0`, §5), the counter
  **MUST skip 0** on wrap (the value after `2⁶⁴−1` is `1`, not `0`). Its ABA
  significance is only diagnostic (§15), but this conformance requirement stands.

### Free-list linkage

**Concrete decision:** the free list is **process-local and never represented in
shared memory**. Because each arena has a single owning writer per direction
(design §12), free-list linkage never crosses the process boundary; a
cross-process free-list corruption is therefore structurally impossible, not
merely unlikely. The implementation SHOULD use a LIFO stack of free slab indices
per class (a `[]uint32` index stack is sufficient and matches the retained
`bench/spike` allocator). No slab bytes are reserved for linkage; the full
`slab_size` of every slab is available for payload (minus the §5 trace/CRC
overhead when those flags are set).

Reclamation is **head-gated** (design §12): a producer MAY return a slab to its
free list only after the consumer's ring head has advanced past the descriptor
referencing it **and** the payload has been fully copied out (v1 consumers copy
before advancing head, §9). Cancellation/timeout never reclaims a slab early
(design §12); the slot is released only via normal head advancement or region
teardown, so use-after-free and ABA are impossible by construction.

---

## §7 Atomic primitive matrix

Every field named in §2–§6, mapped to its access discipline. "seq_cst" means
sequentially-consistent atomic load/store (Go `sync/atomic`), the **only**
ordering used for the tail and park-state words and, by the ground rule (design
§12, §15), the only ordering anywhere in this protocol. Release/acquire or weaker
is **explicitly forbidden** for the tail and park-state words (§13 shows why).

| Field(s) | Location | Discipline |
|---|---|---|
| `magic`, `layout_version`, `header_flags`, `generation` (page), `region_size`, `page_size`, `descriptor_size`, `ring_capacity`, `num_classes_hp`, `num_classes_ph`, `reserved_u32`, all `*_offset` (incl. `class_table_hp_offset`/`class_table_ph_offset`), `arena_bytes_hp`, `arena_bytes_ph`, `shard_count`, `lifecycle_reserve`, `reserved_hdr`, both size-class tables | Layout page (§2) | **write-once before publish** (fd-pass/seal boundary); no per-field atomic; readers cache locally and never re-read. |
| `tail_hp`, `tail_ph` | Sync page (§3) | **seq_cst** store by producer on publish; **seq_cst** load by consumer and by supervisor. Single writer per word. |
| `head_hp`, `head_ph` | Sync page (§3) | **seq_cst** store by consumer on advance; **seq_cst** load by producer (reclaim gate) and supervisor. Single writer per word. |
| `park_state_hp`, `park_state_ph` | Sync page (§3) | **seq_cst** store by the owning consumer; **seq_cst** load by the paired producer. Single writer per word. |
| `poison` | Sync page (§3) | **seq_cst CAS** (0→cause), first-setter-wins; **seq_cst** load on both read and write paths. |
| `shutdown` | Sync page (§3) | **seq_cst** store on teardown; **seq_cst** load by consumers in the park loop. |
| `call_id`, `service_id`, `method_id`, `payload_offset`, `payload_length`, `alloc_seq`, `budget_ns`, `kind`, `flags`, `generation` (desc), `reserved` | Descriptor (§4) | **write-once before publish**, then read-only. Ordered solely by the producer's seq_cst `tail` store and the consumer's seq_cst `tail` load — no per-field atomic. Validated against the cached generation on read (§15). |
| Payload bytes, trace block, CRC trailer | Arena slab (§5, §6) | **write-once before publish** by producer; read-then-copy by consumer before head advance. Ordered by the same tail store/load edge as the descriptor. |
| `alloc_seq` counter, free lists | Process-local (§6) | not shared; ordinary in-process state, single owning goroutine. |

---

## §8 Ring enqueue pseudocode (producer)

Single-producer per ring. `Ring` binds the ring's descriptor slab bytes and the
sync-page `tail`/`head` words for its direction. `mask = ring_capacity − 1` (§10).
References only §2–§6 field names.

`isLifecycle` is true for `CANCEL` / `STREAM_ACK` (the lifecycle lane, §18), false
for data kinds. `data_inflight` / `lifecycle_inflight` are producer-local lane
counters; `slot_lane[]` is the producer-local ring-slot→lane map;
`last_reconciled_head` is the producer's local copy of how far it has reconciled
lane counters against consumer progress; `R = lifecycle_reserve`, read **once from
the layout page** (§2) at attach and cached. `poison(cause)` is the §16 helper (CAS
+ unconditional teardown wake). `Admit` returns a `Reservation` token that
`Publish` / `RollbackAdmit` consume, so the reserved lane/class/slab can never be
confused with a different frame's.

```
type Reservation { ok bool; lane {DATA|LIFECYCLE}; class {ClassId|none}; slab {Handle|none} }

// Phase 1 — admission gate + reservation. Snapshots head ONCE; validates depth
// BEFORE touching any lane counter; reconciles against THAT snapshot; makes every
// admission decision from THE SAME snapshot (no head reload). Reserves a ring lane
// slot and (for data) an arena slab BEFORE anything is written.
func Admit(isLifecycle, stored_length, direction) Reservation:
    if atomic.Load_seqcst(poison) != 0 or atomic.Load_seqcst(shutdown) != 0:
        return {ok:false}                                      // §16/§14 fail-closed
    // (a) ONE head snapshot; validate depth FAIL-CLOSED before reconcile.
    head = atomic.Load_seqcst(head_word)                       // single snapshot for this Admit
    tail = atomic.Load_seqcst(tail_word)                       // producer-owned; sole writer
    depth = tail - head                                        // §10 unsigned
    if depth > ring_capacity:                                  // corrupt/backwards head ⇒ huge depth
        poison(POISON_RING_CORRUPT); return {ok:false}         // BEFORE any counter mutation
    // (b) Bounded reconcile against the SAME snapshot. In correct operation the
    //     producer reconciles every Admit and publishes ≤1 frame per Admit, so
    //     steps ≤ ring_capacity; a larger gap is corruption.
    steps = head - last_reconciled_head                        // §10 unsigned
    if steps > ring_capacity: poison(POISON_RING_CORRUPT); return {ok:false}
    while steps > 0:
        slot = last_reconciled_head & mask
        if slot_lane[slot] == LIFECYCLE: lifecycle_inflight -= 1 else data_inflight -= 1
        last_reconciled_head += 1; steps -= 1
    // (c) Admission decisions — all from the same snapshot's depth / reconciled counters.
    if depth == ring_capacity: return {ok:false}               // ring full → backpressure
    if not isLifecycle and data_inflight >= ring_capacity - R: // §18 lifecycle window
        return {ok:false}                                      // data window full → backpressure
    class = (stored_length > 0) ? serving_class(stored_length, direction) : none
    slab  = none
    if class != none:
        slab = arena.TryAlloc(class)                           // §6; reserve the slab now
        if slab == none: return {ok:false}                     // arena exhausted → typed backpressure (§18)
    lane = isLifecycle ? LIFECYCLE : DATA
    if lane == LIFECYCLE: lifecycle_inflight += 1 else data_inflight += 1   // reserve ring lane slot
    return {ok:true, lane, class, slab}

// Phase 2/3 — the caller writes payload/trace/CRC into r.slab (§5) and builds the
// descriptor d (payload_offset/alloc_seq nonzero iff r.slab != none, §5).
// Phase 4 — Publish, consuming the reservation token.
func Publish(d, r Reservation):
    // Pre-publish gate (§16, best-effort early-out): re-check poison/shutdown
    // IMMEDIATELY before the tail store. A fault OBSERVED here rolls the reservation
    // back and does not publish. A fault that linearizes AFTER this load but before
    // the tail store may still publish — that residual is backstopped by the
    // consumer final pre-dispatch gate (§9/§16), never dispatched. Not a visibility
    // barrier; see §16.
    if atomic.Load_seqcst(poison) != 0 or atomic.Load_seqcst(shutdown) != 0:
        RollbackAdmit(r); return                               // dropped; call → ErrOutcomeUnknown (§16)
    tail = atomic.Load_seqcst(tail_word)                       // producer-owned; sole writer
    slot = tail & mask                                         // §10
    slot_lane[slot] = r.lane                                   // §18 lane map for reconcile
    // Ordering (design §12, §15): payload write (Phase 2) → descriptor write → tail store.
    ring_slot[slot] = d                                        // §4 descriptor write (all 64 bytes)
    atomic.Store_seqcst(tail_word, tail + 1)                   // §3 publish; single ordering edge
    // r.lane counter was already incremented at Admit (the reservation is realized).

// Rollback — if Phase 2/3 fails, or the pre-publish gate refuses, AFTER Admit but
// BEFORE the tail store. Restores every reservation the token holds.
func RollbackAdmit(r Reservation):
    if r.slab != none: arena.Free(r.slab)                      // §6 return the slab
    if r.lane == LIFECYCLE: lifecycle_inflight -= 1 else data_inflight -= 1  // release ring slot
    // tail unchanged, nothing published, slot_lane untouched (Publish's store never ran).
```

**Ordering (normative):** payload write **happens-before** descriptor write
**happens-before** the seq_cst tail store (in `Publish`). The tail store is the
sole publication edge; a consumer that observes the new `tail` (via its seq_cst
tail load, §9) is guaranteed by the seq_cst total order to observe the fully-written
descriptor and payload (design §12–§13). No field within the descriptor or payload
needs its own atomic.

**Fail-closed (normative).** The invariant is the **computed `depth = tail − head`
(unsigned)**, not the numeric comparison of the two words: after a legitimate
`uint64` wrap, `tail` can be numerically less than `head` while the unsigned
`depth` is still the correct small in-flight count (§10). `depth > ring_capacity`
is impossible for a correct single-writer ring and MUST `poison(POISON_RING_CORRUPT)`
(this is the check that catches a corrupt/backwards tail, since such corruption
makes the unsigned `depth` exceed `ring_capacity`). The poison word is loaded
(seq_cst) at the top of `Admit` so a poisoned region stops admitting immediately.

**Reconcile consistency guarantee (normative).** `Admit` takes **one** `head`
snapshot and uses it for the depth check, the lane reconcile, **and** the admission
decision — the lane counters and `depth` therefore describe the **same** snapshot,
so the invariant `data_inflight + lifecycle_inflight == depth` holds exactly at the
decision point (there is no head reload between reconcile and decision). The depth
check runs **before** the reconcile walk, so a corrupt far-ahead `head` (which
makes `depth` or `steps` exceed `ring_capacity`) poisons **before** any counter is
mutated — the walk can never spin or underflow on corrupt state, and is bounded to
`≤ ring_capacity` iterations. A consumer that advances `head` **after** the
snapshot is simply observed at the **next** `Admit`; in the interim, admission uses
the slightly-stale (larger) `depth`, which is **conservative** — it may reject a
frame that would now fit, but can never over-admit.

The producer's signaling step (`Signal`, §12) runs **after** `Publish`. If the
producer observes `poison != 0` after its tail store, it MUST NOT `Signal` and
proceeds to teardown (§16 race resolution).

---

## §9 Ring dequeue pseudocode (consumer)

Single-consumer per ring. Dequeue is split into **peek → copy → advance** so that
head advancement is the cross-process reclaim signal (§6): the head MUST NOT
advance until the payload has been copied out, or the producer's head-gated
reclaim could free a slab the (possibly cross-process) consumer is still reading.

`poison(cause)` is the §16 helper (CAS + unconditional teardown wake).

```
func TryPeek() (Descriptor, bool):
    head = atomic.Load_seqcst(head_word)          // consumer-owned; §3 head_hp / head_ph
    tail = atomic.Load_seqcst(tail_word)          // §3 seq_cst tail load — the observation edge
    depth = tail - head                            // §10 unsigned (invariant is computed depth, not tail<head)
    if depth > ring_capacity:                       // §10 corruption
        poison(POISON_RING_CORRUPT)                 // §16; do not read the slot
        return zero, false
    if depth == 0:                                  // §10 empty test
        return zero, false
    slot = head & mask                            // §10
    d = ring_slot[slot]                            // §4 descriptor read (safe: tail load ordered it)
    return d, true                                // head NOT advanced yet

func Consume():
    // Fail-closed gate BEFORE peek: a poisoned OR shutting-down region consumes nothing.
    if atomic.Load_seqcst(poison) != 0 or atomic.Load_seqcst(shutdown) != 0:
        stop_consuming(); return
    d, ok = TryPeek()
    if not ok: return                              // empty, or ring corruption already poisoned
    if uint32(d.generation) != uint32(cached_generation):   // §15 stale late-write signal
        stale_frames_discarded += 1                 // diagnostic counter (§15); MAY escalate at a documented threshold
        AdvanceHead(); return                       // DISCARD (design of record): skip, release slot, no dispatch
    if not validate(d):                             // §4/§5/§16; poisons + returns false on any violation
        stop_consuming(); return                    // STOP before any copy or head advance
    // Normative ordering (design §12, §15):
    //   seq_cst tail load (TryPeek) → descriptor read → payload read/copy.
    if stored_length(d) != 0:
        base = arena_base + d.payload_offset        // §5 slab start (validated present & in serving class)
        payload = copy_out(base + trace_prefix(d), d.payload_length)   // copy before advancing
        if d.flags.CRC32C_PRESENT:                  // feature negotiated (validated via allowed_flags, §5)
            if not verify_crc32c(payload, crc_at(base, d)):
                poison(POISON_CHECKSUM); stop_consuming(); return   // §16; no advance
    else:
        payload = empty                             // no slab (§5)
    // FINAL GATE (§16 TOCTOU / §14): no dispatch may BEGIN after a poison or shutdown
    // observation. Re-load BOTH immediately before dispatch.
    if atomic.Load_seqcst(poison) != 0 or atomic.Load_seqcst(shutdown) != 0:
        stop_consuming(); return                    // do NOT advance; region is torn down whole
    dispatch(d, payload)                            // hand to rpcruntime (response path re-checks poison/shutdown)
    AdvanceHead()                                   // §3 seq_cst head store: reclaim signal (§6)

// validate returns false (and poisons) on any conformance violation; otherwise true.
// It performs NO copy and NO head advance — a false result stops Consume before both.
func validate(d) bool:
    if (d.kind >> 8) != 0 or lowByte(d.kind) not in {0..8}:   // §5 assigned kinds
        poison(POISON_BAD_FRAME); return false
    if (d.flags & ~allowed_flags) != 0:            // §5 fail-closed reserved/unnegotiated flags
        poison(POISON_BAD_FRAME); return false
    if d.budget_ns < 0:                             // §4 budget MUST NOT be negative
        poison(POISON_BAD_FRAME); return false
    if isDescriptorOnly(d.kind):                    // §5 CANCEL / STREAM_ACK
        if d.flags & (COMPRESSED|TRACE_PRESENT|CRC32C_PRESENT) != 0
           or d.payload_offset != 0 or d.payload_length != 0 or d.alloc_seq != 0:
            poison(POISON_BAD_FRAME); return false
        return true
    sl = stored_length(d)                           // §5 = trace_prefix + payload_length + crc_trailer
    if sl == 0:
        if d.payload_offset != 0 or d.alloc_seq != 0:
            poison(POISON_BAD_FRAME); return false
        return true
    // slab present (§5 presence markers): offset and stamp MUST be nonzero.
    if d.payload_offset == 0 or d.alloc_seq == 0:   // offset 0 = reserved slab-zero ⇒ "no slab"
        poison(POISON_BAD_FRAME); return false
    if d.payload_length > max_payload(this_direction):   // §18 geometry-derived cap
        poison(POISON_BAD_FRAME); return false
    c = serving_class(sl, this_direction)           // §6 smallest class ≥ sl
    if not slabInClass(d.payload_offset, sl, c, this_direction_arena):  // §5/§6
        poison(POISON_BAD_FRAME); return false
    return true

func AdvanceHead():
    head = atomic.Load_seqcst(head_word)
    atomic.Store_seqcst(head_word, head + 1)        // §3 publish consumer progress
```

`slabInClass(off, len, c, arena)` requires `off == class_base_offset[c] +
i·slab_size[c]` for some slab index `i < slab_count[c]` with `(c,i) != (0,0)` (the
reserved slab-zero, §5/§6) and `len ≤ slab_size[c]` — i.e. `off` names a whole slab
**of the serving class `c`** (the class containing `off` MUST equal
`serving_class(stored_length)`), never straddling slabs, another class, or the
arena boundary.

**Ordering (normative):** seq_cst tail load **happens-before** descriptor read
**happens-before** payload read. The head store in `AdvanceHead` happens **after**
copy-out **and** after both a successful `validate` and the final poison/shutdown
gate; any `validate` failure, CRC failure, or observed poison/shutdown returns
**before** dispatch and **before** any head advance, so a corrupt, poisoned, or
post-teardown descriptor is never dispatched and its slot is never released as
consumed. The consumer's parking (`Wait`, §11) wraps this consume loop.

---

## §10 Wraparound / full-empty arithmetic

`tail` and `head` are **unsigned 64-bit monotonic sequence numbers**, incremented
by 1 per enqueue / dequeue and **never** reduced modulo capacity. They are
distinct from the **slot index**, which is `sequence & mask`.

- `ring_capacity` MUST be a power of two in `[64, 1<<20]` (host-chosen, §1; e.g.
  8192 in the `benchmark` profile, 4096 in `default`); `mask = ring_capacity − 1`.
  Slot of a sequence value `n` is `n & mask`.
- **Count in flight:** `tail − head` computed in `uint64`. Because both counters
  advance by 1 and the ring holds at most `ring_capacity` entries, `0 ≤ tail −
  head ≤ ring_capacity` always.
- **Empty test:** `tail == head`.
- **Full test:** `tail − head == ring_capacity` (equivalently `≥`, since the
  count can never exceed capacity when the producer honors the full check).

### Wraparound proof sketch

Unsigned subtraction is exact modulo 2⁶⁴: for any `tail, head ∈ [0, 2⁶⁴)`, the
computed `(tail − head) mod 2⁶⁴` equals the true number of increments the
producer has made beyond the consumer, **provided** that true difference is `<
2⁶⁴`. The producer's full check keeps the live difference `≤ ring_capacity`
(`≤ 1<<20` by the §1 bound) `≪ 2⁶⁴`, so the difference never approaches the
modulus; even when `tail`
itself wraps past `2⁶⁴` while `head` has not yet, `tail − head` is still the
correct small count because both are reduced mod 2⁶⁴ identically. At one enqueue
per nanosecond it takes ~584 years to wrap a `uint64`, and correctness does not
even depend on avoiding the wrap — only on the live count staying below `2⁶⁴`,
which the capacity bound guarantees. The slot mapping `n & mask` is likewise
wrap-immune: it depends only on the low `log2(ring_capacity)` bits, which advance
cyclically regardless of the high bits wrapping.

---

## §11 Consumer parking protocol

Hybrid **spin-then-park** per direction, with a race-free arming protocol
(design §15). The consumer is the sole writer of its `park_state` word (§3) and
reads its ring `tail` word; both accesses are seq_cst. `lastSeen` is the tail
value the consumer has already drained up to.

**`shutdown` is checked before returning work in EVERY path** (spin, arm re-check,
and post-wake): a teardown that has set `shutdown` MUST NOT see `Wait` hand a
descriptor to `Consume`, so `ok=false` always wins over pending work once shutdown
is observed.

```
func Wait(lastSeen) -> (tail, ok):
    // 1. Spin up to a wall-time budget (NOT an iteration count).
    deadline = now() + effectiveSpinBudget()          // see spin policy below (non-ABI value)
    while effectiveSpinBudget() > 0 and now() < deadline:
        if atomic.Load_seqcst(shutdown) != 0: return lastSeen, false   // shutdown BEFORE work
        t = atomic.Load_seqcst(tail_word)
        if t != lastSeen: return t, true              // work appeared; never parked
        runtime.Gosched()                             // yield to scheduler at sub-budget intervals

    // 2. Arm + re-check (the race-free core).
    loop:
        atomic.Store_seqcst(park_state, PARKED)       // seq_cst arm
        if atomic.Load_seqcst(shutdown) != 0:         // shutdown BEFORE returning work
            atomic.Store_seqcst(park_state, AWAKE); return lastSeen, false
        t = atomic.Load_seqcst(tail_word)             // seq_cst re-load AFTER arming
        if t != lastSeen:
            atomic.Store_seqcst(park_state, AWAKE)     // work seen while arming; disarm
            return t, true

        // 3. Block on the eventfd (drains counter; §14).
        if blocking_read_eventfd() fails:
            atomic.Store_seqcst(park_state, AWAKE); return lastSeen, false

        // 4. On EVERY wake (real or spurious): store AWAKE FIRST, then re-scan.
        atomic.Store_seqcst(park_state, AWAKE)         // AWAKE first, unconditionally
        if atomic.Load_seqcst(shutdown) != 0: return lastSeen, false    // shutdown BEFORE work
        t = atomic.Load_seqcst(tail_word)
        if t != lastSeen: return t, true
        // spurious/coalesced wake with no new work: re-arm (loop). The parked
        // state is never left dangling — AWAKE was just stored, PARKED is
        // re-stored at the top of the loop.
```

The arm-path shutdown check sits **between** the `PARKED` store and the `tail`
re-load; it does not disturb the race-free tail/park-state pair the litmus (§13)
reasons about (that pair is still `store PARKED` → `load tail`), it only adds an
earlier exit. `Consume` re-checks `shutdown` (and `poison`) before dispatch (§9),
so even a descriptor that `Wait` returned an instant before teardown is never
dispatched after `shutdown` is observed.

**Normative invariants (ABI):**

- The arm step is a **seq_cst store** of `PARKED`, **followed by** a **seq_cst
  re-load** of `tail`. This order is mandatory (design §15): arming before
  re-checking is what makes the producer's `PARKED` observation and the
  consumer's post-arm tail observation mutually exclusive-or-both-visible (§13).
- On **every** wake — eventfd, spurious, or coalesced — the consumer stores
  `AWAKE` **before** re-scanning the ring. The parked state is **never left
  dangling** after any wake.
- A spurious wakeup is permitted and handled by the re-scan/re-arm loop; a lost
  wakeup is impossible (§13).

**Spin policy (configuration, NON-ABI).** The spin budget is a wall-time value
(a `time.Duration`), never an iteration count, tuned by benchmarking. What is
**ABI-normative** is only the state-word protocol and its seq_cst ordering above;
the concrete budget is configuration. The following requirement, however, is
**binding** (M0 gate exit condition, `docs/plans/2026-07-16-m0-gate-report.md`
§10–§11): `effectiveSpinBudget()` MUST be **quota-aware** — spin MUST be disabled
(budget forced to 0), or sharply shrunk, under **any finite cgroup CPU quota**
(not merely below a 2-CPU threshold), and MUST be 0 when `GOMAXPROCS == 1`. A
spinner MUST NEVER be able to starve the producer, dispatcher, heartbeat, or GC of
the only runnable P. A zero-spin mode MUST exist and MUST be correct (only
slower). These are policy constraints on the *value*, not on the state-word ABI.

---

## §12 Producer signaling protocol

Runs on the producer immediately after a successful `Publish` (§8). The
producer is the sole reader of the paired consumer's `park_state` word.

```
func Signal():
    // Precondition (already done by Admit→…→Publish, §8):
    //   payload write → descriptor write → seq_cst tail store.
    if atomic.Load_seqcst(poison) != 0 or atomic.Load_seqcst(shutdown) != 0: return  // §16/§14
    s = atomic.Load_seqcst(park_state)                 // seq_cst load of consumer state
    switch s:
        case PARKED: write_eventfd()                   // §14 wake the parked consumer
        case AWAKE:  /* nothing: consumer will re-scan and see the new tail */
        default:                                        // §3: only AWAKE/PARKED are legal
            poison(POISON_BAD_SYNC)                     // corrupt sync-page word ⇒ fail-closed (§3, §16)
```

**Normative ordering (design §15):**
**payload write → descriptor write → seq_cst tail store → seq_cst `park_state`
load → conditional eventfd write.** The tail store (in `Publish`) and the
`park_state` load (here) are both seq_cst and therefore participate, together with
the consumer's `park_state` store and tail load (§11), in a single total order.
That is what guarantees at least one side observes the other's write in every
interleaving: **a wakeup may be spurious but is never lost** (§13).

---

## §13 Litmus test table

The four load-bearing seq_cst accesses:

| Label | Actor | Access |
|---|---|---|
| **P1** | producer | seq_cst **store** `tail := tail+1` (publish, §8) |
| **P2** | producer | seq_cst **load** `park_state` (§12) |
| **C1** | consumer | seq_cst **store** `park_state := PARKED` (arm, §11) |
| **C2** | consumer | seq_cst **load** `tail` (re-check after arm, §11) |

Program order fixes **P1 → P2** and **C1 → C2**. Because all four are seq_cst,
they occupy a **single total order** `S`. The producer signals iff at P2 it reads
`PARKED` (C1 precedes P2 in `S`, before any AWAKE store). The consumer avoids
blocking iff at C2 it reads the new tail (P1 precedes C2 in `S`). The six
interleavings consistent with program order:

| # | Order in `S` | Producer signals? (P2 sees PARKED) | Consumer self-observes work? (C2 sees new tail) | Outcome | Total-order guarantee |
|--:|---|---|---|---|---|
| 1 | P1 P2 C1 C2 | no (P2 before C1) | yes (P1 before C2) | consumer sees work in C2, never blocks | seq_cst: C2 after P1 ⇒ new tail visible |
| 2 | P1 C1 P2 C2 | yes | yes | both fire; wake is (harmlessly) spurious | seq_cst total order |
| 3 | P1 C1 C2 P2 | **maybe** (consumer's AWAKE disarm store may precede P2) | yes (P1 before C2) | consumer sees work in C2, disarms, consumes; signal, if it fires, is spurious | seq_cst: C2 after P1 ⇒ new tail visible |
| 4 | C1 P1 P2 C2 | yes (P2 after C1) | yes (C2 after P1) | armed + signaled + work visible | seq_cst total order |
| 5 | C1 P1 C2 P2 | maybe | yes (C2 after P1) | consumer sees work in C2, disarms, consumes | seq_cst: C2 after P1 ⇒ new tail visible |
| 6 | **C1 C2 P1 P2** | **yes (P2 after C1)** | **no (C2 before P1)** | **consumer parks; producer sees PARKED, signals eventfd; blocking read returns; consumer stores AWAKE, re-scans, sees new tail** | **the critical case — see proof** |

**Mutual-exclusion proof (no lost wakeup).** The only dangerous outcome is *both*
"consumer sees old tail" (C2 before P1 in `S`) *and* "producer sees not-PARKED"
(P2 before C1 in `S`). Assume both. With program order C1 → C2 and P1 → P2, `S`
would contain `C2 < P1` and `P2 < C1`, hence `C1 → C2 < P1 → P2 < C1`, i.e.
`C1 < C1` — a contradiction. Therefore in **every** interleaving at least one of
{producer signals, consumer self-observes the work} holds: **the wakeup is never
lost.** This argument requires a *single total order over all four accesses*,
which **only sequential consistency provides** — release-publish + independent
acquire load does not order P2 against C1 (or C2 against P1) and is why weaker
orderings are forbidden here (design §15).

**Post-eventfd AWAKE transition (exhaustive).** After a blocking eventfd read
returns, the consumer stores `park_state := AWAKE` (**C3**) and then re-loads
`tail` (**C4**) before either returning with work or re-arming (next C1). Just
before C3, `park_state == PARKED` (the arm that led to the block); C3 clears it to
`AWAKE`, which persists until the next C1 re-stores `PARKED`. A producer publishing
concurrently runs P1 (tail store) → P2 (park-state load) against C3 (AWAKE store) →
C4 (tail load). Program order fixes **P1 → P2** and **C3 → C4**; all six
linear extensions, each an individual row, with what P2 reads:

| # | Order in `S` | P2 reads | Producer signals? | C4 sees new tail? | Outcome (no lost wakeup) |
|--:|---|---|---|---|---|
| 7 | P1 P2 C3 C4 | `PARKED` (P2 before C3) | yes | yes (C4 after P1) | consumer returns with work at C4; the signal is a coalesced, harmless extra wake |
| 8 | P1 C3 P2 C4 | `AWAKE` (P2 after C3) | no | yes (C4 after P1) | consumer self-observes work at C4; no signal needed |
| 9 | P1 C3 C4 P2 | `AWAKE` (P2 after C3) | no | yes (C4 after P1) | consumer self-observes work at C4; no signal needed |
| 10 | C3 P1 P2 C4 | `AWAKE` (P2 after C3) | no | yes (C4 after P1) | consumer self-observes work at C4; no signal needed |
| 11 | C3 P1 C4 P2 | `AWAKE` (P2 after C3) | no | yes (C4 after P1) | consumer self-observes work at C4; no signal needed |
| 12 | **C3 C4 P1 P2** (+ re-arm) | **AWAKE or PARKED (maybe)** — depends on whether the re-arm's next C1 precedes P2 in `S` | maybe | **no (C4 before P1)** | consumer finds no work at C4 and **re-arms** into a fresh `(C1,C2)` cycle. **Two subcases:** (a) **P2 → AWAKE** (P2 before the re-arm's next C1): no signal, but the next C2 then observes the new tail (P1 before that C2 in this ordering) and the consumer does not park — no lost wakeup; (b) **P2 → PARKED** (next C1 before P2, e.g. `C3 C4 C1 C2 P1 P2`): the next C2 reads the **old** tail and the consumer heads toward blocking, but **P2 observes PARKED and signals** — this is exactly base-litmus **row 6** on the re-armed cycle. Either subcase is safe; see the re-arm proof below |

**Re-arm proof (row 12, corrected).** It is **not** true that P1 necessarily
precedes the next C2. The extension `C3 C4 C1 C2 P1 P2` is legal: the re-armed
`C2` reads the **old** tail (P1 has not happened yet), so the consumer proceeds
toward blocking on the eventfd. Safety instead comes from the **signal route**:
after the re-arm's `C1` stores `PARKED`, the producer's `P2` (which follows P1 in
program order, and here follows `C1` in `S`) **observes `PARKED` and writes the
eventfd**. That is exactly base-litmus **row 6** (`C1 C2 P1 P2`) applied to the
re-armed cycle: `P2` after `C1` ⇒ producer signals ⇒ the blocking read returns ⇒
the consumer wakes (C3′), stores `AWAKE`, re-scans, and now sees the tail. The
no-lost-wakeup guarantee for the re-armed cycle is the row-6 mutual-exclusion
argument (a single seq_cst total order makes "C2 sees old tail" and "P2 sees
not-PARKED" mutually exclusive), **not** an ordering of P1 relative to the next C2.

Every row ends with the consumer either returning with work or re-arming into a
fresh `(C1,C2)` cycle covered by rows 1–6; **no ordering loses the wakeup.**
Coalesced eventfd signals (rows 7, and any double-signal) are harmless because the
consumer always re-scans after waking (§14). A model test SHOULD explore every
total order of {P1,P2,C1,C2} and {P1,P2,C3,C4} plus the re-arm edge rather than
encoding only these prose rows.

---

## §14 Eventfd semantics

Eventfds are created by the **host** and passed with the region fd (design §15).
One eventfd per direction wakes that direction's parked consumer.

- **Mode:** non-semaphore (`eventfd(2)` **without** `EFD_SEMAPHORE`). A read
  returns the accumulated counter and resets it to 0 — i.e. reads **drain** the
  counter, and multiple signals **coalesce** into one wake. Coalescing is safe
  because the consumer always re-scans the ring after waking (§11, §13).
- **Blocking discipline:** an eventfd that a `Wait` may block on MUST be created
  in **blocking** mode (no `EFD_NONBLOCK`) so the park's `read(2)` sleeps until
  the counter is nonzero. A non-blocking eventfd would turn the park into a
  100%-CPU `EAGAIN` busy-spin. (A producer-only endpoint that only ever writes
  the eventfd is unaffected by the fd's blocking mode.)
- **Signal write:** the producer writes the 8-byte little-endian value `1`
  (`{1,0,0,0,0,0,0,0}`) to increment the counter (§12).
- **Retry rule:** `read(2)` retries on `EINTR` and `EAGAIN`; `write(2)` retries on
  `EINTR`. `EAGAIN` on a write cannot occur for this protocol except at the
  pathological counter-overflow-to-`0xffffffffffffffff` case, which the
  8-byte-of-1 increment never approaches.
- **Runtime integration (non-ABI, design §15):** the blocking read SHOULD go
  through the Go runtime poller so the goroutine parks and the OS thread is
  released, rather than pinning a thread in a raw `read`.
- **Shutdown / teardown:** a dedicated `shutdown` word (§3) is set (seq_cst store
  of 1) **and BOTH per-direction eventfds are written** (one write each), so a
  consumer parked on either direction — and any future waiter — is unparked and no
  goroutine sleeps through region destruction. Writing both eventfds is mandatory
  because a single crash/poison must release waiters in both directions, including
  the "tail stored but never signaled by a dead producer" window (§15). On waking,
  the consumer checks `shutdown` before re-scanning and returns `ok=false` when set
  (§11). This is the teardown unpark the lifecycle state machine relies on
  (design §9 step 2, §15). Teardown is idempotent: a second `shutdown` store /
  eventfd write is harmless (coalesced).

---

## §15 Generation and crash recovery

- **Where `generation` is stamped.** The authoritative generation is the 64-bit
  `generation` field on the **immutable layout page** (§2, offset 16), written by
  the host before sealing. Every **descriptor** carries the low 32 bits in its
  `generation` field (§4, offset 52). Every **slab** carries `(generation,
  alloc_seq)` via its descriptor (§6) — no separate in-slab stamp.
- **Increment on restart.** `generation` increments on **every** restart
  (design §11, §15). Because a restart always creates a **fresh region** with a
  new memfd and a new sealed layout page, the new generation is baked into the
  new region's immutable layout page; there is no in-place mutation of a live
  generation word.
- **Generation comparison (truncated) and disposition — DISCARD.** Each side
  caches the 64-bit region `generation` at attach; descriptors carry its **low 32
  bits** (§4). The read-path check (§9 `Consume`) compares
  `uint32(d.generation) == uint32(cached_generation)`. **A mismatch is a
  stale-generation late-write signal and is DISCARDED, not poisoned** (design of
  record, §15/§11 adjudication; M1 precedent): the consumer skips the frame,
  advances head (releasing the slot), and increments a **diagnostic counter**
  (`stale_frames_discarded`). This is the late-write detection signal that feeds
  the higher-layer machinery (Task 8); it is deliberately not fatal, because a
  late write from a dying incarnation is an expected, bounded event, not
  corruption. **The discard count is exposed as a diagnostic signal the supervisor
  (Task 8) consumes to drive escalation policy.** Escalation to `poison` after a
  threshold of discards (a heuristic for "this is not late writes, this is a storm
  of corruption") is **supervisor policy**, not an ABI rule: at the ABI level it is
  a **MAY**, and the concrete threshold and action are **owned by Task 8**, not
  frozen here. The **normative** ABI disposition of a single mismatch is discard.
  Note the consumer
  does **not** read the discarded frame's arena slab (it only advances head), so a
  stale frame with garbage offset/length cannot cause an out-of-bounds read.
  Discarding *late frames for a terminal call ID* is a separate, higher-layer
  concern in `internal/rpcruntime` (design §14).
  **Truncation wrap:** a 32-bit collision needs exactly 2³² restarts between a
  write and its observation; each restart is a full teardown with a fresh region,
  so a live region never holds a 2³²-apart generation — the truncation is safe.
- **Crash reclaims everything at once.** There is no partial recovery: on crash
  or poison the region is discarded whole and a fresh region (new generation) is
  created by the supervisor (design §11, §12). No in-place repair path exists
  (§16).

### Producer-death and poison-while-parked windows (normative)

The correctness-defining windows are a few instructions wide (design §23's
failpoint matrix). For a producer that dies mid-enqueue, the disposition per window
is:

| Producer dies after… | Visible to consumer? | Disposition |
|---|---|---|
| **payload write**, before descriptor write | no (tail not advanced) | frame never existed; slot never published; region torn down |
| **descriptor write**, before tail store | no (tail not advanced) | same — the descriptor bytes are unpublished garbage in an unreached slot |
| **tail store**, before `Signal`'s park-state load | yes (a consumer already parked gets **no data-plane wake**) | the descriptor **may** be consumed if the consumer is still awake and re-scans; a parked consumer is released only by the teardown path below — never left to sleep on the published-but-unsignaled frame |
| **park-state load**, before eventfd write | yes | if it read `PARKED`, the wake is owed but not delivered; teardown delivers it |
| **eventfd write** | yes | wake delivered normally; the frame is consumable until teardown wins |

**Who sets `shutdown`, and which eventfds.** Producer death is detected by the
**supervisor** (control-plane heartbeat + `SIGCHLD`/`waitpid`, design §18) or by
the surviving side observing control-socket EOF (design §9). On any crash or
poison detection, **the surviving side (and/or the supervisor on the host)** MUST,
as part of teardown step 2 (design §9): (1) seq_cst-store `shutdown = 1`, and
(2) **write BOTH per-direction eventfds** — so that a consumer parked on *either*
direction is unparked and no goroutine sleeps through region destruction. A
consumer woken this way observes `shutdown != 0` (checked before re-scan, §11) and
returns `ok=false`, exiting the consume loop. This closes the "tail stored but
never signaled" window: the parked consumer is always released by the teardown
eventfd write even though the dead producer never sent the data-plane signal.

**Poison while parked.** The `poison(cause)` helper (§16) is defined to perform the
teardown wake unconditionally — CAS the cause, then set `shutdown = 1` and write
**both** eventfds whether or not the CAS won — so every code path that detects a
fault (`§8` admission, `§9` validate/CRC, `§12` Signal) automatically releases a
consumer parked in either direction. A consumer woken this way sees `shutdown`
(checked before returning work, §11) and/or `poison` (checked before dispatch, §9)
and stops. No parked waiter is ever left dangling across a poison or crash.

---

## §16 Poison protocol

- **Safety principle (normative — poison vs. discard).** The disposition of an
  anomaly is governed by one rule: **poison when continuing to process would be
  unsafe** — an out-of-bounds arena/slab read, a mis-dispatch to the wrong call, a
  corrupt sync word, or any state whose *interpretation* could act on bad memory —
  and **discard (skip) when the anomaly is safely skippable** without touching
  untrusted memory. A **generation mismatch is the canonical skippable case** (§15):
  the consumer only advances head, never reads the slab, so the frame is discarded
  and counted, not poisoned. Everything in the "who may set it" list below is a
  *would-be-unsafe* case and therefore poisons. Discardable anomalies feed a
  diagnostic counter that the supervisor (Task 8) may escalate on — escalation is
  supervisor policy, not an ABI-frozen threshold (§15).
- **Location / width / atomic.** The `poison` word: sync page (§3), offset 4480,
  `uint32` LE, one cache line, accessed with **seq_cst** atomics. `0` =
  `POISON_NONE` (healthy); any nonzero value is a frozen cause enum (§3).
- **Set semantics — `poison(cause)` helper (CAS + unconditional wake).** Every
  poison is performed via this normative helper, and every `poison(cause)` call in
  this document (§8, §9, §12, §15) means exactly:

  ```
  func poison(cause):
      atomic.CAS_seqcst(poison_word, 0, cause)   // first-setter-wins; a lost CAS keeps the original cause
      // UNCONDITIONALLY (whether or not this call won the CAS):
      atomic.Store_seqcst(shutdown, 1)           // §3 shutdown word
      write_eventfd(hp_eventfd)                   // §14 wake H→P consumer
      write_eventfd(ph_eventfd)                   // §14 wake P→H consumer
  ```

  The CAS makes the first setter win (the original cause is preserved); the
  unconditional wake guarantees that no matter which side detects the fault, a
  consumer parked in **either** direction is released (§14, §15). The wake is
  idempotent (coalesced), so a lost CAS still safely re-issues it.
- **Who may set it, and when.** Either side MAY set poison upon detecting any
  unrecoverable condition or ABI conformance violation, including: bad `magic` or
  `layout_version` mismatch (§2); region-size or geometry inconsistency (§1, §2);
  an out-of-range or reserved `kind` (§5); a descriptor `payload_offset` /
  `payload_length` that overruns the referenced arena or slab class; a
  `TRACE_PRESENT`/`CRC32C_PRESENT` layout that does not fit the slab (§5); a
  CRC32C mismatch when the `checksum` feature is negotiated (§5); an invalid
  park-state word (`POISON_BAD_SYNC`, §12); or a control-plane teardown decision.
  Note a **generation mismatch is NOT** in this list — it is discarded, not
  poisoned (§4, §15).
- **Detection points on both paths (integrated into §8/§9).** The poison word
  MUST be checked with a seq_cst load: on the **producer** path at the top of
  `Admit` (§8) and before `Signal` (§12); on the **consumer** path both at the top
  of `Consume` (before `TryPeek`) **and** at the final gate immediately before
  `dispatch` (§9). A poisoned region MUST stop being used: the producer stops
  admitting/publishing, the consumer dispatches nothing further, and the supervisor
  tears down and restarts with a fresh region (design §11, §18).
- **Poison vs. dispatch race — honest TOCTOU bound (normative).** The bound is:
  **no frame is DISPATCHED after a poison (or shutdown) observation; a frame MAY
  still become VISIBLE on the ring.** Visibility is not prevented — a producer that
  passes its pre-publish check can be beaten by a poison that linearizes before its
  tail store — but such a published frame is never acted on. Two mechanisms, of
  which only the second is authoritative:
  - **(a) Producer pre-publish gate — best-effort early-out.** `Publish` (§8)
    re-loads `poison`/`shutdown` (seq_cst) **immediately before** the tail store; if
    either is set it calls `RollbackAdmit` and does **not** store the tail, so a
    producer that *observes* the fault before publishing never publishes. Poison
    can still win in the narrow window between that load and the tail store; in that
    case the frame **is** published (becomes visible). This gate reduces, but does
    not eliminate, post-poison publications — it is not a visibility barrier.
  - **(b) Consumer final pre-dispatch gate — the authoritative quarantine.** The
    consuming side re-loads `poison`/`shutdown` (seq_cst) **immediately before**
    `dispatch` (§9); if either is nonzero it stops, does **not** dispatch, and does
    **not** advance head. This is what actually prevents any post-poison frame — a
    raced request **or** a raced response — from being acted on. A dispatch already
    in flight when poison wins may run to completion, but the response it produces
    hits its **own** consumer final gate on the return direction and is likewise
    never dispatched; the call surfaces as `ErrOutcomeUnknown` (design §14/§17) —
    the honest outcome for "the handler may have run but the region died". The same
    gate covers the **graceful-shutdown** residual.
  - **`Signal` is NOT a visibility barrier.** By the time `Signal` (§12) runs, the
    tail store has already published the descriptor; `Signal` refusing to write the
    eventfd only suppresses a spurious wake. The consumer re-scans regardless, so
    correctness rests entirely on the consumer final pre-dispatch gate (b), never
    on `Signal`.
  - A producer that observes `poison != 0` after its tail store MUST NOT `Signal`
    and MUST NOT publish further; it proceeds to teardown.
- **No in-place repair.** There is **no** in-place repair path — by design
  (design §11). A poisoned region is discarded whole; recovery is a fresh region
  with an incremented generation (§15).

---

## §17 Compile-time layout assertions

The implementation MUST enforce this ABI's sizes, offsets, and alignments at
**compile time**, on **every** supported architecture (amd64 **and** arm64). The
assertions are architecture-independent in form (the ABI uses only fixed-width
types and explicit padding, §0), but MUST be **compiled and run in CI on both
GOARCH targets** so a toolchain or type-size surprise on arm64 is caught.

**Canonical idiom (REQUIRED form).** Go lacks a `static_assert`, so the
implementation MUST use the **constant out-of-range array index** pattern: index
a length-1 array literal by the constant `wantOffset − gotOffset` (or `wantSize −
gotSize`). The index is a compile-time constant; it is `0` (in range) exactly
when the invariant holds, and any nonzero value — including an unsigned
`uintptr` underflow when the actual value is smaller than expected — is a
compile-time "index out of range" error. Each assertion is a package-level
declaration:

```go
// Descriptor MUST be exactly 64 bytes (one cache line).
var _ = [1]byte{}[unsafe.Sizeof(Descriptor{})-64] // compiles iff Sizeof == 64

// Every §4 field offset (one line each).
var _ = [1]byte{}[unsafe.Offsetof(Descriptor{}.call_id)-0]
var _ = [1]byte{}[unsafe.Offsetof(Descriptor{}.service_id)-8]
// … through …
var _ = [1]byte{}[unsafe.Offsetof(Descriptor{}.reserved)-56]

// Alignment and fixed schema constants (NOT geometry values, which are config).
var _ = [1]byte{}[unsafe.Alignof(Descriptor{})-8]
var _ = [1]byte{}[DescriptorSize-64]
var _ = [1]byte{}[unsafe.Sizeof(SizeClassEntry{})-16]
// ring_capacity's power-of-two-in-[64,1<<20] rule is a STARTUP (runtime) check
// (§1), not a compile-time assertion — the value is host-chosen, not a constant.
```

Assertions split into two tiers per the schema-vs-values refactor (§0):

**(a) Structural assertions — ABI MUSTs, compile-time, both GOARCH.** These test
the frozen schema (offsets, widths, alignment, page/entry layout) and are
independent of the chosen geometry. Each is a separate declaration so the failing
one is named:

| Invariant | Assertion |
|---|---|
| `unsafe.Sizeof(Descriptor{}) == 64` | descriptor is one cache line (§4) |
| `unsafe.Offsetof(Descriptor.call_id) == 0` … through … `Offsetof(reserved) == 56` | every §4 field offset (all 11 fields, one assertion each) |
| `unsafe.Sizeof(Descriptor.reserved) == 8` (ends at 64) | §4 trailing reserved field |
| `unsafe.Alignof(Descriptor{}) == 8` | descriptor 8-byte aligned |
| `DescriptorSize == 64`, `PageSize == 4096`, `CacheLine == 64` | §1 fixed schema constants |
| sync-page field offsets (§3): `tail_hp==4096`, `head_hp==4160`, `tail_ph==4224`, `head_ph==4288`, `park_state_hp==4352`, `park_state_ph==4416`, `poison==4480`, `shutdown==4544` | one word per cache line (each 64 apart) |
| `(head_hp_off − tail_hp_off) == 64`, `(tail_ph_off − head_hp_off) == 64`, … through `shutdown` | false-sharing separation (§3) |
| layout-page header offsets (§2): `magic==0`, `layout_version==8`, `header_flags==12`, `generation==16`, `region_size==24`, `page_size==32`, `descriptor_size==36`, `ring_capacity==40`, `num_classes_hp==44`, `num_classes_ph==48`, `reserved_u32==52`, `sync_page_offset==56`, `ring_hp_offset==64`, `ring_ph_offset==72`, `arena_hp_offset==80`, `arena_ph_offset==88`, `arena_bytes_hp==96`, `arena_bytes_ph==104`, `class_table_hp_offset==112`, `class_table_ph_offset==120`, `shard_count==128`, `lifecycle_reserve==136`, `reserved_hdr==140` | every §2 header field |
| `unsafe.Sizeof(reserved_hdr) == 116` (header ends at 256) | §2 reserved header padding |
| `SizeClassEntry` size == 16, `Alignof == 4`; per entry `slab_size`@+0, `slab_count`@+4, `class_base_offset`@+8, `reserved`@+12 | §2 size-class entry schema |
| `LayoutPageSize == 4096` and `SyncPageSize == 4096` (both fixed pages, §1/§2/§3) | whole-page extents |
| sync-page reserved tail: bytes `[512, 4096)` in-page (absolute `[4608, 8192)`), a reserved span of 3584 bytes after `shutdown` (§3) | sync-page reserved tail |
| layout-page header region `[0, 256)` fixed; class tables + trailing reserved fill `[256, 4096)` | layout-page extent partition |

**(b) Geometry-value golden tests — per configured profile, NOT ABI MUSTs.** These
check the *values* a particular build selected (they vary by profile, so they are
golden tests, not `layout_version` invariants). A build configured for the
`benchmark` profile asserts `ring_capacity == 8192`, `arena_bytes_hp ==
arena_bytes_ph == 76021760`, `region_size == 153100288`, and the span offsets of
§1; a `default`-profile build asserts `ring_capacity == 4096`, `arena_bytes_dir ==
31719424`, `region_size == 63971328`. **Every** build additionally asserts the
*structural* geometry rules at startup (§1 Phase-2 / §2 validation): `ring_capacity`
a power of two in `[64,1<<20]`; **`0 < lifecycle_reserve (R) < ring_capacity`**;
**slab bounds** (each `slab_size` a multiple of 64 and `≥ 64`, strictly ascending,
`slab_size[last] ≥ 4096`, every `slab_count ≥ 1`); contiguous bases; **class-table
address checks** (each table offset `∈ [256,4096)`, 4-byte aligned, `offset +
16·num_classes ≤ 4096`, tables non-overlapping, validated before entry
dereference); **arena roundup/padding** (`arena_bytes_dir == roundup(class_total,
PageSize)`, pad range reserved-zero); per-arena `≤ 4 GiB−1`; total `≤ 1<<40`; spans
contiguous/page-aligned/non-overlapping.

The structural assertions (a) are `const` expressions over
`unsafe.Sizeof`/`unsafe.Offsetof` and are evaluated by the compiler on **each**
GOARCH; a mismatch on arm64 fails the arm64 build. The assertion file MUST be an
ordinary (non-`_test.go`, non-build-tag-gated) source file so it compiles for
every target, and CI MUST build for both `amd64` and `arm64`
(`GOARCH=amd64 go build ./...`, `GOARCH=arm64 go build ./...`). This satisfies the
design's "compile-time layout assertions per supported architecture" (design §13):
the **schema** is asserted at compile time on both arches; the **geometry values**
are golden-tested per profile and structurally validated at startup.

---

## §18 Capacity invariant formula

Admission control runs **before any resource is allocated** (design §19).
`internal/transport/shm` validates exactly two invariants at startup — **ring
deadlock-freedom** and **per-frame fit** (below) — and **refuses to load** if
either fails. It does **not** certify that the region can physically hold every
concurrently-admitted call: arena exhaustion — running out of free slabs of a
serving class at runtime — is **typed backpressure by design** (design §12/§19;
M2 plan Task 4, "exhaustion as typed backpressure"), **not** a safety violation.
`Admit` (§8) returns a backpressure result and the caller blocks until space or
receives `ErrBackpressure` (design §19). Avoiding exhaustion at a target load is a
**sizing** concern (a non-normative guideline, and an optional STRICT
certification below), not an ABI invariant.

Let, **per direction** (arenas and class tables are per-direction, §2/§6):

- `C = ring_capacity` (host-chosen; power of two in `[64,1<<20]`): the
  descriptor-slot bound (shared by both rings);
- `R = lifecycle_reserve` — read from the **layout page** (§2), `0 < R < C`,
  **RECOMMENDED default `R = C/16`** (= 256 at the `default` profile `C = 4096`,
  512 at the `benchmark` `C = 8192`): ring slots kept unavailable to data frames so
  the lifecycle lane (design §12/§19) always has room. This is a **starting**
  default, empirically tuned in Task 11; it is a scaling rule, not a magic
  constant. Both producers read the same `R` from the layout page, so the
  reservation is identical on both sides.
- `overhead = 32·[TRACE_PRESENT ∈ allowed_flags] + 4·[CRC32C_PRESENT ∈ allowed_flags]`:
  the **conservative** worst-case per-frame slab overhead, counted for exactly the
  negotiated payload-layout features. The 32-byte trace prefix is counted **iff the
  `trace` feature is negotiated** (`TRACE_PRESENT ∈ allowed_flags`, §5); the 4-byte
  CRC trailer is counted iff `checksum` is negotiated. `trace` is now a negotiated
  feature symmetric with `checksum`/`compression` — the `TRACE_PRESENT ∈
  allowed_flags` predicate is well-defined via the acknowledged feature tuple (§5),
  so there is no undefined "trace negotiated" state. When `trace` is **not**
  negotiated, small messages pay no trace tax and are not pushed into a larger
  serving class.
- `max_payload(dir) = slab_size[last](dir) − overhead`: the largest message payload
  the arena can physically carry. `slab_size[last] ≥ 4096` (§1) keeps this
  positive (`≥ 4096 − 36 = 4060`). A larger payload is rejected with a typed error
  (design §19).
- `serving_class(x, dir)` = the smallest class in `dir` with `slab_size ≥ x` (§6;
  no cross-class fallback); `count(c)` = that class's `slab_count` (minus the
  reserved slab-zero for class 0, §6).

### Two normative startup invariants

The transport (`internal/transport/shm`) MUST validate both at startup, per
direction, and **refuse to load** on failure:

- **(i) Deadlock-freedom (ring slots only):** `max_data_inflight ≤ C − R`. This is
  the *only* ring-capacity certification. It guarantees data admission stops at
  `C − R`, leaving `≥ R` ring slots reachable only by lifecycle frames.
- **(ii) Config sanity (arena fit):** for each direction, the host's declared
  `max_payload` satisfies `max_payload + overhead ≤ slab_size[last]` (equivalently
  `max_payload ≤ max_payload(dir)` above) — the largest admissible frame fits the
  largest class after overhead. Combined with the §1 `slab_size[last] ≥ 4096` rule,
  `max_payload` is always positive.

Neither invariant claims exhaustion-freedom (see the capacity model above).

**Boundary cases the transport MUST accept/reject (ring):** admit up to exactly
`C − R` data frames; reject the `(C − R + 1)`-th data frame even while the physical
ring still has `R` free slots; always admit a `CANCEL` / coalesced `STREAM_ACK`
while `depth < C`.

### Lane accounting and lifecycle-lane guarantees (normative)

`Admit`/`Publish`/`RollbackAdmit` (§8) keep process-local `data_inflight` /
`lifecycle_inflight` reconciled to one head snapshot. The equality
`data_inflight + lifecycle_inflight == depth ≤ C` holds **at the admission decision
point and at every post-reconcile stable point** (§8 consistency guarantee); it is
**not** continuously maintained across the whole enqueue — between `Admit`'s
reservation increment and `Publish`'s tail store the counters lead `depth` by one,
which is the reservation itself. Admission: a **data** frame iff
`data_inflight < C − R` **and** `depth < C` (and a slab is available); a
**lifecycle** frame iff `depth < C`.

- **(a) Progress guarantee (what actually holds).** Because data admission stops at
  `C − R`, at least `R` ring slots are reachable **only** by lifecycle frames — the
  lifecycle lane is **never starved by data**. Pending lifecycle intents drain
  through that `R`-slot window as the consumer advances; this is a *liveness under a
  live consumer* guarantee, **not** a claim that all pending lifecycle intents fit
  in `R` simultaneously. A **wedged consumer** stalls the whole ring; detecting and
  restarting it is the wedge-detection machinery's job (design §18), not this ABI's.
- **(b) Bounded memory (aggregate invariant).** At most **one outstanding lifecycle
  intent per in-flight call**: a `CANCEL` is terminal per call (the request state
  machine emits at most one, and a cancel for a not-yet-published call resolves
  locally with no frame, design §14); and `STREAM_ACK`, when streaming is activated,
  is bounded by that stream's own credit accounting (reserved wording — the exact
  bound lives in `stream-protocol.md`). Hence the in-process lifecycle-intent queue
  with capacity `max_data_inflight` suffices to hold every distinct pending intent
  without loss.

### Non-normative sizing guideline (avoiding arena backpressure)

To keep arena backpressure rare at a target concurrency, size each class so
`count(c) ≥ expected concurrent payloads served by class c` (per direction). This
is a **guideline for provisioning**, not a certification — a configuration that
violates it is still valid and simply experiences typed backpressure under load.

*Worked example (`default` profile, no features negotiated ⇒ overhead 0, guideline
figures).* `C = 4096`, `R = C/16 = 256` ⇒ deadlock-freedom caps
`max_data_inflight` at `C − R = 3840`. Class capacities (usable slabs):
64 B ×`(4096−1)=4095`, 4 KiB ×1024, 1 MiB ×26. With no payload-layout features
negotiated, a 64-byte message needs 64 B ⇒ served by the 64 B class (4095 slabs);
a ~1 MiB message ⇒ 26 slabs. **If `trace` is negotiated and a descriptor sets
`TRACE_PRESENT`**, that descriptor's 64-byte payload stores as `64 + 32 = 96` B ⇒ it
is served by the 4 KiB class (1024 slabs); a negotiated-but-untraced 64-byte message
— `trace` negotiated but `TRACE_PRESENT` not set on that descriptor — still stores
as 64 B and stays in the 64 B class, since negotiation only permits the bit rather
than setting it. So to serve, say, 1000 concurrent small calls without backpressure:
calls that don't set `TRACE_PRESENT` fit the 64 B class's 4095 slabs; calls that do
set `TRACE_PRESENT` consume the 4 KiB class's 1024 slabs. To serve more than 26 concurrent 1 MiB-class
calls, enlarge that class. The `benchmark` profile (`C = 8192`, `R = 512`, counts
{8192, 2048, 64}) raises all three.

### Optional STRICT mode (MAY, exhaustion-free certification)

A host **MAY** additionally certify, per direction,
`max_data_inflight ≤ min( C − R , min over reachable classes c of count(c) )`.
When this holds, **no** admitted data call can ever hit arena exhaustion (every
reachable class has at least `max_data_inflight` slabs), so arena backpressure is
impossible by construction. This is an **optional** stronger guarantee a
latency-sensitive deployment may opt into; it is a **MAY**, not a MUST, and does
not weaken the two normative invariants.

---

## §19 Versioning and change process

`layout_version` versions the region/ring/descriptor memory layout
**independently** of the protocol version and the feature set (design §10). It is
negotiated as one axis of the compatibility tuple and **matched exactly** (§0): a
memory layout cannot be partially compatible, so there is no `min..max` range for
`layout_version` — each side advertises the **set** of layout versions it can
speak, and negotiation selects the single highest version in the intersection (or
fails the handshake with `ErrIncompatible`).

### Additive vs. breaking changes

**Additive (does NOT bump `layout_version`).** A change that only consumes
already-reserved space, leaving every existing field at its existing offset,
width, and meaning:

- assigning a **reserved flag bit** (§5, bits 3..15) behind a negotiated feature
  flag;
- assigning a **reserved frame-kind** value (§5, 9..255) — subject to the frozen
  numbering (existing values never move);
- using a **reserved field** (`Descriptor.reserved` at offset 56, the layout-page
  `reserved_hdr`, the size-class entries' reserved `uint32`, or the sync page's
  reserved tail bytes);
- using `header_flags` bits.

Additive changes are gated by the **protocol version and feature-flag**
machinery (design §10), not by a layout bump, so two peers that agree on
`layout_version = 1` but differ in optional features interoperate on the shared
bytes.

**Reserved-zero is feature-scoped, not absolute (resolves the exact-attach
tension).** A reserved bit or field MUST be zero **unless its governing feature was
negotiated** in the acknowledged tuple. Attach's Phase-2 validation (§1) therefore
checks reserved-zero **only for features not in the acknowledged tuple**: an
un-negotiated reserved bit/field MUST be zero (fail-closed → `POISON_BAD_GEOMETRY`
for layout-page reserved fields, `POISON_BAD_FRAME` for reserved flag bits, §5
`allowed_flags`), while a reserved bit/field whose feature **was** negotiated is
permitted and interpreted per that feature. This is why attach is **structural**,
not exact-constant (§1): a future negotiated `layout_version = 1` feature that
consumes `header_flags`, `reserved_hdr`, a size-class entry's reserved word, or a
reserved flag bit is *not* rejected by a peer that negotiated it, yet is still
rejected (as it must be) by a peer that did not — no `layout_version` bump, no
contradiction.

**Breaking (REQUIRES a new `layout_version`).** Any change that moves, resizes,
re-types, or repurposes an existing field, or that changes region/page order or
sizes, including:

- changing `DescriptorSize`, any §4 field offset/width, or the descriptor's
  one-cache-line property;
- changing the sync-page geometry (§3): a field's offset, width, or cache-line
  assignment;
- changing the layout-page **header schema** (any field's offset/width/meaning) or
  the size-class **entry schema** (§2);
- changing the page/span **order** (§1), `PageSize`, or the fixed-page sizes;
- redefining an existing frame-kind value or flag-bit meaning;
- changing endianness (out of scope, §0).

**Not** breaking (configuration within the schema, §0): choosing a different
`ring_capacity`, per-direction size-class table (sizes/counts), arena byte totals,
`region_size`, or `R`; these vary per region and are validated structurally at
attach (§1), never versioned.

### Introducing a new `layout_version`

A breaking change ships as a **new document** (`shm-abi-v2.md`) defining
`layout_version = 2` in full; this document remains the frozen contract for
`layout_version = 1`. A host and plugin that both support versions {1, 2}
negotiate 2; a peer supporting only {1} negotiates 1; a peer supporting only {2}
against a peer supporting only {1} fails the handshake with `ErrIncompatible`
carrying both sides' offered sets (design §10). Because the version is
acknowledged **before any region is attached**, an untested version combination
can never map a region.

---

## Appendix A — Large messages (> `max_payload`) (non-normative)

`max_payload(dir)` is bounded by the largest configured size class (§18). Messages
larger than that are rejected in v1 with a typed error (design §19). Four future
paths exist; **v1 ships Path 1 only**. **Path 4 (size-based transport routing) is
the PREFERRED mechanism for M3 and supersedes Path 3 (spill)**, which stays
documented as an alternative. The relevant ABI extension points are preserved so
that adopting any path later needs no `layout_version` bump:

- **Path 1 — size a class for it (available now; just geometry).** Because size
  classes are host-configured (§2), a deployment that needs larger messages
  configures a larger top class (up to the per-arena `4 GiB − 1` bound, §1). No
  schema change, no negotiation — purely a `CreateRegion` geometry choice.
  *Extension point:* the geometry itself.
- **Path 2 — RPC-layer chunking over the reserved `STREAM_*` kinds.** Split a large
  message into a sequence sharing a call ID using the already-frozen streaming
  frame kinds (`STREAM_OPEN`=3 … `STREAM_ERR`=7, §5). This is additive RPC-runtime
  semantics layered on the existing descriptor path; it needs **no
  `layout_version` bump** (the kinds are already reserved and numbered). *Extension
  point:* the frozen `STREAM_*` kind values and the shared `call_id`.
- **Path 3 — out-of-band spill via a separate memfd over the control socket
  (alternative; superseded by Path 4).** For a truly large transfer, pass a
  dedicated sealed memfd out of band on the control plane (as hot-reload snapshots
  already do, design §9), referenced from the frame. The control plane is
  unconstrained by this ABI, so the region layout is untouched; a future negotiated
  **flag bit** (introduced safely through `allowed_flags`, §5) would mark "payload
  is a spill handle". *Extension point:* a reserved flag bit + the control-plane
  fd-passing channel. Retained as an alternative to Path 4 but not preferred.
- **Path 4 — size-based transport routing (PREFERRED for M3; supersedes Path 3).**
  A composite `Transport`, built **above** the `Transport` interface **entirely in
  `internal/rpcruntime`** — **no ABI hooks, no layout change, no descriptor/flag
  additions** — routes each message by size: messages `≤ shm_inline_max` go over
  the SHM transport (this ABI), larger messages are sent over the **UDS** transport
  on demand. This gives gRPC-like *transient* memory behaviour for rare giants (the
  UDS path allocates per-transfer and frees after, versus permanently sizing a huge
  top class into every region as Path 1 does) while keeping the common small-message
  fast path on SHM. Two thresholds govern it:
  - `shm_inline_max` — the **routing boundary**, equal to the largest slab's usable
    capacity (`max_payload(dir)`, §18); at or below it a message travels inline on
    SHM, above it the composite transport routes to UDS;
  - `max_payload` — the **absolute per-plugin ceiling**, enforced on the UDS path;
    a message above this is rejected with a typed error (design §19).
  Because Path 4 lives wholly in `rpcruntime` over the existing two transports, it
  needs **no** `layout_version` bump and no ABI extension point at all. *Extension
  point:* none in this ABI — it is an RPC-runtime composition of the SHM and UDS
  transports.

Mechanism trade-off (informational): **Path 4 is preferred** because it pays only
per actual large transfer (transient UDS memory) yet needs no ABI surface — it
supersedes Path 3's out-of-band spill, which required a negotiated flag bit and
frame-referenced handle. Path 1 (a large top class permanently sized into every
region) remains simplest when large messages are *common* rather than rare;
eqp-hub's bulk-read pattern (large, occasional device reads) is exactly the "rare
giant" shape Path 4 targets. This is guidance for the M3 decision, not a v1
commitment.

**RSS ratcheting (implementation note, not ABI).** Under the v1 seal set there is
**no in-place page reclaim** for a live region, and RSS ratchets **up** for the
region's lifetime: once a page is touched (an arena slab used), it stays resident
until the whole region is unmapped. The reason both candidate primitives are
unavailable:

- `MADV_FREE` is **not applicable** to a `MAP_SHARED` **file-backed** mapping (the
  region is a `memfd`, i.e. `tmpfs`-backed shared memory); `MADV_FREE` lazily frees
  only private anonymous pages, so it cannot reclaim shared arena pages.
- `fallocate(FALLOC_FL_PUNCH_HOLE)` **is forbidden by `F_SEAL_SHRINK`**: Linux
  treats hole-punching a sealed memfd as a shrink and returns `EPERM`. The v1 seal
  set (`F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL`, §1) therefore rules it out.

So the **only** reclaim for v1 is **region recycle**: teardown and a fresh region,
which the generation machinery (§15) already supports (a restart, hot-reload, or a
deliberate recycle discards the whole region and its RSS at once). A deployment
whose large-message bursts leave an unacceptable resident footprint should recycle
the region rather than expect in-place reclaim. Choosing a different seal set to
enable hole-punching would be a future, non-v1 change and is out of scope here.

---

*End of `layout_version = 1` ABI.*
