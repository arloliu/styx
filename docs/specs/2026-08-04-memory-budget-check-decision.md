# Startup memory-budget check for shared-memory regions

> **Outcome: not building the runtime enforcement.** The sizing half shipped —
> `ShmGeometry.RegionBytes` and the capacity-planning guide — and answers the
> question this note set out to answer for anyone who capacity-plans. The
> refusal half is dropped. What follows is kept as the record of why, because
> the reasons are specific to this codebase and a future attempt should start
> from them rather than rediscover them.
>
> **What killed it was ownership, not cost.** The check must know which regions
> the process currently holds. Three attempts at that accounting each failed on
> a different fact about what the code actually owns:
>
> - **A successful attach leaves two host mappings, not one.** `shm.Attach`
>   opens its own duplicate of the region fd while the host retains the original
>   (`internal/supervisor/supervisor.go:1688-1694`). `Transport.Close` returns
>   the duplicate's `Region.Close` result
>   (`internal/transport/shm/transport.go:1676-1691`), but `JoinGoroutines`
>   discards it (`internal/supervisor/supervisor.go:1038-1043`) and the
>   teardown's `Unmap` step covers only the original. Any accounting that
>   watches one mapping is watching half the commitment.
> - **Release is not observable at the point a charge would be settled.** On a
>   join timeout the teardown abandons the join goroutine and proceeds to unmap
>   the original and reap (`internal/lifecycle/teardown.go:115-140`), so the
>   original can unmap successfully — freeing the charge — while the duplicate is
>   still being closed. `Region.Close` also joins its `munmap` and fd-close
>   errors (`internal/shm/region.go:419-443`), so a non-nil error does not say
>   whether the mapping is gone.
> - **No existing object has the mapping's lifetime.** A `pluginRuntime`
>   outlives its mapping — a plugin that exhausts its restart budget has its
>   generation torn down while the runtime stays registered until `Stop` — and a
>   `TransportAuto` plugin that negotiates uds never had a mapping at all.
>
> Enforcing even the narrow claim that survived review — one `Host`, cgroup v2
> only, fails open on an unreadable limit, binds only limits observed at
> admission — would mean introducing a real mapping-lifetime lease across the
> transport, supervisor restart, reload, and shutdown paths. That is a larger
> change than the feature, for a guarantee that still cannot promise to prevent
> a cgroup OOM.
>
> **What remains true and useful:** `RegionBytes()` reports the exact per-plugin
> cost derived through the same code that lays out a real region, and
> `docs/configuration.md` works the arithmetic through for the default and lean
> profiles including the reload peak. An operator who reads it avoids the
> failure. An operator who does not still gets an OOM under load — that is the
> accepted residual, and the trigger to revisit is evidence of it happening in a
> real deployment despite the guidance.
>
> Everything below is the design as it stood when it was dropped, including the
> three rounds of narrowing. It is not an implementation plan.

Every claim is cited to source as of when it was written; line numbers will
have drifted. The symbol names and the quoted code are the load-bearing part,
not the numbers after the colon.

Part of this note's groundwork now exists. `ShmGeometry.RegionBytes` ships
(`geometry.go:113-142`), deriving through the same `shm.DeriveLayout` that
`CreateRegion` itself calls (`internal/shm/region.go:94-198`), so the size this
check reads and the size actually mapped cannot drift. Its agreement,
zero-value and invalid-geometry tests exist (`geometry_test.go:247-347`,
`internal/shm/region_test.go:292-386`), as does the capacity-planning guide with
the exact per-profile and reload figures (`docs/configuration.md:294-388`).
Those are prerequisites already met, not work this note still has to cost. What
remains unimplemented is the policy, the limit resolver, the Host accounting,
and the refusal itself.

## Invariant

**Stated as a decision rule, not as a continuous property.** Every operation
that can create a shared-memory region — a `Start`, or a `Reload` — decides
against the cgroup memory limit *that operation observed*, and refuses before
creating the region when the nominal sizes it would then hold exceed what that
observation allows. What it holds means the steady-state set plus one extra
region for each plugin whose reload is in flight.

The earlier phrasing — "at every moment of a `Host`'s life" — is withdrawn as
unachievable, and it is worth being exact about why, because the gap is not
closable by more careful implementation. `memory.max` is writable at runtime and
a process can be migrated into a stricter cgroup. A limit can therefore shrink
after a preflight and before the region that preflight admitted is created, or
shrink while the `Host` sits idle with no operation at which to notice. Only the
cgroup controller observes memory continuously; a library reading a file at
admission time cannot. A check that claimed otherwise would be claiming to
prevent an OOM it cannot see coming.

What the narrower rule still buys is the whole motivating failure: a
configuration whose regions cannot fit the limit *as it stands at startup* is
refused with an actionable error instead of being admitted and later OOM-killed
under load, once traffic faults in pages that were reserved but never touched.

Three scope boundaries are part of the rule, not caveats to it: it covers one
`Host` at a time (see "Why the `Host` is the right scope"); it is enforceable
only where a cgroup memory limit can actually be read (see "Reading the limit");
and it binds only limits observed at admission, never later administrative
changes. Outside those boundaries the check is skipped, never guessed.

## What was verified

**Regions are lazily faulted and never reclaimed.** `createSealedRegion` maps
the memfd with `unix.MAP_SHARED` and no `MAP_POPULATE`
(`internal/shm/region.go:194`); the attach path does the same
(`internal/shm/region.go:310`). Nothing walks the arena afterward:
`newClassState` builds each class's free list as process-local Go slices
(`make([]uint32, ...)`, `make([]uint64, sc.SlabCount)` at
`internal/arena/arena.go:377-393`), so no arena byte is written until a payload
is stored there. Pages are therefore charged as a slab is first used, and once
charged they stay charged — nothing in `internal/arena` returns pages to the
kernel; only `shmHostResources.closeRegion`
(`internal/supervisor/supervisor.go:1400`) frees anything, and that destroys
the whole region.

**Backpressure is denominated in slabs, not bytes.**
`ValidateStartupCapacity` (`internal/transport/shm/admission.go:131`) bounds
`max_data_inflight` against ring slots and, under STRICT, against per-class
usable slab counts. The set-aside path
(`writer.noteArenaSetAside`, `internal/transport/shm/writer.go:713`) fires when
a *size class* has no free slab. No path anywhere compares bytes to a limit.
`MetricArenaUtilization` (`observe/sink.go:30-32`) is the closest thing to a
byte gauge, and it reports *current occupied* bytes — it falls back toward
zero after a burst while the pages that burst faulted remain charged. So the
one existing byte-shaped signal is structurally incapable of showing the
high-water charge that gets the container killed.

**The exact numbers are already pinned in this repo.** `geometry_test.go:82-83`
declares `defaultLadderArenaBytes = 32731136` and `defaultLadderRegionSize =
65994752`, and `TestGeometryDefault_PinsTheSevenRungLadder_AndItsRegionCost`
asserts them against `region.Layout()` built through `toLayout` +
`shm.CreateRegion`. Recomputing the ABI formula independently
(`2*PageSize + 2*RingCapacity*DescriptorSize + arenaHP + arenaPH`, from
`deriveSpans`, `internal/shm/layout.go:411-444`) reproduces both figures
exactly. `GeometryLean` works out to 299008 arena bytes per direction and
671744 bytes of region.

So the real per-plugin default cost is **65,994,752 bytes (62.94 MiB)**, and
four plugins on the default profile is **251.75 MiB**, not the 62.43 MiB /
250 MiB that summing only the two arenas gives. The 62.43 MiB figure omits the
layout page, the sync page, and both descriptor rings (512 KiB of ring alone).
Any check must take the number from the derivation, never from an arena sum.

**A hot reload cannot change geometry.** `Host.Reload(ctx, name)`
(`reload.go:145`) takes only a name — there is no path to hand it a new
`PluginSpec`. Geometry is fixed at `NewHost` from `HostConfig.Plugins[i].Geometry`,
converted once per start in `supervisorConfig` (`host.go:669`,
`ShmLayout: spec.Geometry.toLayout()`), and stored immutably on
`supervisor.Config.ShmLayout` (`internal/supervisor/supervisor.go:330`). What a
reload *does* change is the region **count**: the transaction spawns a
successor and promotes it before reaping the predecessor
(`reload.go:105-139`, `internal/lifecycle/reload.go:302-337`), so one plugin
transiently holds two regions.

**Reloads of different plugins can overlap.** `Host.Reload` takes `h.mu` only to
look the runtime up and unlocks before calling `rt.sup.Reload(ctx)`
(`reload.go:151-166`). Nothing else serializes them, so N shared-memory plugins
reloading at once transiently hold 2N regions. This is the fact the earlier draft
of this note acknowledged and then declined to act on; see "The reload peak".

**A reload returns with exactly one region live for that plugin, on every
path.** On a pre-promote abort, `rollback` tears the successor down
synchronously before returning (`internal/lifecycle/rollback.go:41-46`). On
success, `tx.old.Teardown(...)` completes before the transaction returns
(`internal/lifecycle/reload.go:326-334`). And `Teardown.Run` calls `t.Unmap()`
— wired to `shmHostResources.closeRegion`
(`internal/supervisor/supervisor.go:1003`) — unconditionally after the bounded
join, returning the join error without skipping the unmap
(`internal/lifecycle/teardown.go:101-113`). So "the reload call has returned"
is a sound release point for a per-plugin charge; it does not depend on the
join succeeding.

**`HostConfig` is caller-owned mutable memory, not an immutable snapshot.**
`NewHost` stores the config by shallow assignment (`h := &Host{cfg: cfg, ...}`,
`host.go:432-441`). `HostConfig.Plugins` is a slice (`host.go:47`) and each
`ShmGeometry` holds two class-table slices (`geometry.go:29-36`). `h.mu` does
not protect the caller's backing arrays. `toLayout` copies the class elements
into fresh `[]shm.SizeClass` (`toSizeClasses`, `geometry.go:157-165`), but it
does so at `startOne` time — after any check that read the same fields. The
earlier claim that the verdict is a pure function of immutable state was wrong
as stated; see "Where the check runs" for the snapshot that makes it true.

**A process-global byte tally would be the `idregistry` mistake again — but
that argument does not carry as far as it was made to.** `idregistry.go` holds
`var idRegistry = newIdentityRegistry()` (line 8), a package-level singleton.
`TestRegisterIdentityName_FallsBackToHex_OnConflictingNames`
(`idregistry_test.go:67`) asserts at line 71 that the first registration
resolves before the collision — but the ID is permanently poisoned by the first
run (`register`'s `if r.poisoned[id] { return }`, `idregistry.go:83`), so the
second run's re-registration is a no-op. Confirmed by running it:

```
go test -count=2 -run TestRegisterIdentityName_FallsBackToHex_OnConflictingNames .
  idregistry_test.go:71: expected "first.Name", actual "0x5f78d094fba32b98"
```

The honest reading: `idRegistry` leaks across tests because its state is
*deliberately permanent*, not because it is a counter. A balanced
acquire/release tally would not inherit that. So testability is no longer the
argument against a process-global tally — the argument is what such a tally
would have to *do*, which is in "Why the `Host` is the right scope".

## Why the `Host` is the right scope

`shm.CreateRegion` has exactly one production caller:
`internal/supervisor/supervisor.go:1538`, inside `Supervisor.attachSHM`. Every
other call site is a test or test harness (`internal/transport/shm/chaos/harness.go:248`,
`internal/transport/shm/shmtest/shmtest.go:163`, plus `_test.go` files). Verified
by `grep -rn "CreateRegion(" --include=*.go .`. `shm.OpenRegion`'s production
callers are both in `internal/transport/shm/transport.go` (lines 143 and 444) —
the attach path, which maps an fd it was handed and never creates one.

The geometry that call uses comes only from `PluginSpec.Geometry` via
`supervisorConfig`. And the plugin set is closed for the Host's life —
`NewHost`'s own comment states it: *"The plugin count is fixed for the Host's
whole life as of this call — Start only starts the configured PluginSpecs, and
there is no API to add a plugin afterward"* (`host.go:420-424`). Plugin
subprocesses are spawned by the same Host through `startOne` → `supervisorConfig`
→ `Supervisor.Run`. So one Host's own config is a complete and static
description of *that Host's* demand.

**What one Host cannot see, stated exactly.** The earlier claim that "within one
container there is no arena consumer the Host does not already know about" is
false, and the counterexample is a normal lifecycle transition rather than an
exotic one:

- `Host.Stop` bounds only how long it *waits*. A plugin whose `Supervisor.Run`
  does not join inside `ctx` leaves Stop returning that plugin's deadline error
  while its runtime is retained and its teardown finishes detached
  (`host.go:858-869`, `retainUnjoined`/`watchJoin`, `host.go:945-989`).
  `Supervisor.Stop` returns `ctx.Err()` before `Run` joins
  (`internal/supervisor/supervisor.go:655-681`).
- A caller that builds a replacement `Host` on that error gets an independent
  config and runtime set (`host.go:415-472`). Its preflight sees only its own
  plugins, so the two Hosts' regions can overlap while each check passes.

This is a real gap and it is **accepted, documented, and not closed** by this
design. Three reasons:

1. The overlap is bounded, not permanent. The old instance's teardown always
   reaches `terminateAndReap` (it is in a `defer` that runs even through a
   panic or an abandoned join, `internal/lifecycle/teardown.go:95-113`), and the
   detached watcher completes the teardown when `Run` finally exits
   (`host.go:975-989`).
2. Closing it means gating a replacement Host's `Start` on state no Host owns —
   process-global commitments with leases released at
   `shmHostResources.closeRegion`. That turns a shutdown *timeout* into a
   startup *failure*, which is the availability-hostile direction this design
   has chosen against everywhere else (see the fail-open rule below). It also
   still cannot see a genuinely separate process sharing the cgroup, which is
   the case a process-global tally would exist for.
3. The operator already has the signal and the knob: `Stop` returned an error
   naming the plugin that did not join, and `MemoryBudgetReserve` is the
   allowance for everything the check cannot see — sidecars, unrelated
   processes, and a predecessor Host still tearing down.

The godoc for the budget fields must say this in one sentence: *the check covers
the plugins of the Host it runs on, at the moment it runs.* A design that
claimed more than that would be lying.

## Paths that create or destroy a region

Create:

1. `Supervisor.attachSHM` on first handshake of a generation
   (`internal/supervisor/supervisor.go:1538`), reached from `handshakeAndAttach`
   → `runOneInstance` → `Run`, i.e. once per `Host.Start` of an shm-transport
   plugin.
2. The same call on each crash-restart. `Run`'s loop is strictly sequential —
   `runOneInstance` tears the instance down (`cur.teardown(...)`,
   `internal/supervisor/supervisor.go:882`, whose `Unmap` step is
   `shmRes.closeRegion`) before returning, and only then does the loop take the
   next generation (`internal/supervisor/supervisor.go:591-599`). A restart
   therefore does **not** overlap two regions.
3. The same call for a reload successor, spawned while the predecessor is still
   live. This **does** overlap: the predecessor is reaped after the promote
   (`reload.go:110`, `internal/lifecycle/reload.go:315-334`).

Destroy: exactly one path — `lifecycle.Teardown`'s unmap step, wired to
`shmHostResources.closeRegion` (`internal/supervisor/supervisor.go:1003`,
`:1400`). It runs after `JoinGoroutines` released the transport's own duplicate
mapping, and it runs whether or not that join succeeded
(`internal/lifecycle/teardown.go:101-113`). There is no other release.

Two consequences for the arithmetic:

- Steady-state total = Σ over plugins that may map a region of that plugin's
  nominal region size. Peak = that, plus one extra region per concurrently
  reloading plugin. Both halves are now checked; see the next two sections.
- The host maps each region **twice** — the original from `CreateRegion` and a
  duplicate from `shmtransport.Attach` → `shm.OpenRegion`
  (`internal/supervisor/supervisor.go:1611-1618`, and the ownership contract
  spelled out at `:1385-1391`). Same memfd, same physical pages, so the cgroup
  charge is not doubled, but the host's own RSS counts both mappings. That
  compounds the OOM-victim-selection problem described below; it does not
  change the budget sum.

## Which specs count: shm and auto, not shm alone

`TransportAuto` is the zero value (`resolveTransport` maps `""` to it,
`host.go:1764-1769`; `PluginSpec.Transport`'s godoc says so at `host.go:138-141`)
and it offers shared memory *preferred* (`types.go:245-250`). Whether it actually
creates a region is only learned after handshake negotiation: `Supervisor.attach`
dispatches on the acknowledged tuple
(`internal/supervisor/supervisor.go:1417-1433`) and `attachSHM` then calls
`shm.CreateRegion` (`internal/supervisor/supervisor.go:1522-1541`).

An earlier version of this design put only explicit `TransportSHM` specs in the
mandatory sum. That is fatal: four default-everything plugins in a 100 MiB
cgroup would contribute **zero** bytes to the mandatory total, then negotiate
shared memory and map 263,979,008 bytes. The default check would pass the exact
failure the feature exists to catch.

**Decision: the mandatory sum covers every transport that may create a region —
`TransportSHM` and `TransportAuto` — and only an explicit `TransportUDS`
contributes zero.**

The cost, stated plainly: this refuses a configuration that would in fact have
negotiated uds and created nothing. That happens only when the plugin binary
cannot speak the shared-memory transport, which is exactly the case where the
caller can and should write `Transport: TransportUDS`. And the mandatory band
refuses only `Σ ≥ limit`, so a mixed deployment is refused only if the
*potential* regions alone would consume the whole cgroup — at which point the
caller either pins uds or is one plugin upgrade away from the real kill.

**There is a second cost, and it is a compatibility change rather than a
conservative refusal.** Geometry is validated today only on the negotiated
shared-memory path: transport dispatch sends a uds tuple straight to `attachUDS`,
while `attachSHM` validates capacity and creates the region
(`internal/supervisor/supervisor.go:1499-1515`,
`internal/supervisor/supervisor.go:1604-1623`). Sizing an `auto` spec before
negotiation means calling `RegionBytes`, which rejects an invalid geometry
(`geometry.go:113-142`). So an `auto` plugin whose binary is uds-only and whose
`Geometry` is malformed starts today — the geometry is simply never read — and
would fail `Start` afterwards, even on a host with memory to spare and even
though no region would ever have been created.

**Decision: accept it, and say so where callers will read it.** Once `auto` may
create a region, its geometry is configuration that must be valid, not a field
that happens to be ignored on one negotiated path; a geometry that is malformed
is a latent failure waiting for the first host whose plugin does speak shared
memory. The `PluginSpec.Geometry` and policy Godoc must state that an `auto` or
`shm` spec's geometry is validated at `Start` regardless of what is later
negotiated, and that an explicit `TransportUDS` spec's geometry remains unread.
This belongs in the release notes as a behavior change, not only in the design.

The alternative — reserve nominal bytes after the transport is negotiated but
before `CreateRegion`, aggregate transactionally, release on attach failure and
on region close — is rejected here. It moves the verdict from a config-time pure
function into `internal/supervisor`'s attach path, needs a cross-Host aggregator
to be worth anything (which is the process-global tally rejected above), and
turns a spawn into an operation that can fail on machine state. Its one genuine
advantage — never refusing a config that would have gone uds — is not worth that
machinery for a band that only fires on the arithmetically impossible.

## The reload peak

A reload spawns a successor and promotes it before reaping the predecessor, and
reloads of different plugins are not serialized (both verified above). So the
peak this Host can reach is:

```
peak = steady + Σ over plugins whose reload is in flight of that plugin's region
```

An earlier version of this note argued that refusing a tight reload is worse than
attempting it, because the overlap is brief. That argument is withdrawn. The
overlap is brief but the pages it faults are not free, and `memory.max` does not
grant a grace period for briefness: an OOM kill during a reload takes down the
host process and every other plugin with it, whereas a refused reload leaves the
predecessor serving. `Host.Reload` already has that exact shape in its contract —
*"On any pre-promote failure the reload has already rolled back … and Reload
returns the reason it aborted with that same instance still serving"*
(`reload.go:120-124`). Refusing before the spawn is the same outcome reached more
cheaply.

**Decision: charge the reload, do not serialize it.** Of the three ways to make
the invariant and the mechanism agree:

- *Budget the worst case at `Start`* (`2 × steady`) would refuse deployments that
  work and never reload. It doubles the required limit for everyone to cover an
  event most Hosts never trigger. Rejected.
- *Serialize reloads host-wide and budget `steady + max region` at `Start`*
  changes `Reload`'s concurrency for every user, makes an N-plugin rolling
  reload take N times as long, and still charges every deployment for a
  hypothetical. Rejected.
- *Acquire a charge before spawning the successor and refuse the reload if it
  does not fit* costs a `uint64` field on `Host`, is exact rather than
  worst-case, changes no existing behavior for a reload that fits, and has a
  release point already proven correct (the reload call returns with exactly one
  region live for that plugin, whatever the outcome). **Chosen.**

Mechanically, in `Host.Reload`, after the runtime lookup and before
`rt.sup.Reload(ctx)`:

- under `h.mu`, compute `steady + h.leakedBytes + h.reloadInFlightBytes +
  thisRuntime'sCommittedBytes` against a freshly read limit; if the policy
  refuses and the total meets or exceeds the limit, return without spawning
  anything;
- otherwise add that runtime's committed bytes to `h.reloadInFlightBytes` under
  the same lock;
- when `rt.sup.Reload` returns, settle the charge against the reload's unmap
  outcome: subtract it if the retired mapping is gone, or move it to
  `h.leakedBytes` if `munmap` failed (see "Releasing a charge means the mapping
  is gone"). Settling exactly once on every exit — rollback, promotion, panic,
  expired context — is the property to test, and it is keyed on the charge the
  call took, not on the runtime still existing, so a concurrent `Stop` cannot
  strand it.

The charged size is the runtime's committed bytes rather than the current
config's, so a caller editing `Geometry` between `Start` and `Reload` cannot
under-charge a successor for a predecessor that is still mapped at its original
size.

The limit is re-read at reload rather than cached from `Start`, because
`memory.max` is writable at runtime and a reload is rare enough that one file
read is free. An unreadable limit at reload time skips the gate, exactly as at
`Start`.

The refusal is a **retryable** condition, not a config error: the configuration
was fine at `Start` and will be fine again once the other reload finishes. It
therefore gets its own exported sentinel (`ErrMemoryBudget`) rather than reusing
`*ConfigError`/`ErrInvalidConfig`, whose whole meaning is "this value can never
be honored". One new exported sentinel is the minimum public cost of getting that
distinction right; a caller that treats them identically loses nothing, and a
caller that retries gets the retry it wants.

**Residual, stated honestly.** Under the default policy the gate fires only in
the arithmetically-impossible band, so a reload that merely makes the container
*tight* still proceeds. That is deliberate and consistent with the `Start`
verdict: the reserve band is the operator's number, and only
`MemoryBudgetRefuseReserve` enforces it.

## What owns a region's bytes

A per-`Start` snapshot is enough to make one call self-consistent and **not**
enough to keep the Host's total honest across calls. `Start` deliberately
supports retry after a partial failure, keeping the plugins that did start
(`host.go:542-553`, `host.go:623-628`), while `pluginRuntime` records the name
and supervisor but no region size (`host.go:427-449`). So: plugin A starts on
the default geometry, plugin B fails, the caller edits A to the lean profile,
and retries. The second snapshot budgets a lean A while A's original
65,994,752-byte region is still mapped. The Host would then admit a reload
against a steady total smaller than it actually owns.

The two available stories cannot both be true — "later edits are legitimate" and
"retry is identical and idempotent" — and the lifecycle already chose the first.

**Decision: bytes are owned by the runtime, not by the configuration.** When a
plugin is installed, the region size committed for it is recorded on its
`pluginRuntime`, taken from the same resolved geometry handed to its supervisor.
From then on:

- the steady sum is folded over **installed runtimes' committed bytes** plus the
  candidates this call may still start, never over the current config alone;
- a reload charges **the named runtime's committed bytes**, which is the size of
  the predecessor actually mapped, not whatever the caller's slice says now;
- a retry that re-reads an edited config changes only what it is about to start,
  and cannot retroactively re-price what is already live.

Duplicate names fall out of the same model: `Start` starts the first entry and
`runtimeFor` rejects the second (`host.go:511-518`, `host.go:543-554`), so only
one runtime is ever installed for a name and only one commitment exists.

The per-call snapshot is still taken — it keeps the check and the supervisors
built by that one `Start` reading identical geometry, which caller-owned mutable
slices otherwise do not guarantee (`host.go:47-50`, `geometry.go:18-36`,
`host.go:432-441`). It is a within-call consistency device, not the accounting.

## Releasing a charge means the mapping is gone

The reload charge is only sound if its release corresponds to a mapping that
actually went away. Today that fact is unobservable. `Region.Close` clears its
slice and then unmaps, counting the region closed **only** on a successful
`munmap` so a failure shows as a created/closed imbalance rather than a masked
equal count (`internal/shm/region.go:419-443`). It returns that error. But
`shmHostResources.closeRegion` discards it (`internal/supervisor/supervisor.go:1480-1487`),
`lifecycle.Teardown.Unmap` is a bare `func()` with nowhere to put it
(`internal/lifecycle/teardown.go:50-82`), and pre-promote rollback discards the
successor teardown error too (`internal/lifecycle/rollback.go:37-47`). A
`defer`-subtract after `rt.sup.Reload` returns therefore frees a charge whose
region may still be mapped — and since `Close` already cleared `r.data` and won
its CAS, nothing can retry that release.

In practice `munmap` on a valid mapping does not fail. That is an argument for
the state being rare, not for accounting that cannot represent it.

**Decision: make successful unmap the release boundary, and make failure
sticky.** The unmap seam returns an error, propagated through both successor
rollback and predecessor retirement. On success the charge is released. On
failure the bytes are moved to a Host-held leak total that is never released and
is counted by every later admission decision, so a region that leaked cannot be
spent twice. The failure is reported through `Logger`. This is deliberately
one-way: there is no correct way to reclaim bytes whose mapping the process can
no longer address.

## Atomicity

Two pieces of state, one primitive each:

- **The configuration one call reads.** The resolved snapshot above, taken and
  used under `h.mu`, which `Start` already holds across its whole loop
  (`host.go:499-518`). This replaces the earlier "no atomicity required, the
  config is immutable" claim, which was wrong: the config is caller-owned
  mutable slice memory (verified above).
- **The in-flight reload charge and the leak total.** `uint64` fields on `Host`,
  read-compare-add under `h.mu` in one critical section — a decide-and-commit,
  not a bare counter — with the charge released, or converted to leak, once the
  reload's unmap outcome is known. `h.mu` already serializes `Start` against
  `Reload` (`Start` holds it across its whole loop; `Reload` takes it before the
  lookup), so the steady sum and the charge cannot interleave.

The charge is taken under `h.mu` and released outside it, so the release must
tolerate a `Stop` that removed the runtime meanwhile: `Stop` marks names
stopping and drops runtimes under the same lock (`host.go:1170-1210`). Release
is keyed on the charge the reload took, not on the runtime still being present,
so a concurrent `Stop` cannot strand or double-free it.

There is no cross-process state and no shared counter in shared memory. That
absence is still the main argument for this shape over any check that must
observe another process's true current commitment: shared memory gives no way to
do that atomically, and a check that pretends otherwise is a check that lies.

## Reading the limit

cgroup v2 lives at the process's own cgroup, not at the mount root. This repo
already learned that the hard way and documented it: see the `cgroupRoot`
comment at `internal/event/cgroup.go:11-22` — *"cgroupRoot+\"/cpu.max\" does not
exist there at all (confirmed empirically)"*. Confirmed again for memory on this
machine:

```
/proc/self/cgroup                       -> 0::/user.slice/user-1000.slice/session-c2.scope
/sys/fs/cgroup/memory.max               -> No such file or directory
.../session-c2.scope/memory.max         -> max
.../session-c2.scope/memory.current     -> 25296326656
.../session-c2.scope/memory.stat        -> anon …, file …, shmem 152059904
/sys/fs/cgroup/user.slice/memory.max    -> max
```

So the resolution is the same ancestry walk `resolveCPUQuota` already performs
(`internal/event/cgroup.go:145-193`): read `/proc/self/cgroup` for the `0::`
line, walk from that path up to and including the mount root, take the
**minimum** finite `memory.max` found, treat `max` as unlimited at that level,
treat `ENOENT` as a benign absence, and retry `EINTR`.

**The result is a tri-state, not a `(bytes, ok)` pair.** Collapsing "proven
unlimited", "no controller", "malformed content" and "permission denied" into
one silent skip would make parser drift indistinguishable from a healthy
unlimited host. The classification mirrors `quotaClass`
(`internal/event/cgroup.go:26-50`):

- `Limited(bytes)` — **every** level in the ancestry resolved cleanly and at
  least one carried a finite `memory.max`; `bytes` is the minimum of those finite
  values. The check runs.
- `Unlimited` — every level resolved cleanly and none was finite (canonical
  `max`, or benign `ENOENT`). The check is skipped, and this is the *expected*
  outcome on a developer machine.
- `Unknown(reason)` — any level failed to resolve: a real read error, content
  matching neither form, an unresolvable `/proc/self/cgroup`, or no `0::` line
  at all. The check is skipped and the reason is reported through `Logger`.

**A partial read is `Unknown`, not `Limited`.** These conditions would otherwise
overlap, and the overlap is not academic: a readable 200 MiB leaf under an
unreadable parent whose real limit is 100 MiB yields a minimum-over-readable of
200 MiB, which is not a limit anything actually enforces. Enforcing against it
refuses configurations that fit and admits configurations that do not. The CPU
resolver already faces this and answers it with a separate `exact` bit that its
exported certification requires (`internal/event/cgroup.go:117-194`,
`internal/event/cgroup.go:78-85`); this consumer folds the same distinction into
`Unknown` because a fail-open consumer has no use for an inexact number it is
not allowed to enforce. When several levels fail, every reason is preserved so
the log names all of them rather than the first.

`ENOENT` stays the one documented benign absence — a level that has no memory
controller is not a level that failed to answer.

**cgroup v1: not supported, fails open, and says so.** A v1-only host has
numbered controller lines and no `0::` line, which `parseOwnCgroupPath` already
classifies as `ownPathAbsent` (`internal/event/cgroup.go:270-310`). The memory
resolver maps that to `Unknown("no cgroup v2 unified hierarchy")` and skips.

The earlier proposal — read `/sys/fs/cgroup/memory/memory.limit_in_bytes` — was
wrong: it reads the hierarchy *root*, which the kernel documents as unlimitable,
while the process's real limit lives on its own directory or an ancestor. A
correct v1 resolver has to parse the numbered `/proc/self/cgroup` entry carrying
the `memory` controller, find that hierarchy's mount in `/proc/self/mountinfo`
(handling co-mounted controllers such as `memory,cpu` and non-default mount
roots, which containers routinely have), translate the mount root against the
process's path, and then walk that. That is more subtle code than the whole rest
of this feature, it cannot be verified on this machine (v2-only), and no test in
this repo would ever run against a real v1 host. Given that modern container
runtimes are overwhelmingly v2, the honest trade is to claim nothing on v1 rather
than to ship an unverified resolver whose failure mode is a *wrong number* rather
than a skip. If a v1 deployment ever matters, this is an additive change: one
more branch producing `Limited`, with everything downstream unchanged.

The `> 1 TiB means unlimited` rule from the earlier draft is **removed
entirely**. `maxRegionSize = 1 << 40` (`internal/shm/layout.go:171`) is Styx's
own region-size ceiling and has nothing to do with any cgroup encoding; a real
2 TiB limit is finite, checkable, and can absolutely be overcommitted. There is
no sentinel to recognize on v2 — `max` is a literal keyword — and v1's sentinel
question no longer arises because v1 is not supported.

**This contradicts shipped documentation, and that documentation is what gives
way.** `docs/configuration.md` currently states that a future enforcing check
"must support cgroup v1 as well as v2 — that is a stated requirement for that
future work, not an open question to revisit." That sentence was written before
anyone costed a correct v1 resolver. Having costed it, the requirement is
withdrawn: an unverified resolver that reads the unlimitable hierarchy root
produces a wrong number, and a wrong number is worse than a skip for a check
whose only job is to refuse configurations that cannot work. Amending that
paragraph — to say the check reads cgroup v2 and fails open everywhere else,
with the reason — is part of this feature's work and lands in the same change,
so the published text never describes a guarantee the shipped code does not
make.

**Direction of the fail-safe.** It is the **opposite** of
`internal/event/cgroup.go`'s. That code fails *closed* (shrink the spin budget
when unsure) because being wrong costs a little CPU. Here, being wrong costs a
refused start, so it fails *open*: a dev laptop, a Mac in a Linux VM with an
unusual mount, bare metal, and a restricted container view all skip the check. A
library that refuses to start because it could not read a file it does not own is
worse than the bug it is preventing. This must be said explicitly wherever the
code lands, because the two files will otherwise read as inconsistent.

## The runtime allowance — do not estimate it

The check compares `Σ regions` against a limit that must also hold the host's Go
runtime and one Go runtime per plugin process. Three options:

1. **Estimate it.** Rejected. There is no defensible number: it depends on
   `GOGC`/`GOMEMLIMIT`, the plugin's own workload heap, the host application's
   heap (Styx is a library inside someone else's process), and the arena's own
   process-local bookkeeping. That bookkeeping alone is computable — see below —
   but it is a rounding error next to a Go heap. A guessed constant would decide
   the verdict while being unvalidatable by anyone. An arbitrary fudge factor
   that produces false alarms is worse than no check.
2. **Make it configurable, no default.** Recommended.
3. **Exclude it entirely.** Also recommended — as the *default behavior*, which
   falls out of (2) when the knob is unset.

**The arena's process-local bookkeeping, computed exactly.** `newTransport`
builds an `arena.Arena` for the endpoint's **outbound direction only**
(`internal/transport/shm/transport.go:513-515`); the inbound direction keeps just
a byte span and a decoded class table (`inboundArenaBytes`, `inboundClasses`,
`internal/transport/shm/transport.go:568-578`). Per endpoint, then, one arena:
`newClassState` allocates a free list of `SlabCount - first` `uint32`s (`first`
is 1 for class 0, whose slab 0 is reserved) and a `liveSeq` of `SlabCount`
`uint64`s (`internal/arena/arena.go:381-393`, called with `i == 0` as
`reserveZero` at `:161`). For the default ladder's 7592 slabs that is
`4 × 7591 + 8 × 7592 = 91,100` bytes — about **89 KiB per endpoint**, and about
**178 KiB across the host and plugin endpoints together**, before allocator size
rounding. The earlier figure of ~182 KiB per attached side double-counted: it
charged both directions to each endpoint.

The check therefore has two verdicts with very different epistemic status:

- **Arithmetically impossible**: `Σ regions ≥ limit`. No traffic pattern makes
  this work, because the Go runtimes need strictly more than zero bytes. This is
  provable with no estimate at all.
- **Tight**: `Σ regions + Reserve > limit`, where `Reserve` is supplied by the
  operator. Zero (the default) means this band coincides with `Σ > limit` and
  adds nothing beyond the impossible verdict.

**Arithmetic, specified.** All of it saturating, never subtractive:

- `satAdd(a, b) = MaxUint64 if a+b < a, else a+b`.
- `steady = satAdd` fold over per-plugin region bytes. (Overflow needs ~2^24
  plugins at the 1 TiB region ceiling; the fold is checked anyway because it
  costs one comparison and the alternative is a silent wrap.)
- Impossible verdict: `steady >= limit`. Equality refuses — two Go runtimes
  cannot live in zero bytes.
- Reserve verdict: `satAdd(steady, Reserve) > limit`. Never `limit - Reserve`,
  which underflows for `Reserve > limit` into a near-`MaxUint64` allowance that
  would accept precisely the configuration most deserving of refusal. With
  saturating addition, `Reserve >= limit` makes the reserve verdict fire for any
  non-zero `steady`, which is the correct reading of "my runtime needs the whole
  container".
- Reload gate: `satAdd(satAdd(steady, inFlight), thisRegion) >= limit` refuses,
  and under `MemoryBudgetRefuseReserve` the same sum plus `Reserve` compared
  with `>`.

## Where the code lives

`internal/` — `100-project-map.md` makes the root `styx` package the only public
import, and a cgroup filesystem reader is exactly the kind of platform detail
that belongs below it.

**Package**: a new leaf, `internal/cgroupmem`. Not `internal/event`: that
package is the eventfd/spin-park waiter, and its cgroup file is there only
because the spin policy needed a CPU quota. Putting a memory limit reader there
would make `internal/event` the de facto cgroup package by accident.

**Seam**: copy the shape `internal/event/cgroup.go` already proves out —
an exported entry point delegating to a reader-injected core:

```go
// Limit resolves this process's effective cgroup v2 memory limit …
func Limit() Result { return limitVia(cgroupfs.ReadNoFail) }

// limitVia is the reader-injected core of Limit, factored out so the ancestry
// walk and the classification are testable against a synthetic
// path -> (content, err) map without touching the real cgroup filesystem.
func limitVia(read cgroupfs.Reader) Result

// Result is the tri-state above: Class (Limited/Unlimited/Unknown), Bytes
// (meaningful only for Limited), and Reason (a short, loggable phrase,
// non-empty only for Unknown).
```

`internal/event/cgroup_test.go:207-212` already defines a `fakeCgroupFS` of
exactly this shape; the new package's tests mirror it.

**Sharing with `internal/event`: the pure primitives only.** The ancestry walk,
`/proc/self/cgroup` parsing, the `EINTR` retry and the full-file reader would
otherwise exist in two copies that can drift — roughly 60 lines of subtle,
already-battle-tested logic. The full consolidation (mount discovery, walk, and
raw classification all in a shared layer, with each consumer applying its own
fail direction) is more than this feature needs and touches decision logic whose
fail-closed semantics are load-bearing for the spin policy.

The narrower alternative adopted here: extract only the four pieces that contain
no policy — `readFileNoFail`, `readRetryEINTR`, `parseOwnCgroupPath` with its
three-way kind, and `parentCgroupPath` — into `internal/cgroupfs`, and have
`internal/event/cgroup.go` call them. The move is mechanical, the classification
and fail direction stay entirely with each consumer, and `internal/event`'s
existing test corpus is the regression net. The ancestry walk itself is *not*
shared: the CPU walk computes a minimum ratio with an `exact` taint and the
memory walk computes a minimum byte count with a reason, and forcing one
signature over both would be a worse abstraction than two twenty-line loops.

**This is scope growth and it should be counted as such.** The feature as
originally pitched touched no existing package. It now moves four functions out
of a working file and relocates their tests. That is the price of not shipping a
second, drifting copy of the parsing that this repo already got wrong once.

**Public surface in `styx`**: three additions.

```go
// RegionBytes reports the exact size, in bytes, of the shared-memory region
// this geometry produces — the same number internal region creation derives,
// obtained without creating a region, mapping anything, or opening a file
// descriptor. It is the memory a plugin using this geometry will eventually
// charge to this process's memory cgroup: the region is mapped MAP_SHARED
// with no MAP_POPULATE and faults in lazily, so the charge appears as slabs
// are first used rather than at start, and no path ever returns those pages
// while the plugin is running.
//
// The zero ShmGeometry reports the default profile's size, matching the
// zero-value rule everywhere else on this type. An empty direction reports
// the size it takes from the other direction, again matching how the region
// is actually built.
//
// It returns an error, wrapping ErrInvalidConfig, for a geometry that could
// not produce a region at all — a ring capacity outside its bounds, a
// non-ascending size-class table, a total past the region ceiling. The error
// is the same rejection a plugin start would report, surfaced here without
// having to start one.
func (g ShmGeometry) RegionBytes() (uint64, error)
```

```go
// MemoryBudgetPolicy selects what Start and Reload do when the shared-memory
// regions this Host's plugins will map do not fit the memory limit of the
// cgroup this process runs in. The check is skipped entirely, whatever the
// policy, when no cgroup memory limit can be read — a development machine,
// bare metal, cgroup v1, or a container whose cgroup files this process
// cannot see.
//
// Whatever the policy, the check covers the plugins of the Host it runs on,
// at the moment it runs. It cannot see another Host in this process, a
// predecessor Host still tearing down after a Stop that timed out, or any
// other tenant of the cgroup; MemoryBudgetReserve is the allowance for those.
type MemoryBudgetPolicy int

const (
    // MemoryBudgetRefuseImpossible refuses a configuration that cannot work
    // arithmetically — the regions alone meet or exceed the whole limit, so no
    // traffic pattern leaves room for even one Go runtime — and reports a
    // configuration that merely eats into MemoryBudgetReserve through Logger.
    // It refuses a reload on the same arithmetic, leaving the running instance
    // serving. This is the zero value.
    //
    // It refuses only what is provable without estimating anything. The
    // regions' size is exact (ShmGeometry.RegionBytes); the limit is read from
    // the cgroup; nothing else enters the comparison. A deployment that is
    // over-provisioned on paper but fine in practice is not refused by it,
    // because "fine in practice" still requires the regions to fit.
    MemoryBudgetRefuseImpossible MemoryBudgetPolicy = iota

    // MemoryBudgetRefuseReserve additionally refuses a configuration, or a
    // reload, that eats into MemoryBudgetReserve, rather than only logging it.
    // Choose it once you have measured your own runtime footprint and set
    // MemoryBudgetReserve from that measurement; it enforces a number this
    // package cannot check.
    MemoryBudgetRefuseReserve

    // MemoryBudgetWarn reports every finding through Logger and never fails
    // Start or Reload. Useful while migrating an existing deployment, where the
    // honest first step is to learn whether the limit is wrong rather than to
    // stop shipping. Note that a Host with no Logger configured reports
    // nowhere: this setting is a deliberate choice to be told nothing unless
    // you are listening.
    MemoryBudgetWarn

    // MemoryBudgetOff performs no check and reads no cgroup file. For a
    // deployment whose container runtime reports a limit that is not the one
    // actually enforced, where the check would refuse a configuration that
    // works.
    MemoryBudgetOff
)
```

```go
// MemoryBudgetPolicy selects Start's and Reload's response when the configured
// plugins' shared-memory regions do not fit this process's cgroup memory limit.
// The zero value refuses only the arithmetically impossible case.
MemoryBudgetPolicy MemoryBudgetPolicy

// MemoryBudgetReserve is the bytes this deployment needs for everything that
// is not a shared-memory region this Host maps: this process's Go heap, one Go
// runtime per plugin process, any other tenant of the cgroup, and any
// predecessor Host still tearing down. The budget check treats the regions
// plus MemoryBudgetReserve as what must fit the limit.
//
// Zero (the default) disables the reserve band, leaving only the check that
// needs no estimate: regions alone against the whole limit. There is no
// default value on purpose — a guessed allowance would decide whether your
// Host starts, using a number neither you nor this package can validate, and
// a check that cries wolf is worse than one that stays quiet. Set it from a
// measurement of your own container at rest, not from a rule of thumb.
MemoryBudgetReserve uint64
```

Plus one exported sentinel, `ErrMemoryBudget`, and one error type carrying the
numbers behind it.

**Both refusals use it — `Start` and `Reload` alike.** An earlier version of
this note returned `*ConfigError` from `Start` and `ErrMemoryBudget` from
`Reload`. That split is wrong, and the reason matters: `*ConfigError` means a
value that can never be honored (`errors.go:189-213`, `errors.go:304-308`),
while a budget shortfall depends on machine state. The identical `HostConfig`
succeeds after an operator raises `memory.max`, after a concurrent reload
finishes, or on a larger node. Classifying it by *which method noticed* would
tell a caller to give up at `Start` and retry at `Reload` for the same cause.

So one contract for both, with the totals structured rather than formatted into
a message: the observed limit, the steady bytes, in-flight reload bytes, any
leak total, the reserve, the policy, and the per-plugin breakdown — reachable
via `errors.As`, matching `ErrMemoryBudget` via `errors.Is`.

`*ConfigError` keeps what it is actually for: a `MemoryBudgetPolicy` outside its
range, or a geometry `RegionBytes` rejects. Those are rejected before any cgroup
read or spawn.

`ErrMemoryBudget` stays outside `IsRetryable`, alongside the other lifecycle
sentinels rather than the call-error classifier (`errors.go:336-389`,
`errors.go:416-446`) — it is retryable by an operator changing something, not by
a caller re-sending the same request into the same conditions.

## Where the check runs

**Not `NewHost` — it cannot fail.** `func NewHost(cfg HostConfig) *Host`
(`host.go:415`) returns no error. Giving it one is a breaking change to the
first line of every user's program, for a check that has a perfectly good home
one call later.

The check runs **once at the top of `Host.Start`**, under `h.mu`, before the
`for _, spec := range h.cfg.Plugins` loop (`host.go:508-516`) — so before
`startOne` does any binary hashing, spawning, or handshaking, and therefore
before any process exists to leak.

**`Start` takes a resolved snapshot first.** Under the same `h.mu`, `Start`
copies `h.cfg.Plugins` and each spec's two geometry class tables into
Host-private memory, runs the check against that snapshot, and iterates
`startOne` over *that snapshot* rather than over `h.cfg.Plugins`. Without it, a
caller mutating the slice it passed to `NewHost` — legal today, since `NewHost`
stores it by shallow assignment (`host.go:432-441`) — can make the checked
geometry and the created geometry differ. The snapshot is ~10 lines inside
`Start` and needs no API change.

Two limits of that snapshot, stated rather than glossed: a caller mutating the
config *concurrently with* `Start` still has a data race in their own program,
which no amount of copying fixes; and the guarantee delivered is that the verdict
and the regions agree *within one `Start`*, not that `HostConfig` becomes
immutable. Deep-copying at `NewHost` instead would additionally freeze the config
for the Host's whole life, but it would also silently change what a second
`Start` observes after a legitimate caller edit — a bigger behavioral change than
this feature needs, so it is not taken.

The verdict is then a pure function of that snapshot, so a retried `Start`
(which the godoc explicitly supports, `host.go:483-491`) reports identically and
idempotently. Note that the sum covers every *configured* plugin, including ones
already started — which is correct, because their regions are still mapped. On
the impossible verdict, `Start` returns the `ErrMemoryBudget` error immediately and
starts nothing; folding it into the `errors.Join` of per-plugin failures would
be wrong, because the finding is about the set, not about any one plugin.

**Duplicate plugin names** are counted once per config entry, which over-counts:
the second entry is refused by `startOne` with `ErrPluginAlreadyStarted`
(`host.go:543-554`) and never maps a region. The over-count is conservative in
the safe direction but can refuse a configuration that would half-work. This
design does **not** add a duplicate-name config error — that is a separate
decision about `HostConfig` validation with its own compatibility question — and
instead pins the behavior with a test so it is chosen rather than accidental.

**Reload** runs the charge gate described above, using a freshly read limit and
the same snapshot-derived steady sum. Geometry cannot change between `Start` and
a reload (verified above), so the successor's size is exactly the predecessor's.

**Start's laziness** is already covered: `Start` starts every configured plugin
in one call, and the check precedes the whole loop. A partial `Start` failure
leaves *fewer* regions than the check assumed, never more.

## The dynamic check — recommend not building it

The second proposed check (read `memory.current` and `memory.max` at each region
creation; refuse if this region's nominal size would not fit in what remains)
should not be built as specified. Two reasons, and they point in opposite
directions:

**It is systematically optimistic about the thing that matters.** At the moment
it runs, every already-created region contributes approximately zero to
`memory.current`, because the pages are not faulted yet — that is the entire bug.
So the "remaining headroom" it computes is inflated by exactly the quantity the
static check knows exactly. It would pass, cheerfully, on the four-plugins-in-a-
100 MiB-container configuration this whole note exists for.

**It is systematically pessimistic about things that do not matter.**
`memory.current` includes the page cache. On this machine, at this moment, the
same cgroup reports `file 17073819648` — about 17 GiB of reclaimable file pages
against `anon 5885169664`. A container with a warm page cache routinely sits near
its limit in `memory.current` while being under no pressure at all, because the
kernel reclaims file pages on demand. Refusing to create a region on that basis
would fire constantly, in exactly the healthy case. Subtracting the reclaimable
components from `memory.stat` is not a fix — there is no reliable "what the
kernel will actually reclaim" number, and getting it wrong reintroduces the false
alarms.

It also costs what the static check does not: it reads two files on the spawn
path, it makes region creation's outcome depend on machine state, and it makes
its own failure mode (a refused region) indistinguishable from the failures
`attachSHM` already reports.

**What survives.** The measurement is worth taking, just not as a gate. When the
static check fires, include the live figures in the message: `memory.current`,
`memory.stat`'s `shmem`, and the limit. That turns "your config needs 251.75 MiB
and the limit is 100 MiB" into something an operator can act on immediately, at
zero false-alarm risk, because the numbers are reported rather than compared. A
failure to read those figures must degrade the *message*, never the verdict.

## Test plan

Written against current source. Seams that exist:

- **`RegionBytes` agreement.** `geometry_test.go:99` already builds a region via
  `geo.toLayout()` + `shm.CreateRegion` and reads `region.Layout().RegionSize`.
  A test asserting `RegionBytes() == region.Layout().RegionSize` for the default
  profile, the lean profile, and a custom geometry is writable today, against the
  constants at `geometry_test.go:82-83`. This is the test that makes the number
  exact rather than an estimate; it must compare against `CreateRegion`'s output,
  not against a recomputed formula, or it only tests that arithmetic is
  deterministic.
- **Zero-value and partial-zero rules.** `toLayout` (`geometry.go:123-155`)
  handles the fully-zero geometry and the "custom ring geometry, both class
  tables empty" case; `geometry_test.go:49` already drives `toLayout` through
  table cases. `RegionBytes` must route through `toLayout`, and a test must pin
  `ShmGeometry{}.RegionBytes() == GeometryDefault().RegionBytes()` and
  `ShmGeometry{RingCapacity: 512, LifecycleReserve: 32}.RegionBytes()` picking up
  the default ladder — otherwise a caller sizing a container from the zero value
  gets a number the runtime does not honor.
- **Invalid geometry.** `RegionBytes` on a non-ascending class table, a
  non-power-of-two ring capacity, and a table whose total exceeds `maxArenaBytes`
  must return the same rejections `CreateRegion` gives. `internal/shm`'s
  `ErrBadGeometry` paths (`internal/shm/layout.go:296-345`) are already covered
  package-internally; this pins the public projection of them.
- **cgroup resolution.** The synthetic `path -> (content, err)` map at
  `internal/event/cgroup_test.go:207-212` is the model. New tests drive
  `limitVia` over: a v2 ancestry with a finite `memory.max` at an intermediate
  level only; `max` at every level; `max` at the leaf with a finite ancestor;
  finite limits at several ancestors where the strictest is *not* the nearest;
  `ENOENT` at intermediate levels; no memory controller anywhere; an unreadable
  ancestry level; a permission error; an `EINTR` that must be retried rather
  than counted as unreadable; a malformed value; and a v1-shaped file with
  numbered controller lines and no `0::` line. Each case asserts the *class* and,
  for `Unknown`, that a reason is present — not merely that the check was
  skipped. All without touching the real filesystem.
- **Shared-parsing regression.** After the `internal/cgroupfs` extraction,
  `internal/event`'s existing cgroup test corpus must pass unchanged. That is the
  whole safety argument for doing the move at all, so it is a required gate, not
  a nice-to-have.
- **On-host sanity.** `TestOwnCgroupPath_FindsExistingReadableCPUMax_OnThisHost`
  (`internal/event/cgroup_test.go:178`) is the precedent for one test that
  asserts against the real machine. The memory equivalent must assert only that
  resolution *terminates and classifies*, never that a particular limit exists —
  CI containers and developer laptops differ, and this machine has no memory
  limit at all.
- **Warn delivery.** `observe.Logger` (`observe/logger.go:12-21`) is a small
  interface; `observability_test.go` already exercises fake sinks and loggers
  through the same dispatcher.

Seams that must be **built**:

1. **An injectable unmap failure at the `Region.Close` seam.** Without it, the
   leak path is unreachable in a test and the charge-settling logic ships
   unexercised. Needed for both pre-promote rollback and post-promote
   predecessor retirement.

   The shared region-size derivation this plan once had to build already exists
   (`geometry.go:113-142`, `internal/shm/region.go:94-198`), with its agreement
   corpus (`geometry_test.go:247-347`, `internal/shm/region_test.go:292-386`).
   Keep that corpus as a required regression gate; do not rewrite it.
2. **Limit injection into the `styx` package.** Tests must never depend on the
   machine's real cgroup. Add a package-level `var cgroupMemoryLimit = cgroupmem.Limit`
   in `styx`, overridden from `export_test.go` with a set-and-restore helper —
   the pattern `SetPluginAttachSHMFailAtForTest` already uses
   (`export_test.go:330-339`), which returns a restore func rather than relying
   on the test to remember. The seam must allow a *different* result on a second
   call, so a test can change the limit between `Start` and `Reload`.
3. **A no-spawn assertion.** The impossible-verdict test must prove `Start`
   refused *before* spawning. With the check at the top of `Start`, a spec whose
   `Path` does not exist is enough: assert `errors.Is(err, ErrInvalidConfig)`
   and specifically **not** the binary-not-found error `startOne` would produce.
   A test that only asserts "Start returned an error" passes whichever check
   fired, and is the kind of assertion that verifies less than it appears to.
4. **Direct access to the reload charge.** An `export_test.go` helper that
   acquires and releases a named plugin's reload charge on a `Host` lets the gate
   arithmetic be tested deterministically — two charges of *different* sizes held
   at once, a third refused, each released — without racing two real reload
   transactions. One slower end-to-end test then pins the wiring: that
   `Host.Reload` actually takes the charge and actually releases it on both the
   success and the rollback path.
5. **Negotiated-transport visibility — deliberately not built.** Nothing on
   `Host` today reports which transport a running instance negotiated (grep for
   an accessor finds none; the value lives in `Supervisor.attach`'s dispatch,
   `internal/supervisor/supervisor.go:1417-1433`). With it, the reload gate could
   charge zero for an instance that actually went uds instead of charging the
   `auto` upper bound. Without it, the gate is conservative in the same direction
   as `Start`. Listed because it is the one seam that would make this design
   tighter, and its absence is a known, accepted imprecision rather than an
   oversight.

Cases the tests must cover, beyond the seams:

1. **All-zero-value transport.** Four specs with default everything against a
   100 MiB limit: `Start` must return an `ErrMemoryBudget` error and spawn
   nothing. This is
   the motivating failure, and it is the test that would have caught the earlier
   design's exclusion of `auto`.
2. **A transport mix.** Two `TransportSHM`, one `TransportAuto`, one
   `TransportUDS`, against a limit between the shm-only sum and the shm+auto sum.
   The mandatory verdict **must** fire, because `auto` participates; the uds spec
   must contribute zero. The complementary case — the same four specs with the
   `auto` one pinned to `TransportUDS` — must start. Without both, the sum could
   drop `resolveTransport` and stay green.
3. **A reserve-band case with `MemoryBudgetReserve` set**, asserting
   `MemoryBudgetRefuseImpossible` logs it and `MemoryBudgetRefuseReserve`
   refuses it — two policies, one configuration, different outcomes. Asserting
   only one of them lets the policy field be ignored entirely.
4. **Reserve arithmetic boundaries**: reserve zero; one byte below the limit;
   exactly the limit; above the limit; regions exactly equal to the limit
   (refused); regions plus reserve exactly equal to the limit (allowed); and a
   fold that would overflow `uint64`. Each under warn, refuse-impossible, and
   refuse-reserve.
5. **A skip case.** The limit resolver returning `Unlimited` *and* returning
   `Unknown` must both start normally under *every* policy including
   `MemoryBudgetRefuseReserve`, with an impossible configuration. This is the
   test that stops a laptop or an exotic container from being bricked by this
   feature, and it is the one most likely to be omitted. `Unknown` must also
   produce a logged reason.
6. **The reload peak formula, with asymmetric geometries.** Two plugins with
   *different* region sizes charged concurrently; assert the admitted set matches
   `steady + Σ charges < limit` exactly, and that the refused reload left its
   predecessor serving and returned `ErrMemoryBudget`.
7. **Charge release on every reload outcome**: successful promotion followed by
   predecessor teardown, pre-promote rollback after successor attach failure, and
   a reload whose ctx is cancelled. The charge must be zero afterwards in all
   three, and a subsequent reload that needs the same bytes must succeed.
8. **Crash-restart never charges anything.** The restart path is inside
   `Supervisor.Run` and never touches the gate; a test that restarts a plugin
   repeatedly and then reloads it proves the counter did not drift
   (`internal/supervisor/supervisor.go:591-599`, `:871-890`).
9. **`Start` retried after a partial failure prices what is live, not what the
   config now says.** Start a default-geometry plugin alongside one that fails,
   edit the original slices to the lean profile, retry, and prove the verdict
   still counts the live plugin at its committed 65,994,752 bytes. Then reload
   that plugin and prove its charge uses the committed size too. Run the inverse
   edit (lean to default) to catch over-counting as well as under-counting.
   Already-started names are still refused by `startOne` with
   `ErrPluginAlreadyStarted` (`host.go:483-493`, `host.go:533-554`).
10. **Duplicate plugin names**: pin that the sum counts both entries while only
    one region is created, so the over-count is a chosen behavior.
11. **A limit that changes between `Start` and `Reload`**: `Start` passes under
    the old limit, the limit shrinks, and the reload gate refuses on the new one.
12. **Config mutation *during* `Start`, not before it.** Mutating between
    `NewHost` and `Start` proves nothing: a snapshot taken at `Start` observes
    that mutation either way, so the test passes whether or not a snapshot
    exists. The real assertion is that the check and every supervisor built by
    one `Start` see identical geometry — exercise it against a mutation racing
    that call, and pin agreement between the checked total and what each
    supervisor received.
13. **Two Hosts in one process**, one built while the other's plugins are still
    live: prove each checks only its own plugins. This test pins the *documented
    limitation*, so its name and assertion must say that plainly rather than
    imply coverage the design does not have.
14. **A failed unmap becomes a sticky leak.** Inject `munmap` failure at
    predecessor retirement, and prove: the charge is not returned to the pool, a
    later reload cannot spend those bytes again, and the failure is logged. Do
    the same for pre-promote rollback of a successor. Keep the join-timeout case
    separate — teardown attempts unmap even when its bounded join times out
    (`internal/lifecycle/teardown.go:115-140`), so that path does not exercise a
    failed unmap.
15. **Mixed cgroup ancestry classifies as `Unknown`.** Finite leaf plus `EACCES`
    parent; finite parent plus malformed leaf; two finite levels with one
    unreadable level between them; `EINTR` that succeeds on retry. Assert the
    final class, that no bytes are enforced against, that every failing level's
    reason is preserved, and that the policy outcome is fail-open.
16. **One error contract for both refusals.** Refuse a `Start`, raise the
    injected limit, retry the identical `Host` successfully — matching
    `ErrMemoryBudget` both times — and `errors.As` the structured totals. Then
    prove an out-of-range `MemoryBudgetPolicy` and a geometry `RegionBytes`
    rejects are `*ConfigError`, raised before any cgroup read or spawn.
17. **An `auto`, uds-only plugin with invalid geometry is refused at `Start`.**
    This is the accepted compatibility change; the test names it as such. Its
    counterparts: an explicit `TransportUDS` spec with the same geometry still
    starts, and an `auto` plugin that negotiates shared memory is refused before
    the region is created.
18. **`MemoryBudgetOff` reads nothing.** No cgroup file access, no telemetry
    read. Assert against the injected resolver, not by timing.
19. **A reload charge cannot be stranded by a concurrent `Stop`.** Hold a reload
    after its charge is committed and before the supervisor request, begin
    `Stop` — which removes runtimes and marks names stopping under `h.mu`
    (`host.go:1170-1210`) — and prove the charge settles exactly once, never
    underflows, and does not block teardown. Use causal barriers around that
    boundary, not sleeps.
20. **Two concurrent reloads of the same plugin.** The supervisor serializes
    transactions through one buffered request channel
    (`internal/supervisor/supervisor.go:539-544`,
    `internal/supervisor/reload.go:50-72`); Host accounting may hold two charges
    conservatively, but each must settle exactly once.
21. **Attach failures after `CreateRegion`** and after the transport's duplicate
    mapping opens: the existing cleanup closes the original region on every
    returned error (`internal/supervisor/supervisor.go:1620-1633`,
    `internal/supervisor/supervisor.go:1688-1710`), and no Host charge may
    survive outside a reload transaction.
22. **Telemetry independence**: a failure to read `memory.current`/`memory.stat`
    must change the error message and never the verdict.

## Cost and benefit

**The highest-value part is already built and shipped.** `RegionBytes` exists
and derives through the same `shm.DeriveLayout` that `CreateRegion` calls, so it
cannot drift from the size actually mapped (`geometry.go:113-142`,
`internal/shm/region.go:94-198`). The capacity-planning guide states the exact
figures — 65,994,752 bytes per default-profile plugin, 251.75 MiB for four, a
rolling reload peaking at 314.7 MiB (`docs/configuration.md:294-388`). That was
the lowest-risk, highest-value piece, and it landed without any runtime
machinery or new failure mode.

Everything below is what remains, and it must be judged on what it adds *beyond*
that guide — because an operator who reads the guide and sizes the container
correctly already avoids the motivating failure. The check is worth building for
the operator who does not read it, or who inherits a config, or whose limit is
set by someone else entirely. That is a real population, but it is a smaller
claim than "this prevents the OOM", and the cost below should be weighed against
the smaller claim.

**Build the static check, defaulting to refusing only the impossible case.**
Benefit: converts a delayed, load-dependent, misattributed kill — where the host,
mapping every region twice, is the highest-scoring OOM victim and takes down
plugins that were behaving — into an `ErrMemoryBudget` error at `Start` naming the exact
shortfall. The default has no false-alarm surface by construction on the
impossible band, because it refuses only what is arithmetically impossible.

**What remains, listed honestly.** The original pitch was "a pure function over
config, one new leaf package, two `HostConfig` fields, no changes to existing
behavior". What is actually left:

- a new leaf package `internal/cgroupmem` (v2 only, tri-state, partial reads
  classified `Unknown`);
- a new leaf package `internal/cgroupfs` holding four functions **moved out of
  `internal/event`**, with their tests relocated — a change to working code;
- committed region bytes recorded on `pluginRuntime`, and the steady sum folded
  over installed runtimes rather than over the current config;
- a resolved config snapshot at the top of `Start`, changing what `startOne`
  iterates over;
- an error return threaded through the teardown unmap seam
  (`internal/lifecycle/teardown.go:50-82`) and through successor rollback
  (`internal/lifecycle/rollback.go:37-47`), so a failed `munmap` is observable
  at all — a signature change in working internal API;
- a charge gate, a charge field and a leak field on `Host`, plus a new refusal
  path in `Reload` — a new failure mode on a public method that previously could
  not fail this way;
- two public additions (`MemoryBudgetPolicy`, `MemoryBudgetReserve`), one
  exported sentinel (`ErrMemoryBudget`) and its structured error type;
- Godoc on `PluginSpec.Geometry` stating that an `auto` spec's geometry is now
  validated at `Start`, plus a release note for that behavior change;
- an amendment to `docs/configuration.md:390-397`, replacing both the
  "information, not enforcement" text and the cgroup-v1 requirement;
- ~25 test cases, several needing seams that do not exist yet, including an
  injectable limit resolver and an injectable unmap failure.

The growth is concentrated in three places — the reload gate, the cgroup-parsing
extraction, and now the unmap-observability plumbing — all of which came from
taking the rule seriously rather than from gold-plating. `RegionBytes`, the
shared derivation, their tests and the capacity guide are **prerequisites
already met** and are deliberately absent from this list.

A reviewer who thinks the reload half is not worth its cost should say so
explicitly, because dropping it means narrowing the rule to steady state and
saying so in the godoc, not leaving the gap unstated.

**Do not build the dynamic check.** Argued above: optimistic about untouched
pages, pessimistic about reclaimable page cache, and expensive in exactly the
places the static check is free.

**The "do not build any of this" case**, stated fairly and now stronger than it
was. Three things push against building the runtime half:

1. If `GeometryLean` were the default rather than `GeometryDefault`, four plugins
   would cost 2.56 MiB and this failure would not exist for anyone who did not
   deliberately opt into a large geometry. Changing the default is a breaking
   behavioral change with its own serious problem — the lean profile's 4160-byte
   largest class is a hard ceiling, not a backpressure point
   (`geometry.go:88-99`), so it would turn a working deployment's kilobyte
   payloads into outright rejections. But "the default is too big for a
   container" and "add a check that tells you the default is too big for your
   container" are answers to the same question, and only one of them adds code.
2. The check's coverage is narrower than its name suggests: cgroup v2 only, one
   Host only, and — at the default policy — only the arithmetically impossible
   band. `RegionBytes` plus a documented capacity table gets a careful operator
   to the same place with none of the machinery.
3. Every skip path is silent by design. A deployment that most needs the check
   (an unusual container view, a v1 host) is exactly one where it does nothing.

The middle option is genuinely attractive and should be considered on its
merits: **ship `RegionBytes`, the doc section with the real numbers including the
reload peak, and nothing else** — then add the `Start` check once someone has hit
the failure *with* the documentation in place. It costs one method and a doc
page, has no new failure mode, no cgroup dependency, and no behavior change. The
argument against it is that documentation does not fire at 3 a.m. and the failure
it prevents is misattributed by construction.

## What this does not solve

- **It does not reclaim memory.** Nothing here makes a faulted arena page go
  back to the kernel. `MADV_DONTNEED` on a `MAP_SHARED` tmpfs mapping does not
  free the page — it must be `fallocate(FALLOC_FL_PUNCH_HOLE)` on the memfd, and
  the memfd is sealed with `F_SEAL_SHRINK` at creation (`wantSeals`,
  `internal/shm/region.go:70`, applied at `:203`), which is exactly the seal
  that forbids it. A reclaim design would have to revisit the seal set, which is
  part of the frozen ABI. Large, and out of scope here.
- **It does not make backpressure byte-denominated.** A geometry that fits the
  limit still reports pressure in slabs, and a workload that exhausts one class
  while others sit idle still sees no byte-level signal. What would fix it: an
  arena high-water byte counter alongside `MetricArenaUtilization`, and a
  set-aside trigger that can fire on total faulted bytes rather than only on a
  class's free list. That is a change inside `internal/arena` and
  `internal/transport/shm/writer.go`, independent of anything in this note.
- **It does not help a workload whose genuine high-water exceeds the limit.** If
  the payloads really need the default ladder and the container really has 100
  MiB, the check converts a mysterious kill into a clear error — and the error is
  still "buy more memory or use a smaller geometry". What would help: a smaller
  custom geometry sized to the actual message distribution, which
  `docs/configuration.md` already explains how to build.
- **It does not see any other tenant of the cgroup**, another `Host` in this
  process, or a predecessor `Host` whose teardown outlived an expired `Stop`. The
  sum is one Host's demand, not the container's.
- **It does not cover cgroup v1**, by decision. A v1 host skips the check
  entirely and logs why.
- **It does not bind a limit that changes after it was observed.** A decision is
  made against the limit that operation read. An administrator lowering
  `memory.max`, or moving this process into a stricter cgroup, afterwards is
  outside the rule — including a shrink that lands between a `Start`'s preflight
  and the handshake at which a region is actually created
  (`internal/supervisor/supervisor.go:1604-1623`), and a shrink while the `Host`
  sits idle with no operation at which to notice. Only the cgroup controller
  watches memory continuously.
- **It does not recover bytes whose unmap failed.** A region whose `munmap`
  returns an error stays mapped and unaddressable, so its bytes are held against
  every later decision for the life of the process rather than released. This is
  a deliberate one-way state, not an oversight; the alternative is spending
  bytes that were never given back.

## Unverified

- **cgroup charges a shared tmpfs page exactly once**, to the memcg that first
  touches it, so the host and plugin both mapping a region does not double the
  charge. The budget arithmetic here depends on it. It is standard memcg
  behavior and consistent with the measurement quoted in the problem statement
  (32 MiB written, `shmem` rose by exactly 33554432), but I did not re-measure
  the two-process case.
- **The host's double mapping double-counts in its own RSS.** Reasoned from how
  RSS counts page-table entries per mapping, not measured. It affects
  OOM-victim selection, not the budget sum, so nothing in the proposed check
  turns on it.
- **How much a reload's successor actually faults before promotion.** The charge
  gate budgets the successor's full nominal region, which is the safe bound, but
  a successor that is refused was perhaps never going to fault more than a
  fraction of it. Measuring this would tell us whether the gate's default band is
  too conservative; it does not change correctness.
- **Whether any CI runner for this repo runs under a memory limit.** If one
  does, the on-host sanity test must still assert only that resolution
  terminates, never a particular value.
- **Whether a real deployment has ever had an `auto` plugin negotiate uds.**
  The mandatory sum's one false-refusal mode depends entirely on this being rare.
  Every plugin built from this module speaks the shared-memory transport, so the
  case requires a deliberately uds-only plugin that did not declare itself as
  one — but this is an assumption about users, not a fact about the code.
- **The exact allocator rounding on the arena's free lists.** The 91,100-byte
  figure is the requested size; Go's size classes round each slice allocation up.
  Irrelevant at this magnitude, and it is stated as a floor rather than an exact
  resident cost.

## What changed in this revision, and why

- **The invariant is a decision rule, not a continuous property.** "At every
  moment of a Host's life" was unachievable and is withdrawn: `memory.max` is
  writable at runtime and a process can be moved to a stricter cgroup, so a
  limit can shrink after a preflight and before the region it admitted is
  created, or while the Host sits idle. Each `Start` and `Reload` now decides
  against the limit it observed, and the changing-limit boundary is listed
  alongside the cgroup-v1 and replacement-Host ones.
- **Region bytes are owned by the runtime, not by the configuration.** A
  per-`Start` snapshot keeps one call self-consistent but cannot keep the total
  honest across a supported retry: `Start` keeps the plugins that started, the
  caller may legitimately edit the config in between, and `pluginRuntime`
  recorded no size. The committed size now lives on the runtime, the steady sum
  folds over installed runtimes, and a reload charges the predecessor's actual
  size rather than whatever the config says at that moment.
- **Releasing a charge now means the mapping is gone.** `Region.Close` already
  reports a failed `munmap` and withholds its closed count, but the supervisor
  callback discarded that error and the teardown unmap seam had nowhere to put
  one — so a `defer`-subtract could free a charge whose region was still mapped,
  with no way to retry the release. The unmap outcome is now threaded out, and a
  failed release converts the charge into a sticky leak total counted by every
  later decision rather than silently returned to the pool.
- **A partial cgroup read classifies as `Unknown`, not `Limited`.** The two
  conditions previously overlapped. A finite leaf under an unreadable parent
  yields a minimum-over-readable that nothing actually enforces, which both
  refuses configurations that fit and admits ones that do not.
- **One error contract for both refusals.** `Start` returned `*ConfigError` and
  `Reload` returned `ErrMemoryBudget` for the same cause. A budget shortfall
  depends on machine state, so classifying it by which method noticed told a
  caller to give up in one place and retry in the other. Both now use
  `ErrMemoryBudget` with the totals structured rather than formatted into a
  message; `*ConfigError` keeps malformed policy values and invalid geometry.
- **The `TransportAuto` cost includes a compatibility change, not just a
  conservative refusal.** Sizing an `auto` spec before negotiation validates its
  geometry, so an `auto` plugin that is uds-only with a malformed geometry starts
  today and would fail `Start` afterwards. Accepted deliberately, and now
  required to appear in `PluginSpec.Geometry`'s Godoc and the release notes.
- **The work list is rebased on the current tree.** `RegionBytes`, the shared
  `shm.DeriveLayout` derivation, their agreement and invalid-geometry tests, and
  the capacity-planning guide all shipped since this note was first written. They
  are prerequisites already met, not remaining work, and the cost estimate no
  longer counts them. What the remaining work must justify is therefore narrower:
  an operator who reads the capacity guide already avoids the motivating failure.
- **The unmap-observability plumbing is counted as a change to working code**,
  alongside the cgroup-primitive extraction, rather than being absorbed silently.
