package event

import (
	"errors"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// cgroupRoot is the cgroup v2 mount point. This is NOT this process's own
// cgroup -- it's the top of the whole hierarchy. A bare systemd host's
// processes live several levels below it (e.g.
// /sys/fs/cgroup/user.slice/user-1000.slice/session-2.scope), and
// cgroupRoot+"/cpu.max" does not exist there at all (confirmed
// empirically). Reading that hardcoded path directly would silently return
// "no quota" unconditionally on any such host, even under a correctly
// configured `systemd-run ... -p CPUQuota=200%` quota, because the quota
// lives on the process's own (or an ancestor) cgroup, never on the root
// itself in that setup -- this is why resolveCPUQuota resolves the process's
// own path first (see ownCgroupPath) and then walks up to the root.
const cgroupRoot = "/sys/fs/cgroup"

// quotaClass classifies the outcome of resolving this process's effective
// cgroup v2 CPU bandwidth limit across its full cgroup ancestry. The
// classification is a tri-state because "no finite quota found" is NOT the
// same as "confirmed unlimited": a level whose cpu.max could not be read (or
// whose /proc/self/cgroup path could not be resolved, or whose cpu.max held
// content matching neither the finite nor the "max" form) leaves "unlimited"
// unprovable, and the spin policy must fail closed on that ambiguity rather
// than run the full budget.
type quotaClass int

const (
	// quotaUnlimited: every level of the ancestry was cleanly resolved (a
	// readable "max", or a benign absence -- no cpu controller / no cpu.max
	// at that level) and none carried a finite quota. Only this class may
	// run the full spin budget.
	quotaUnlimited quotaClass = iota
	// quotaLimited: at least one ancestor level carried a finite quota. The
	// accompanying ratio is the MINIMUM finite quota/period across all
	// readable levels -- the effective limit, since CFS bandwidth is
	// hierarchical and a group's sustainable rate is bounded by the
	// strictest ancestor, not the nearest finite one.
	quotaLimited
	// quotaUnknown: no finite quota was found AND at least one level's
	// cpu.max was unreadable by a real error (not a benign absence), so
	// "unlimited" cannot be confirmed. The spin policy fails closed on this
	// class (shrinks the budget) rather than assume unlimited.
	quotaUnknown
)

// CgroupCPUQuota resolves this process's effective cgroup v2 CPU quota across
// its full cgroup ancestry and certifies the effective CPU count (the minimum
// finite quota/period ratio found) with ok=true ONLY when a finite quota was
// found AND every ancestry level was cleanly resolved. It returns (_, false)
// otherwise -- a confirmed-unlimited hierarchy, an unconfirmable one (a level
// unreadable, or its /proc/self/cgroup path unresolved, or its cpu.max
// malformed), OR a finite quota whose ancestry was not fully readable. In that
// last case the returned ratio is only the minimum over the READABLE levels: a
// stricter unreadable ancestor may exist, so the ratio is not the provable
// effective minimum and MUST NOT be certified. ok=true therefore means "this
// ratio is the proven effective limit"; ok=false means "do not trust any
// ratio, resolve the constraint another way".
//
// Exported so callers outside this package can verify a claimed scheduler
// regime actually matches this process's real cgroup constraint -- e.g. a
// benchmark suite can fail loudly when a "cgroup2cpu"-style regime label isn't
// backed by a provable finite quota, using the same probe NewSpinWaiter's
// quota-aware policy relies on. The spin policy itself decides on the (ratio,
// class) tri-state from resolveCPUQuota and does not require exactness: a
// finite-but-inexact result still shrinks or zeros the budget, never runs it
// in full, so ok=false never widens the spin budget.
func CgroupCPUQuota() (float64, bool) {
	return cgroupCPUQuotaVia(readFileNoFail)
}

// cgroupCPUQuotaVia is the reader-injected core of CgroupCPUQuota, factored
// out so the exact-ratio certification -- ok=true only for a finite quota
// resolved cleanly at every ancestry level -- is testable against a synthetic
// path -> (content, err) map without touching the real cgroup filesystem.
func cgroupCPUQuotaVia(read cpuMaxReader) (float64, bool) {
	ratio, class, exact := resolveCPUQuota(read)

	return ratio, class == quotaLimited && exact
}

// resolveCPUQuota walks this process's full cgroup ancestry -- from its own
// cgroup path up to and including cgroupRoot -- reading cpu.max at every
// level via read, and classifies the effective CFS CPU bandwidth limit as a
// tri-state (see quotaClass) plus an exact flag. CFS bandwidth is
// hierarchical, so a finite quota on ANY ancestor applies down the tree and
// the effective limit is the MINIMUM finite quota/period ratio across all
// readable levels; the walk does not stop at the first finite level it finds.
//
// Fail closed on any unconfirmable input. Only a level that is CLEANLY
// resolved may contribute to a confirmed-unlimited outcome:
//   - a readable finite "<quota> <period>" pair -> a finite level (contributes
//     its ratio);
//   - a readable canonical "max <period>" -> unlimited at this level, keep
//     walking up for a stricter ancestor;
//   - a benign absence (ENOENT -- no cpu controller or no cpu.max there,
//     normal at intermediate levels) -> unlimited-eligible, keep walking;
//   - a real read error (permissions, EIO, ...), OR cpu.max content matching
//     neither the finite nor the "max" form (malformed / short / garbage), OR
//     an unreadable /proc/self/cgroup that leaves the process's own path
//     unresolved -> the level is unconfirmable and sets sawRealError.
//
// haveFinite wins the CLASS so the spin policy always keeps a usable ratio
// when one was found: haveFinite -> Limited, else sawRealError -> Unknown,
// else Unlimited. exact = !sawRealError reports whether every level resolved
// cleanly: a Limited-but-inexact result carries a ratio that is only the
// minimum over the READABLE levels (a stricter unreadable ancestor may exist),
// so exact is what CgroupCPUQuota gates its certification on. read is a seam
// so the walk is testable against a synthetic path -> (content, err) map.
func resolveCPUQuota(read cpuMaxReader) (float64, quotaClass, bool) {
	// "" -> the walk starts at (and stays at) cgroupRoot. A real read error of
	// /proc/self/cgroup means this process's own cgroup -- which may itself
	// carry a finite quota -- is unknown, so a root-only walk that finds no
	// finite quota cannot prove the hierarchy unlimited: taint toward Unknown.
	path, ownPathReadErr := ownCgroupPathVia(read)

	var minRatio float64
	var haveFinite bool
	sawRealError := ownPathReadErr
	for {
		data, err := readRetryEINTR(read, cgroupRoot+path+"/cpu.max")
		switch {
		case err == nil:
			switch ratio, kind := classifyCPUMax(data); kind {
			case cpuMaxFinite:
				if !haveFinite || ratio < minRatio {
					minRatio = ratio
				}
				haveFinite = true
			case cpuMaxUnlimited:
				// Canonical "max <period>": unlimited here, keep walking up.
			case cpuMaxMalformed:
				// Readable, but neither a finite pair nor the "max" keyword:
				// unconfirmable at this level, so "unlimited" is not proven.
				sawRealError = true
			}
		case errors.Is(err, unix.ENOENT):
			// Benign absence: no cpu.max at this level. Normal at
			// intermediate levels; unlimited-eligible, keep walking.
		default:
			// A real read error: cannot confirm unlimited at this level.
			sawRealError = true
		}

		if path == "" || path == "/" {
			break
		}
		path = parentCgroupPath(path)
	}

	exact := !sawRealError
	switch {
	case haveFinite:
		return minRatio, quotaLimited, exact
	case sawRealError:
		return 0, quotaUnknown, exact
	default:
		return 0, quotaUnlimited, exact
	}
}

// cpuMaxReader reads a cgroup control file's raw bytes. In production it is
// readFileNoFail (a real open+read of an absolute path); tests substitute a
// synthetic path -> (content, err) map so resolveCPUQuota's ancestry walk is
// exercised deterministically without touching the real filesystem.
type cpuMaxReader func(path string) ([]byte, error)

// readRetryEINTR calls read(path), retrying only on EINTR so a signal
// interrupting the syscall cannot be mistaken for a real read failure (which
// would spuriously push resolveCPUQuota toward Unknown). Any other error --
// including the benign ENOENT the walk relies on -- is returned unchanged.
func readRetryEINTR(read cpuMaxReader, path string) ([]byte, error) {
	for {
		data, err := read(path)
		if errors.Is(err, unix.EINTR) {
			continue
		}

		return data, err
	}
}

// ownCgroupPathVia reads /proc/self/cgroup via read and returns this
// process's own cgroup v2 unified-hierarchy path. A "" path can arise three
// ways, which the readErr flag collapses into "taint or not" for
// resolveCPUQuota:
//   - readErr=true, the file could not be READ: the process's own cgroup is
//     genuinely unknown (it may carry a finite quota), so a root-only walk
//     that finds nothing must NOT conclude Unlimited -- taint toward Unknown.
//   - readErr=true, a "0::" line is present but its path is empty/blank
//     (malformed): the unified hierarchy exists yet its path is unusable, so
//     the process's own level cannot be walked -- taint toward Unknown.
//   - readErr=false with ""=path, no "0::" line at all: read cleanly, cgroup
//     v1 only or an unusual container view. This is the intentional, benign
//     fallback -- walk from cgroupRoot, do not taint.
//
// A "" path (any case) makes resolveCPUQuota's walk start at cgroupRoot; only
// the taint cases set readErr.
func ownCgroupPathVia(read cpuMaxReader) (path string, readErr bool) {
	data, err := readRetryEINTR(read, "/proc/self/cgroup")
	if err != nil {
		return "", true // real read error: caller taints toward Unknown
	}
	p, kind := parseOwnCgroupPath(data)
	switch kind {
	case ownPathResolved:
		return p, false
	case ownPathMalformed:
		return "", true // present-but-empty "0::" path: unusable -> taint
	case ownPathAbsent:
		return "", false // benign: no "0::" v2 line -> walk from root
	}

	return "", false // unreachable: parseOwnCgroupPath returns only the kinds above
}

// ownCgroupPath resolves this process's own cgroup v2 unified-hierarchy path
// against the real filesystem, factored out so the on-host resolution test
// can assert the process resolves a non-empty path with a readable cpu.max.
// ok=false if the file can't be read or has no "0::" v2 line.
func ownCgroupPath() (string, bool) {
	data, err := readFileNoFail("/proc/self/cgroup")
	if err != nil {
		return "", false
	}
	p, kind := parseOwnCgroupPath(data)

	return p, kind == ownPathResolved
}

// ownPathKind is the three-way outcome of parsing /proc/self/cgroup for this
// process's cgroup v2 unified-hierarchy path. Distinguishing a malformed from
// an absent "0::" line is load-bearing for failing closed: a present-but-empty
// path means the unified hierarchy exists yet its path is unusable (the own
// level cannot be walked, so taint toward Unknown), whereas a wholly absent
// "0::" line is the benign cgroup v1 / unusual-container case that walks from
// the root without tainting.
type ownPathKind int

const (
	// ownPathResolved: a "0::" line with a non-empty path -- the usable
	// unified-hierarchy path this process lives under.
	ownPathResolved ownPathKind = iota
	// ownPathAbsent: no "0::" line at all (a cgroup v1-only host has numbered
	// hierarchy lines like "4:cpu,cpuacct:/..." but never a "0::" line, as does
	// an unusual container view). Benign -- walk from cgroupRoot, no taint.
	ownPathAbsent
	// ownPathMalformed: a "0::" line whose path is empty/blank. The unified
	// hierarchy is present but its path is unusable, so "unlimited" cannot be
	// proven from a root-only walk -- taint toward Unknown.
	ownPathMalformed
)

// parseOwnCgroupPath parses /proc/self/cgroup content for this process's own
// cgroup v2 unified-hierarchy path (the "0::<path>" line -- cgroup v2's
// unified hierarchy is always hierarchy-id 0 with an empty controller list),
// factored out so it's testable against synthetic content without touching
// the filesystem (real hosts vary: pure v2, hybrid v1+v2, containers). See
// ownPathKind for the three outcomes and why the malformed/absent split
// matters.
func parseOwnCgroupPath(data []byte) (string, ownPathKind) {
	for line := range strings.SplitSeq(string(data), "\n") {
		rest, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if rest == "" {
			return "", ownPathMalformed // present-but-empty "0::" path
		}

		return rest, ownPathResolved
	}

	return "", ownPathAbsent // no "0::" line at all
}

// parentCgroupPath returns path's parent within the cgroup path namespace
// ("/a/b/c" -> "/a/b", "/a" -> "", "/" -> ""), where "" means cgroupRoot
// itself -- resolveCPUQuota's terminating case.
func parentCgroupPath(path string) string {
	path = strings.TrimSuffix(path, "/")
	i := strings.LastIndexByte(path, '/')
	if i <= 0 {
		return ""
	}

	return path[:i]
}

func readFileNoFail(path string) ([]byte, error) {
	f, err := unix.Open(path, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(f) }()

	// Read to EOF. cpu.max is a few bytes, but the same reader serves
	// /proc/self/cgroup, which on a deep hybrid cgroup v1+v2 host can list a
	// dozen-plus hierarchy lines and exceed one page. A single fixed read
	// could truncate it mid-line -- silently yielding a cut-but-valid-looking
	// path that takes the root fallback -- so append page-sized reads until
	// unix.Read returns 0 (EOF). An EINTR surfaces as an error here and
	// re-runs the whole read from readRetryEINTR, so partial data is never
	// returned.
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, err := unix.Read(f, buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
	}
}

// cpuMaxKind is the three-way classification of a readable cpu.max content.
// The distinction is load-bearing for failing closed: the canonical "max"
// keyword is a legitimate unlimited level (benign, keep walking), whereas
// content matching neither the finite pair nor the "max" form is
// unconfirmable and must taint the resolution toward Unknown.
type cpuMaxKind int

const (
	// cpuMaxUnlimited: the canonical "max <period>" keyword form -- no limit
	// at this level, keep walking up for a stricter ancestor.
	cpuMaxUnlimited cpuMaxKind = iota
	// cpuMaxFinite: a valid "<quota> <period>" pair -- a finite limit whose
	// quota/period ratio the accompanying float reports.
	cpuMaxFinite
	// cpuMaxMalformed: readable content matching neither form (short, empty,
	// garbage, or an out-of-range quota). Unconfirmable: "unlimited" cannot be
	// proven at this level, so it taints toward Unknown.
	cpuMaxMalformed
)

// classifyCPUMax classifies already-read cpu.max content three ways (see
// cpuMaxKind), returning the finite ratio only for cpuMaxFinite. A valid
// "<quota> <period>" pair is finite; the literal canonical "max <period>" is
// unlimited; anything else readable is malformed -- NOT silently treated as
// the canonical unlimited "max".
func classifyCPUMax(data []byte) (float64, cpuMaxKind) {
	if ratio, ok := parseCPUMax(data); ok {
		return ratio, cpuMaxFinite
	}
	if isCanonicalMax(data) {
		return 0, cpuMaxUnlimited
	}

	return 0, cpuMaxMalformed
}

// isCanonicalMax reports whether data is cgroup v2's canonical unlimited
// cpu.max form: exactly the two whitespace-separated tokens "max" and a
// positive-integer period (e.g. "max 100000\n"). Only this exact shape is a
// benign unlimited level; a bare "max", a missing period, or trailing garbage
// is malformed and must not be mistaken for a confirmed unlimited level.
func isCanonicalMax(data []byte) bool {
	fields := strings.Fields(string(data))
	if len(fields) != 2 || fields[0] != "max" {
		return false
	}
	// The period must be a POSITIVE integer: a zero or negative period is
	// nonsensical, and "max 0" / "max -1" must taint toward Unknown rather
	// than pass as a confirmed unlimited level.
	period, err := strconv.ParseInt(fields[1], 10, 64)

	return err == nil && period > 0
}

func parseCPUMax(data []byte) (float64, bool) {
	n, err := parseTwoInts(data)
	if err != nil {
		return 0, false
	}
	quota, period := n[0], n[1]
	if quota <= 0 || period <= 0 {
		return 0, false // "max" or unparsable => no quota
	}

	return float64(quota) / float64(period), true
}

// parseTwoInts parses a finite cpu.max "<quota> <period>" line into its two
// integers. It requires EXACTLY two whitespace-separated tokens (a trailing
// newline is whitespace, not a third token) and converts each with
// strconv.ParseInt in base 10 at 64 bits, so trailing content, non-digit
// bytes, and out-of-range values are all errors -- never a finite pair. That
// strictness is load-bearing for failing closed: "<q> <p> extra" and an
// overflowing quota must classify as malformed (taint toward Unknown), not as
// a truncated or wrapped-small finite. The canonical "max <period>" form
// fails here by design (its first token is not a number) and is classified
// unlimited by isCanonicalMax instead.
func parseTwoInts(data []byte) ([2]int64, error) {
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return [2]int64{}, unix.EINVAL
	}

	var out [2]int64
	for i, tok := range fields {
		n, err := strconv.ParseInt(tok, 10, 64)
		if err != nil {
			return [2]int64{}, unix.EINVAL // non-digit or out-of-range: not a finite pair
		}
		out[i] = n
	}

	return out, nil
}
