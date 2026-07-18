package event

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Test parseOwnCgroupPath against synthetic /proc/self/cgroup content --
// covering pure cgroup v2 (a single "0::" line), hybrid v1+v2 (numbered
// hierarchy lines alongside the "0::" line), cgroup-v1-only hosts (no
// "0::" line at all), and edge cases -- without touching the filesystem.
func TestParseOwnCgroupPath(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantPath string
		wantKind ownPathKind
	}{
		{
			name:     "pure cgroup v2 unified hierarchy",
			content:  "0::/user.slice/user-1000.slice/session-2.scope\n",
			wantPath: "/user.slice/user-1000.slice/session-2.scope",
			wantKind: ownPathResolved,
		},
		{
			name: "hybrid v1+v2 host: 0:: line among numbered hierarchy lines",
			content: "12:pids:/user.slice/user-1000.slice\n" +
				"11:memory:/user.slice/user-1000.slice\n" +
				"10:cpu,cpuacct:/user.slice/user-1000.slice\n" +
				"0::/user.slice/user-1000.slice/session-2.scope\n",
			wantPath: "/user.slice/user-1000.slice/session-2.scope",
			wantKind: ownPathResolved,
		},
		{
			name: "cgroup v1 only host: no 0:: line is absent (benign)",
			content: "12:pids:/user.slice\n" +
				"11:memory:/user.slice\n" +
				"10:cpu,cpuacct:/user.slice\n",
			wantPath: "",
			wantKind: ownPathAbsent,
		},
		{
			name:     "cgroupns=private container: own path is the root",
			content:  "0::/\n",
			wantPath: "/",
			wantKind: ownPathResolved,
		},
		{
			name:     "empty path after 0:: prefix is malformed, not absent",
			content:  "0::\n",
			wantPath: "",
			wantKind: ownPathMalformed,
		},
		{
			name:     "blank (whitespace-only) path after 0:: prefix is malformed",
			content:  "0::   \n",
			wantPath: "",
			wantKind: ownPathMalformed,
		},
		{
			name:     "empty content has no 0:: line: absent",
			content:  "",
			wantPath: "",
			wantKind: ownPathAbsent,
		},
		{
			name:     "no trailing newline",
			content:  "0::/foo.slice/bar.scope",
			wantPath: "/foo.slice/bar.scope",
			wantKind: ownPathResolved,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotKind := parseOwnCgroupPath([]byte(tt.content))
			require.Equal(t, tt.wantKind, gotKind)
			require.Equal(t, tt.wantPath, gotPath)
		})
	}
}

// Test parseCPUMax against synthetic cpu.max content -- the "max" (no
// limit) case, real quota/period pairs, and unparsable/garbage content.
func TestParseCPUMax(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		wantQuota float64
		wantOK    bool
	}{
		{name: "unlimited (max)", content: "max 100000\n", wantQuota: 0, wantOK: false},
		{name: "2 CPU quota (CPUQuota=200%)", content: "200000 100000\n", wantQuota: 2.0, wantOK: true},
		{name: "1 CPU quota", content: "100000 100000\n", wantQuota: 1.0, wantOK: true},
		{name: "2.5 CPU quota", content: "250000 100000\n", wantQuota: 2.5, wantOK: true},
		{name: "no trailing newline still parses", content: "200000 100000", wantQuota: 2.0, wantOK: true},
		{name: "zero quota is invalid", content: "0 100000\n", wantQuota: 0, wantOK: false},
		{name: "missing period", content: "200000\n", wantQuota: 0, wantOK: false},
		{name: "trailing token after a pair is not finite", content: "200000 100000 x\n", wantQuota: 0, wantOK: false},
		{name: "three integer tokens is not finite", content: "200000 100000 50000\n", wantQuota: 0, wantOK: false},
		{
			name:      "overflowing quota is not a wrapped finite",
			content:   "99999999999999999999999 100000\n",
			wantQuota: 0,
			wantOK:    false,
		},
		{name: "garbage", content: "not-a-number\n", wantQuota: 0, wantOK: false},
		{name: "empty", content: "", wantQuota: 0, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQuota, gotOK := parseCPUMax([]byte(tt.content))
			require.Equal(t, tt.wantOK, gotOK)
			if tt.wantOK {
				require.InDelta(t, tt.wantQuota, gotQuota, 1e-9)
			}
		})
	}
}

// Test classifyCPUMax's three-way split, focusing on the strict canonical-max
// rule: only "max <positive period>" is a confirmed unlimited level; a zero or
// negative period, a bare "max", or trailing content is malformed and must
// taint toward Unknown rather than pass as unlimited.
func TestClassifyCPUMax(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    cpuMaxKind
	}{
		{name: "canonical unlimited", content: "max 100000\n", want: cpuMaxUnlimited},
		{name: "canonical unlimited without newline", content: "max 100000", want: cpuMaxUnlimited},
		{name: "finite pair", content: "200000 100000\n", want: cpuMaxFinite},
		{name: "zero period is malformed", content: "max 0\n", want: cpuMaxMalformed},
		{name: "negative period is malformed", content: "max -1\n", want: cpuMaxMalformed},
		{name: "bare max without a period is malformed", content: "max\n", want: cpuMaxMalformed},
		{name: "max with trailing content is malformed", content: "max 100000 extra\n", want: cpuMaxMalformed},
		{name: "garbage is malformed", content: "garbage\n", want: cpuMaxMalformed},
		{name: "empty is malformed", content: "", want: cpuMaxMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := classifyCPUMax([]byte(tt.content))
			require.Equal(t, tt.want, got)
		})
	}
}

// Test parentCgroupPath's path-shortening, including the two ways the walk
// upward terminates (single-segment path, and "/" itself).
func TestParentCgroupPath(t *testing.T) {
	tests := []struct{ in, want string }{
		{"/a/b/c", "/a/b"},
		{"/a/b", "/a"},
		{"/a", ""},
		{"/", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, parentCgroupPath(tt.in))
		})
	}
}

// Test that on THIS host, own-cgroup-path resolution finds a real,
// existing cpu.max somewhere between the process's own cgroup and the root.
// On a bare systemd host /sys/fs/cgroup/cpu.max does not exist -- only
// /sys/fs/cgroup/user.slice/.../session-*.scope/cpu.max does (confirmed
// empirically) -- so the resolver MUST resolve the process's own path and
// walk it, not read a hardcoded root path that is absent regardless of any
// real quota. This test doesn't require a quota to actually be configured (an
// unconstrained "max" cpu.max is a legitimate resolution outcome) -- it
// requires that the resolver reach and read a REAL file.
func TestOwnCgroupPath_FindsExistingReadableCPUMax_OnThisHost(t *testing.T) {
	path, ok := ownCgroupPath()
	if !ok {
		t.Skip("no cgroup v2 unified-hierarchy line in /proc/self/cgroup on this host (cgroup v1?) -- " +
			"own-path resolution is not applicable; CgroupCPUQuota walks from the cgroup root only")
	}
	require.NotEmpty(t, path, "a v2 host with a real scope should resolve a non-empty own cgroup path")

	full := cgroupRoot + path + "/cpu.max"
	_, statErr := os.Stat(full)
	require.NoErrorf(
		t, statErr, "expected the process's own resolved cgroup path to have a readable cpu.max at %s", full,
	)

	ratio, class, exact := resolveCPUQuota(readFileNoFail)
	t.Logf("own cgroup path=%q cpu.max=%q ratio=%v class=%v exact=%v", path, full, ratio, class, exact)
	// Whatever this host's actual quota state is (constrained or not),
	// CgroupCPUQuota must agree with the tri-state resolver run over the real
	// filesystem: the exported ok=true exactly when the ancestry walk finds a
	// real finite (Limited) quota AND resolved every level cleanly (exact),
	// proving the exported entry point uses the full resolution end to end,
	// not a hardcoded-root read.
	gotQuota, gotOK := CgroupCPUQuota()
	require.Equal(t, class == quotaLimited && exact, gotOK)
	if gotOK {
		require.InDelta(t, ratio, gotQuota, 1e-9)
	}
}

// fakeCgroupFS is a synthetic path -> (content, err) map standing in for the
// real cgroup filesystem, so resolveCPUQuota's full ancestry walk is testable
// without touching real cgroup files. A path absent from the map reads as
// ENOENT -- the benign "no cpu.max at this level" absence a real intermediate
// cgroup returns, which does NOT taint the outcome toward Unknown.
type fakeCgroupFS map[string]struct {
	content string
	err     error
}

func (fs fakeCgroupFS) read(path string) ([]byte, error) {
	f, ok := fs[path]
	if !ok {
		return nil, unix.ENOENT
	}

	return []byte(f.content), f.err
}

// Test that resolveCPUQuota walks the FULL cgroup ancestry and classifies the
// outcome as a tri-state (Limited/Unlimited/Unknown), taking the MINIMUM
// finite quota/period ratio across all readable levels -- CFS bandwidth is
// hierarchical, so the effective limit is the strictest ancestor, not the
// nearest finite one.
func TestResolveCPUQuota_HierarchicalTriState(t *testing.T) {
	// Case (a): nested finite levels where a parent is stricter than the
	// leaf -- the MINIMUM ratio (the parent's 0.5) is the effective quota,
	// not the leaf's 2.0.
	t.Run("a_nested_finite_parent_stricter_returns_min", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "200000 100000\n"}, // 2.0 CPUs
			"/sys/fs/cgroup/a/cpu.max":   {content: "50000 100000\n"},  // 0.5 CPU (stricter)
			"/sys/fs/cgroup/cpu.max":     {content: "max 100000\n"},    // unlimited at root
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaLimited, class)
		require.InDelta(t, 0.5, ratio, 1e-9)
		require.True(t, exact, "every level resolved cleanly -> exact")
	})

	// Case (b): a finite leaf plus an unreadable ancestor (a real read
	// error, not a benign absence) -- still Limited, at the minimum of the
	// readable finite levels. A finite level found anywhere means the quota
	// is confirmed, so one unreadable ancestor does not downgrade the CLASS
	// to Unknown -- the spin policy keeps a usable ratio. But the result is
	// NOT exact: the reported ratio is only the minimum over the readable
	// levels, and a stricter unreadable ancestor may exist, so CgroupCPUQuota
	// must NOT certify it (ok=false) even though the class is Limited.
	t.Run("b_finite_leaf_plus_unreadable_ancestor_is_limited_but_inexact", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "200000 100000\n"}, // 2.0 CPUs
			"/sys/fs/cgroup/a/cpu.max":   {err: unix.EACCES},           // unreadable (real error)
			"/sys/fs/cgroup/cpu.max":     {content: "max 100000\n"},
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaLimited, class)
		require.InDelta(t, 2.0, ratio, 1e-9)
		require.False(t, exact, "an unreadable ancestor makes the effective ratio unprovable")

		// The exported verifier must decline to certify an inexact ratio.
		gotQuota, gotOK := cgroupCPUQuotaVia(fs.read)
		require.False(t, gotOK, "an inexact finite ratio must NOT be certified")
		require.InDelta(t, 2.0, gotQuota, 1e-9, "the ratio is still returned for the spin policy")

		// The spin policy still shrinks on the known ratio (never the full budget).
		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "a finite-but-inexact quota still shrinks the budget")
	})

	// Case (c): NO finite level anywhere PLUS one level unreadable by a real
	// error, so "unlimited" cannot be confirmed. resolveCPUQuota returns
	// Unknown (never Unlimited), and the budget is SHRUNK, not left at the
	// full configured value -- the full budget under an unconfirmable quota
	// is the exact CFS-throttle blowup the quota probe exists to prevent.
	t.Run("c_no_finite_plus_unreadable_is_unknown_and_shrinks_budget", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "max 100000\n"}, // unlimited at this level
			"/sys/fs/cgroup/a/cpu.max":   {err: unix.EACCES},        // unreadable (real error)
			// root /sys/fs/cgroup/cpu.max absent -> benign ENOENT
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class)
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0), "Unknown must not hard-zero the budget")
		require.Less(t, budget, DefaultSpinBudget,
			"Unknown quota must FAIL CLOSED to a shrunk budget, never fail open to the full configured value")
	})

	// Case (d): every level cleanly resolved (readable "max" or a benign
	// absence) up to and including the root, none finite -> Unlimited, exact,
	// and the full configured budget is used.
	t.Run("d_all_clean_max_or_absent_is_unlimited_and_full_budget", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "max 100000\n"},
			// /sys/fs/cgroup/a/cpu.max and /sys/fs/cgroup/cpu.max absent -> ENOENT
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnlimited, class)
		require.True(t, exact)
		require.Equal(t, DefaultSpinBudget, effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class))
	})
}

// Test that an EINTR interrupting the cpu.max read is RETRIED, never mistaken
// for a real read error that would taint the outcome toward Unknown: a signal
// hitting the syscall must not change the resolved quota class.
func TestResolveCPUQuota_RetriesEINTR_NotUnknown(t *testing.T) {
	const leaf = "/sys/fs/cgroup/a/cpu.max"
	var leafCalls int
	read := func(path string) ([]byte, error) {
		switch path {
		case "/proc/self/cgroup":
			return []byte("0::/a\n"), nil
		case leaf:
			leafCalls++
			if leafCalls == 1 {
				return nil, unix.EINTR // signal interrupts the first read
			}

			return []byte("100000 100000\n"), nil // 1.0 CPU on retry
		default:
			return nil, unix.ENOENT // root absent -> benign
		}
	}

	ratio, class, exact := resolveCPUQuota(read)
	require.Equal(t, quotaLimited, class, "EINTR must be retried, not treated as an unreadable level")
	require.InDelta(t, 1.0, ratio, 1e-9)
	require.True(t, exact, "a retried EINTR is a clean read, not an unreadable level")
	require.Equal(t, 2, leafCalls, "the cpu.max read must be retried exactly once past the EINTR")
}

// Test that resolveCPUQuota fails CLOSED on every unconfirmable input: an
// unreadable /proc/self/cgroup and malformed cpu.max content each taint the
// resolution toward Unknown (shrinking the budget) when no finite quota is
// found, and each demotes a finite result to inexact (uncertified) when a
// finite quota is found. Full budget is permitted only for a provably
// unlimited hierarchy.
func TestResolveCPUQuota_FailsClosedOnUnconfirmableInput(t *testing.T) {
	// /proc/self/cgroup is unreadable and no finite quota exists at the
	// root -- the process's own cgroup (which may carry a quota) is unknown,
	// so a root-only walk cannot prove Unlimited. Must be Unknown -> shrink.
	t.Run("proc_cgroup_read_error_no_finite_is_unknown", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup": {err: unix.EACCES}, // own cgroup path unresolved
			// root /sys/fs/cgroup/cpu.max absent -> benign ENOENT, no finite
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class,
			"a /proc/self/cgroup read error with no finite quota must be Unknown, never Unlimited")
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget,
			"an unreadable /proc/self/cgroup must FAIL CLOSED to a shrunk budget")

		// Non-vacuity: the ONLY difference is the /proc/self/cgroup read
		// error. Flip it to a clean read of an unlimited hierarchy and the
		// budget returns to full -- proving the read error, not something
		// else in the fixture, is what forced the shrink.
		clean := fakeCgroupFS{"/proc/self/cgroup": {content: "0::/\n"}}
		r2, c2, e2 := resolveCPUQuota(clean.read)
		require.Equal(t, quotaUnlimited, c2)
		require.True(t, e2)
		require.Equal(t, DefaultSpinBudget, effectiveSpinBudget(DefaultSpinBudget, 4, r2, c2))
	})

	// /proc/self/cgroup unreadable but the root DOES carry a finite quota. The
	// class stays Limited (a usable ratio for the spin policy) but the result
	// is inexact, so CgroupCPUQuota must not certify it.
	t.Run("proc_cgroup_read_error_with_finite_root_is_limited_inexact", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":      {err: unix.EACCES},           // own path unresolved
			"/sys/fs/cgroup/cpu.max": {content: "100000 100000\n"}, // 1.0 CPU at root
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaLimited, class)
		require.InDelta(t, 1.0, ratio, 1e-9)
		require.False(t, exact, "an unreadable own-path level makes the finite result inexact")

		_, gotOK := cgroupCPUQuotaVia(fs.read)
		require.False(t, gotOK, "an inexact finite ratio must not be certified")

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "a finite-but-inexact quota still shrinks")
	})

	// cpu.max content that is readable but matches neither a finite pair nor
	// the canonical "max <period>" keyword is malformed -- unconfirmable, NOT
	// silently treated as unlimited. With no finite quota anywhere it must be
	// Unknown -> shrink.
	t.Run("malformed_cpu_max_no_finite_is_unknown", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "garbage\n"}, // malformed, unconfirmable
			// root absent -> benign ENOENT
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class,
			"malformed cpu.max with no finite quota must be Unknown, never Unlimited")
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "malformed cpu.max must FAIL CLOSED to a shrunk budget")

		// Non-vacuity: the ONLY difference is the malformed content. Flip that
		// one level to the canonical "max <period>" and the hierarchy resolves
		// Unlimited at full budget -- proving the malformed content, not the
		// fixture shape, is what forced the shrink.
		clean := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "max 100000\n"}, // canonical unlimited
		}
		r2, c2, e2 := resolveCPUQuota(clean.read)
		require.Equal(t, quotaUnlimited, c2)
		require.True(t, e2)
		require.Equal(t, DefaultSpinBudget, effectiveSpinBudget(DefaultSpinBudget, 4, r2, c2))
	})

	// malformed cpu.max at an ancestor plus a finite leaf. The class stays
	// Limited on the leaf ratio, but the malformed ancestor makes it inexact,
	// so CgroupCPUQuota must not certify it; the spin policy still shrinks on
	// the leaf ratio.
	t.Run("malformed_ancestor_with_finite_leaf_is_limited_inexact", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "200000 100000\n"}, // 2.0 CPUs (finite leaf)
			"/sys/fs/cgroup/a/cpu.max":   {content: "bogus\n"},         // malformed ancestor
			"/sys/fs/cgroup/cpu.max":     {content: "max 100000\n"},
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaLimited, class)
		require.InDelta(t, 2.0, ratio, 1e-9)
		require.False(t, exact, "a malformed ancestor makes the effective ratio unprovable")

		_, gotOK := cgroupCPUQuotaVia(fs.read)
		require.False(t, gotOK, "an inexact finite ratio must not be certified")

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "a finite-but-inexact quota still shrinks on the leaf ratio")
	})

	// Regression guard: the canonical "max <period>" keyword at EVERY level
	// is a provably unlimited hierarchy -- the malformed-tainting must not
	// misclassify it. Unlimited, exact, full budget.
	t.Run("canonical_max_everywhere_stays_unlimited_full_budget", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "max 100000\n"},
			"/sys/fs/cgroup/a/cpu.max":   {content: "max 100000\n"},
			"/sys/fs/cgroup/cpu.max":     {content: "max 100000\n"},
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnlimited, class, "canonical max at every level must stay Unlimited")
		require.True(t, exact)
		require.Equal(t, DefaultSpinBudget, effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class))
	})
}

// Test that the STRICT, overflow-safe parsers fail closed on content the Linux
// kernel never emits but that a lax parser would accept as clean: a
// non-positive "max" period, a finite pair with a trailing token, an
// overflowing quota, and a present-but-empty "0::" own-path line. Each case is
// paired with a non-vacuity mutation -- flip the single offending byte(s) to a
// canonical form and the outcome returns to the full-budget/finite result --
// proving the strictness, not the fixture shape, is what forced the fail-closed
// classification.
func TestResolveCPUQuota_StrictParse_FailsClosed(t *testing.T) {
	// A non-positive "max" period ("max 0") is nonsensical: it must be
	// malformed and taint toward Unknown, never pass as a confirmed unlimited
	// level. With no finite quota anywhere -> Unknown -> shrink.
	t.Run("max_zero_period_no_finite_is_unknown", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "max 0\n"}, // zero period: nonsensical
			// root /sys/fs/cgroup/cpu.max absent -> benign ENOENT, no finite
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class,
			"a non-positive max period must be malformed -> Unknown, never Unlimited")
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "max 0 must FAIL CLOSED to a shrunk budget")

		// Non-vacuity: only the period changes. A positive period is the
		// canonical unlimited form -> Unlimited at full budget.
		clean := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "max 100000\n"},
		}
		r2, c2, e2 := resolveCPUQuota(clean.read)
		require.Equal(t, quotaUnlimited, c2)
		require.True(t, e2)
		require.Equal(t, DefaultSpinBudget, effectiveSpinBudget(DefaultSpinBudget, 4, r2, c2))
	})

	// A finite pair with a trailing token ("<q> <p> x") is NOT a finite 2.0:
	// the strict parser rejects it, so with no finite quota elsewhere it is
	// Unknown -> shrink.
	t.Run("finite_pair_trailing_token_no_finite_is_unknown", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "200000 100000 x\n"}, // trailing token
			// root absent -> benign ENOENT
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class,
			"a trailing token makes the pair malformed -> Unknown, never a finite 2.0")
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "a trailing token must FAIL CLOSED to a shrunk budget")

		// Non-vacuity: drop the trailing token and the same pair is a real
		// finite 2.0.
		clean := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "200000 100000\n"},
		}
		r2, c2, e2 := resolveCPUQuota(clean.read)
		require.Equal(t, quotaLimited, c2)
		require.InDelta(t, 2.0, r2, 1e-9)
		require.True(t, e2)
	})

	// A trailing-token (malformed) ancestor UNDER a finite leaf keeps the class
	// Limited on the leaf ratio but makes the result inexact, so CgroupCPUQuota
	// must not certify it (ok=false).
	t.Run("trailing_token_ancestor_under_finite_leaf_is_inexact", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "200000 100000\n"},  // finite leaf 2.0
			"/sys/fs/cgroup/a/cpu.max":   {content: "50000 100000 x\n"}, // malformed ancestor
			"/sys/fs/cgroup/cpu.max":     {content: "max 100000\n"},
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaLimited, class)
		require.InDelta(t, 2.0, ratio, 1e-9)
		require.False(t, exact, "a malformed ancestor makes the effective ratio unprovable")

		_, gotOK := cgroupCPUQuotaVia(fs.read)
		require.False(t, gotOK, "an inexact finite ratio must NOT be certified")

		// Non-vacuity: drop the trailing token and the ancestor is a valid,
		// stricter finite 0.5 -- now exact, and certified at 0.5.
		clean := fakeCgroupFS{
			"/proc/self/cgroup":          {content: "0::/a/b\n"},
			"/sys/fs/cgroup/a/b/cpu.max": {content: "200000 100000\n"},
			"/sys/fs/cgroup/a/cpu.max":   {content: "50000 100000\n"},
			"/sys/fs/cgroup/cpu.max":     {content: "max 100000\n"},
		}
		r2, c2, e2 := resolveCPUQuota(clean.read)
		require.Equal(t, quotaLimited, c2)
		require.InDelta(t, 0.5, r2, 1e-9)
		require.True(t, e2)
		gotQuota, gotOK2 := cgroupCPUQuotaVia(clean.read)
		require.True(t, gotOK2, "a fully-readable finite ancestry certifies")
		require.InDelta(t, 0.5, gotQuota, 1e-9)
	})

	// An overflowing quota must be malformed, NOT a wrapped-small finite: the
	// checked ParseInt conversion rejects it, so with no other finite quota it
	// is Unknown -> shrink.
	t.Run("overflowing_quota_is_malformed_not_wrapped_finite", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "99999999999999999999999 100000\n"}, // overflows int64
			// root absent -> benign ENOENT
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class,
			"an overflowing quota must be malformed -> Unknown, not a wrapped small finite")
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "an overflowing quota must FAIL CLOSED to a shrunk budget")

		// Non-vacuity: an in-range quota is a real finite 2.0.
		clean := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "200000 100000\n"},
		}
		r2, c2, e2 := resolveCPUQuota(clean.read)
		require.Equal(t, quotaLimited, c2)
		require.InDelta(t, 2.0, r2, 1e-9)
		require.True(t, e2)
	})

	// A present-but-empty "0::" own-path line ("0::\n") is malformed: the
	// unified hierarchy exists but its path is unusable, so the own level
	// cannot be walked -> taint toward Unknown. With no finite quota -> Unknown
	// -> shrink. This is DISTINCT from a wholly absent "0::" line, which stays
	// the benign root fallback (the regression below).
	t.Run("empty_own_path_line_is_malformed_taints_unknown", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup": {content: "0::\n"}, // present but empty: unusable
			// root absent -> benign ENOENT, no finite anywhere
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaUnknown, class,
			"an empty 0:: own-path is malformed -> taint -> Unknown, never Unlimited")
		require.False(t, exact)

		budget := effectiveSpinBudget(DefaultSpinBudget, 4, ratio, class)
		require.Greater(t, budget, time.Duration(0))
		require.Less(t, budget, DefaultSpinBudget, "an empty 0:: own-path must FAIL CLOSED to a shrunk budget")

		// Regression / non-vacuity: a WHOLLY ABSENT 0:: line (cgroup v1-only)
		// is the benign root fallback, NOT a taint -- only a present-but-empty
		// 0:: line is malformed. Same absent root then resolves Unlimited at
		// full budget.
		benign := fakeCgroupFS{
			"/proc/self/cgroup": {content: "12:cpu,cpuacct:/user.slice\n"}, // no 0:: line
			// root absent -> benign ENOENT
		}
		r2, c2, e2 := resolveCPUQuota(benign.read)
		require.Equal(t, quotaUnlimited, c2,
			"a no-0:: file is the benign root fallback, not a taint")
		require.True(t, e2)
		require.Equal(t, DefaultSpinBudget, effectiveSpinBudget(DefaultSpinBudget, 4, r2, c2))
	})

	// Regression guard: a genuine finite "<q> <p>\n" at the own level with a
	// clean ancestry resolves finite AND exact -- the strict parser must not
	// reject a well-formed pair.
	t.Run("well_formed_finite_pair_still_resolves_finite_exact", func(t *testing.T) {
		fs := fakeCgroupFS{
			"/proc/self/cgroup":        {content: "0::/a\n"},
			"/sys/fs/cgroup/a/cpu.max": {content: "150000 100000\n"}, // 1.5 CPU
			// root absent -> benign ENOENT
		}
		ratio, class, exact := resolveCPUQuota(fs.read)
		require.Equal(t, quotaLimited, class)
		require.InDelta(t, 1.5, ratio, 1e-9)
		require.True(t, exact)

		gotQuota, gotOK := cgroupCPUQuotaVia(fs.read)
		require.True(t, gotOK, "a well-formed, fully-readable finite ancestry certifies")
		require.InDelta(t, 1.5, gotQuota, 1e-9)
	})
}
