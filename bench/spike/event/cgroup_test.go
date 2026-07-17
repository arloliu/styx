package event

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test parseOwnCgroupPath against synthetic /proc/self/cgroup content —
// covering pure cgroup v2 (a single "0::" line), hybrid v1+v2 (numbered
// hierarchy lines alongside the "0::" line), cgroup-v1-only hosts (no
// "0::" line at all), and edge cases — without touching the filesystem.
func TestParseOwnCgroupPath(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantPath string
		wantOK   bool
	}{
		{
			name:     "pure cgroup v2 unified hierarchy",
			content:  "0::/user.slice/user-1000.slice/session-2.scope\n",
			wantPath: "/user.slice/user-1000.slice/session-2.scope",
			wantOK:   true,
		},
		{
			name: "hybrid v1+v2 host: 0:: line among numbered hierarchy lines",
			content: "12:pids:/user.slice/user-1000.slice\n" +
				"11:memory:/user.slice/user-1000.slice\n" +
				"10:cpu,cpuacct:/user.slice/user-1000.slice\n" +
				"0::/user.slice/user-1000.slice/session-2.scope\n",
			wantPath: "/user.slice/user-1000.slice/session-2.scope",
			wantOK:   true,
		},
		{
			name: "cgroup v1 only host: no 0:: line",
			content: "12:pids:/user.slice\n" +
				"11:memory:/user.slice\n" +
				"10:cpu,cpuacct:/user.slice\n",
			wantPath: "",
			wantOK:   false,
		},
		{
			name:     "cgroupns=private container: own path is the root",
			content:  "0::/\n",
			wantPath: "/",
			wantOK:   true,
		},
		{
			name:     "empty path after 0:: prefix",
			content:  "0::\n",
			wantPath: "",
			wantOK:   false,
		},
		{
			name:     "empty content",
			content:  "",
			wantPath: "",
			wantOK:   false,
		},
		{
			name:     "no trailing newline",
			content:  "0::/foo.slice/bar.scope",
			wantPath: "/foo.slice/bar.scope",
			wantOK:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotOK := parseOwnCgroupPath([]byte(tt.content))
			require.Equal(t, tt.wantOK, gotOK)
			require.Equal(t, tt.wantPath, gotPath)
		})
	}
}

// Test parseCPUMax against synthetic cpu.max content — the "max" (no
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
		{name: "zero quota is invalid", content: "0 100000\n", wantQuota: 0, wantOK: false},
		{name: "missing period", content: "200000\n", wantQuota: 0, wantOK: false},
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
// existing cpu.max somewhere between the process's own cgroup and the
// root — the exact defect this fix addresses: the pre-fix code read only
// the hardcoded cgroupRoot+"/cpu.max", which does not exist at all on a
// bare systemd host (confirmed empirically: /sys/fs/cgroup/cpu.max is
// absent here, only /sys/fs/cgroup/user.slice/.../session-*.scope/cpu.max
// exists), so it silently reported ok=false unconditionally regardless of
// any real quota. This test doesn't require a quota to actually be
// configured (an unconstrained "max" cpu.max is still a legitimate
// resolution outcome) — it requires that the resolver reach and read a
// REAL file, not merely fail past a nonexistent root path.
func TestOwnCgroupPath_FindsExistingReadableCPUMax_OnThisHost(t *testing.T) {
	path, ok := ownCgroupPath()
	if !ok {
		t.Skip("no cgroup v2 unified-hierarchy line in /proc/self/cgroup on this host (cgroup v1?) — " +
			"own-path resolution is not applicable; CgroupCPUQuota falls back to the pre-fix root-only behavior")
	}
	require.NotEmpty(t, path, "a v2 host with a real scope should resolve a non-empty own cgroup path")

	full := cgroupRoot + path + "/cpu.max"
	_, statErr := os.Stat(full)
	require.NoErrorf(t, statErr, "expected the process's own resolved cgroup path to have a readable cpu.max at %s", full)

	quota, quotaOK := quotaFromPathUpward(path)
	t.Logf("own cgroup path=%q cpu.max=%q quota=%v ok=%v", path, full, quota, quotaOK)
	// Whatever this host's actual quota state is (constrained or not),
	// CgroupCPUQuota must agree with quotaFromPathUpward called directly
	// on the resolved own path — proving the exported entry point uses
	// the fixed resolution end to end, not the old hardcoded-root read.
	gotQuota, gotOK := CgroupCPUQuota()
	require.Equal(t, quotaOK, gotOK)
	if quotaOK {
		require.InDelta(t, quota, gotQuota, 1e-9)
	}
}
