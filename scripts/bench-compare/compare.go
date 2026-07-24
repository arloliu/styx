// Command bench-compare gates a shared-memory benchmark run against a checked-in
// baseline. It reads the harness's JSONL result rows (one per repetition), takes
// the median of each gated cell across the repetitions, and enforces the merge
// gate: allocations per operation must not increase, the shared-memory cells must
// stay at or above the absolute speed floor versus the gRPC-over-UDS reference,
// and the shm-vs-gRPC and shm-vs-UDS median ratios must not regress past the
// baseline by more than the allowed tolerance. Absolute p50/p99 latency is
// reported but not gated. The floor, tolerance, reference cells, and repetition
// count all come from the baseline file, never from constants here.
//
// Comparing medians of N repetitions, not single runs, keeps hosted-runner noise
// from tripping the gate: a lone slow repetition is absorbed by the median.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// result is one JSONL row the benchmark harness writes: the subset the gate reads.
// It mirrors the fields of the harness's result row (bench/internal/benchbaseline).
type result struct {
	Impl        string  `json:"impl"`
	PayloadB    int     `json:"payload_bytes"`
	Concurrency int     `json:"concurrency"`
	P50Ns       float64 `json:"p50_ns"`
	P99Ns       float64 `json:"p99_ns"`
	AllocsPerOp float64 `json:"allocs_per_op"`
}

// baseline is the checked-in curated baseline and the gate policy it carries.
type baseline struct {
	Policy policy                  `json:"policy"`
	Cells  map[string]cellBaseline `json:"cells"`
}

type policy struct {
	PayloadB           int      `json:"payload_bytes"`
	Concurrency        int      `json:"concurrency"`
	Reps               int      `json:"reps"`
	AbsoluteFloorRatio float64  `json:"absolute_floor_ratio"`
	RelativeTolerance  float64  `json:"relative_tolerance"`
	NormativeCells     []string `json:"normative_cells"`
	GRPCRef            string   `json:"grpc_ref"`
	UDSRef             string   `json:"uds_ref"`
}

type cellBaseline struct {
	P50US       float64 `json:"p50_us"`
	P99US       float64 `json:"p99_us"`
	AllocsPerOp float64 `json:"allocs_per_op"`
}

// cellMedian is the median of one cell's per-repetition samples.
type cellMedian struct {
	p50US  float64
	p99US  float64
	allocs float64
	reps   int
}

// loadBaseline decodes and validates a baseline file. A missing or malformed file
// is an error, never a silent pass — the gate must never run without a baseline.
func loadBaseline(data []byte) (*baseline, error) {
	var b baseline
	if err := decodeBaseline(data, &b); err != nil {
		return nil, err
	}
	if len(b.Cells) == 0 {
		return nil, errors.New("baseline has no cells")
	}
	if b.Policy.AbsoluteFloorRatio <= 0 {
		return nil, fmt.Errorf("baseline absolute_floor_ratio must be positive, got %g",
			b.Policy.AbsoluteFloorRatio)
	}
	if b.Policy.RelativeTolerance < 0 || b.Policy.RelativeTolerance >= 1 {
		return nil, fmt.Errorf("baseline relative_tolerance must be in [0,1), got %g",
			b.Policy.RelativeTolerance)
	}
	if b.Policy.Reps <= 0 {
		return nil, fmt.Errorf("baseline policy.reps must be positive, got %d", b.Policy.Reps)
	}
	for _, name := range append(append([]string{}, b.Policy.NormativeCells...), b.Policy.GRPCRef, b.Policy.UDSRef) {
		if _, ok := b.Cells[name]; !ok {
			return nil, fmt.Errorf("baseline policy references cell %q with no entry in cells", name)
		}
	}

	return &b, nil
}

// decodeBaseline decodes into b while tolerating the "_comment" documentation key.
func decodeBaseline(data []byte, b *baseline) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("baseline is not valid JSON: %w", err)
	}
	p, ok := raw["policy"]
	if !ok {
		return errors.New("baseline has no policy")
	}
	if err := json.Unmarshal(p, &b.Policy); err != nil {
		return fmt.Errorf("baseline policy is malformed: %w", err)
	}
	if c, ok := raw["cells"]; ok {
		if err := json.Unmarshal(c, &b.Cells); err != nil {
			return fmt.Errorf("baseline cells are malformed: %w", err)
		}
	}

	return nil
}

// parseResults reads JSONL result rows, skipping blank lines. A malformed row is
// an error rather than a silent drop.
func parseResults(r io.Reader) ([]result, error) {
	var out []result
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		var res result
		if err := json.Unmarshal([]byte(text), &res); err != nil {
			return nil, fmt.Errorf("result line %d is not valid JSON: %w", line, err)
		}
		out = append(out, res)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// median returns the median of vals (the mean of the two middle values for an even
// count). It panics on an empty slice; callers guard with a presence check.
func median(vals []float64) float64 {
	s := append([]float64{}, vals...)
	slices.Sort(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}

	return (s[n/2-1] + s[n/2]) / 2
}

// cellMedianFor collects every row matching impl at the policy's payload and
// concurrency and returns the median of its p50, p99, and allocs. ok is false when
// no row matched, so a missing cell is caught rather than treated as zero.
func cellMedianFor(results []result, impl string, payloadB, concurrency int) (cellMedian, bool) {
	var p50, p99, allocs []float64
	for _, r := range results {
		if r.Impl == impl && r.PayloadB == payloadB && r.Concurrency == concurrency {
			p50 = append(p50, r.P50Ns/1000)
			p99 = append(p99, r.P99Ns/1000)
			allocs = append(allocs, r.AllocsPerOp)
		}
	}
	if len(p50) == 0 {
		return cellMedian{}, false
	}

	return cellMedian{p50US: median(p50), p99US: median(p99), allocs: median(allocs), reps: len(p50)}, true
}

// check is one gate check's outcome.
type check struct {
	name    string
	pass    bool
	message string
}

// report is the whole gate outcome: the hard checks that fail the merge, and the
// advisory lines that are printed but never fail it.
type report struct {
	hard      []check
	advisory  []check
	fatal     string // set when the run could not be evaluated at all (missing cell)
	repsOK    bool
	repsWant  int
	repsFound int
}

func (r report) failed() bool {
	if r.fatal != "" {
		return true
	}
	for _, c := range r.hard {
		if !c.pass {
			return true
		}
	}

	return false
}

// evaluate runs the gate over the measured results against the baseline. It never
// invents a threshold: the floor, tolerance, and reference cells come from bl.
func evaluate(bl *baseline, results []result) report {
	var rep report
	rep.repsWant = bl.Policy.Reps

	grpc, okG := cellMedianFor(results, bl.Policy.GRPCRef, bl.Policy.PayloadB, bl.Policy.Concurrency)
	uds, okU := cellMedianFor(results, bl.Policy.UDSRef, bl.Policy.PayloadB, bl.Policy.Concurrency)
	if !okG {
		rep.fatal = fmt.Sprintf("no measured rows for gRPC reference cell %q", bl.Policy.GRPCRef)

		return rep
	}
	if !okU {
		rep.fatal = fmt.Sprintf("no measured rows for uds reference cell %q", bl.Policy.UDSRef)

		return rep
	}
	rep.repsFound = grpc.reps

	for _, cell := range bl.Policy.NormativeCells {
		cur, ok := cellMedianFor(results, cell, bl.Policy.PayloadB, bl.Policy.Concurrency)
		if !ok {
			rep.fatal = fmt.Sprintf("no measured rows for normative cell %q", cell)

			return rep
		}
		base := bl.Cells[cell]

		rep.hard = append(rep.hard, allocCheck(cell, base.AllocsPerOp, cur.allocs))
		rep.hard = append(rep.hard, floorCheck(cell, bl.Policy.AbsoluteFloorRatio, grpc.p50US, cur.p50US))
		rep.hard = append(rep.hard, ratioRegressionCheck(cell, bl.Policy.GRPCRef, bl.Policy.RelativeTolerance,
			bl.Cells[bl.Policy.GRPCRef].P50US, base.P50US, grpc.p50US, cur.p50US))
		rep.hard = append(rep.hard, ratioRegressionCheck(cell, bl.Policy.UDSRef, bl.Policy.RelativeTolerance,
			bl.Cells[bl.Policy.UDSRef].P50US, base.P50US, uds.p50US, cur.p50US))

		rep.advisory = append(rep.advisory, latencyAdvisory(cell, "p50", base.P50US, cur.p50US))
		rep.advisory = append(rep.advisory, latencyAdvisory(cell, "p99", base.P99US, cur.p99US))
	}

	rep.repsOK = rep.repsFound >= rep.repsWant

	return rep
}

// allocCheck fails when the measured median allocations per operation rounds above
// the baseline: a real regression adds at least one whole allocation, while
// sub-allocation float noise rounds away. A measured value at or below the
// baseline — including a counter that reads far lower, i.e. a fresh transport
// counting from zero — is never a regression.
func allocCheck(cell string, base, measured float64) check {
	if math.Round(measured) <= math.Round(base) {
		return check{name: cell + " allocs/op", pass: true,
			message: fmt.Sprintf("allocs/op %s → %s (not increased)", trim(base), f2(measured))}
	}
	pct := (measured - base) / base * 100

	return check{name: cell + " allocs/op", pass: false,
		message: fmt.Sprintf("allocs/op %s → %s, +%s%% above baseline", trim(base), f2(measured), f2(pct))}
}

// floorCheck fails when a normative cell's measured median ratio versus the
// gRPC-over-UDS reference is below the absolute floor — a run below the floor fails
// even if it is within tolerance of the baseline.
func floorCheck(cell string, floor, grpcP50, cellP50 float64) check {
	ratio := grpcP50 / cellP50
	if ratio >= floor {
		return check{name: cell + " absolute floor", pass: true,
			message: fmt.Sprintf("%s ratio vs grpc %sx (floor %sx)", cell, f2(ratio), trim(floor))}
	}

	return check{name: cell + " absolute floor", pass: false,
		message: fmt.Sprintf("%s ratio vs grpc %sx below absolute floor %sx", cell, f2(ratio), trim(floor))}
}

// ratioRegressionCheck fails when a normative cell's measured ratio versus a
// reference has fallen more than the tolerance below the baseline ratio. Both
// ratios are computed from p50 medians, so a slower reference this run does not
// count against the cell.
func ratioRegressionCheck(cell, ref string, tol, refBaseP50, cellBaseP50, refCurP50, cellCurP50 float64) check {
	baseRatio := refBaseP50 / cellBaseP50
	curRatio := refCurP50 / cellCurP50
	bound := baseRatio * (1 - tol)
	name := cell + " ratio vs " + ref
	if curRatio >= bound {
		return check{name: name, pass: true,
			message: fmt.Sprintf("%s ratio vs %s %sx → %sx (bound %sx)",
				cell, ref, f2(baseRatio), f2(curRatio), f2(bound))}
	}
	pct := (baseRatio - curRatio) / baseRatio * 100

	return check{name: name, pass: false,
		message: fmt.Sprintf("%s ratio vs %s %sx → %sx, -%s%% below baseline (max -%s%%)",
			cell, ref, f2(baseRatio), f2(curRatio), f2(pct), trim(tol*100))}
}

// latencyAdvisory reports the measured versus baseline latency delta without
// gating on it — absolute latency is advisory until a dedicated, quiet runner
// exists, because hosted-runner noise moves it far more than the gated ratios.
func latencyAdvisory(cell, stat string, base, measured float64) check {
	pct := (measured - base) / base * 100
	sign := "+"
	if pct < 0 {
		sign = ""
	}

	return check{name: cell + " " + stat, pass: true,
		message: fmt.Sprintf("%s %s %sus → %sus (%s%s%%)", cell, stat, trim(base), f2(measured), sign, f2(pct))}
}

// trim formats a float compactly (no trailing zeros), for a baseline's own clean
// values. f2 formats a derived or measured value to two decimals so ratio and
// percentage messages stay readable.
func trim(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func f2(f float64) string {
	return strconv.FormatFloat(f, 'f', 2, 64)
}

// verifyCPUQuota is the regime guard: it fails when the cgroup v2 cpu.max the
// benchmark ran under does not carry the requested quota, so a cgroup-regime run
// can never silently pass with the quota missing. cpu.max is "MAX PERIOD" in
// microseconds, or "max PERIOD" for no quota; CPUs = MAX/PERIOD.
func verifyCPUQuota(expectedCPUs float64, cpuMax string) error {
	fields := strings.Fields(strings.TrimSpace(cpuMax))
	if len(fields) != 2 {
		return fmt.Errorf("cpu.max is malformed: %q", cpuMax)
	}
	if fields[0] == "max" {
		return fmt.Errorf("no CPU quota installed (cpu.max = %q) but %g CPUs were requested", cpuMax, expectedCPUs)
	}
	quota, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return fmt.Errorf("cpu.max quota %q is not a number: %w", fields[0], err)
	}
	period, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || period == 0 {
		return fmt.Errorf("cpu.max period %q is invalid", fields[1])
	}
	if got := quota / period; math.Abs(got-expectedCPUs) > 0.01 {
		return fmt.Errorf("CPU quota is %.3f CPUs but %g were requested", got, expectedCPUs)
	}

	return nil
}
