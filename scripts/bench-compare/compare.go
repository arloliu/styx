// Command bench-compare gates a shared-memory benchmark against a checked-in
// baseline. It reads JSONL result rows from the harness (one per repetition),
// computes the median of each cell across repetitions, and enforces merge gates:
// allocations per operation must not increase; shared-memory cells must stay at
// or above the absolute floor versus gRPC-over-UDS; and shm-vs-reference ratios
// must not regress beyond the allowed tolerance. Absolute latency is reported but
// not gated; all thresholds come from the baseline file, not from constants.
//
// Comparing medians of repetitions (not single runs) absorbs hosted-runner noise:
// a single slow run is absorbed by the median. Every cell must provide the full
// repetition count and all finite measurements, or the gate fails closed.
//
// Gaming-resistance scope: the ratio gates are common-mode-invariant by
// construction, so latency movements shared across both codebases on one runner
// show only in advisory deltas, not hard gates. Identity is anchored on
// machine-invariant allocation counts, which are hard-gated on both reference and
// normative cells.
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
	if len(b.Policy.NormativeCells) == 0 {
		return nil, errors.New("baseline policy.normative_cells is empty; the gate would run no hard checks")
	}
	if b.Policy.GRPCRef == "" || b.Policy.UDSRef == "" {
		return nil, errors.New("baseline policy.grpc_ref and uds_ref must both be set")
	}
	for _, name := range append(append([]string{}, b.Policy.NormativeCells...), b.Policy.GRPCRef, b.Policy.UDSRef) {
		c, ok := b.Cells[name]
		if !ok {
			return nil, fmt.Errorf("baseline policy references cell %q with no entry in cells", name)
		}
		if err := validateBaselineCell(name, c); err != nil {
			return nil, err
		}
	}

	return &b, nil
}

// validateBaselineCell requires a baseline cell's latencies and allocations to be
// present, strictly positive, and finite. A missing field decodes to zero and a
// non-finite one to a non-finite float; both are rejected. It is a standalone
// function so a test can witness the finiteness rejection directly, since JSON
// cannot carry a NaN or an infinity to reach it through decoding.
func validateBaselineCell(name string, c cellBaseline) error {
	if !isPositiveFinite(c.P50US) || !isPositiveFinite(c.P99US) || !isPositiveFinite(c.AllocsPerOp) {
		return fmt.Errorf("baseline cell %q has a missing, non-positive, or non-finite field", name)
	}

	return nil
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

// rawResult decodes a result row with pointer measurement fields, so a field that
// is absent (nil) is distinguishable from one that is present and zero. A missing
// measurement must be a hard error, not silently read as zero — a zero p50 would
// make a ratio infinite and a zero allocs would look like an improvement.
type rawResult struct {
	Impl        string   `json:"impl"`
	PayloadB    int      `json:"payload_bytes"`
	Concurrency int      `json:"concurrency"`
	P50Ns       *float64 `json:"p50_ns"`
	P99Ns       *float64 `json:"p99_ns"`
	AllocsPerOp *float64 `json:"allocs_per_op"`
}

// parseResults reads JSONL result rows, skipping blank lines. A row that is not
// valid JSON, is missing a measurement, or carries a non-finite, non-positive
// latency (or a negative allocation) is a hard error — the gate fails closed on
// malformed input rather than treating a missing field as zero.
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
		var raw rawResult
		if err := json.Unmarshal([]byte(text), &raw); err != nil {
			return nil, fmt.Errorf("result line %d is not valid JSON: %w", line, err)
		}
		res, err := raw.validate()
		if err != nil {
			return nil, fmt.Errorf("result line %d (%s): %w", line, raw.Impl, err)
		}
		out = append(out, res)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// validate converts a decoded row to a result, requiring every measurement to be
// present and finite: p50 and p99 strictly positive (a zero would corrupt a
// ratio), allocations present and non-negative (a genuine reset to zero is a
// legitimate improvement, not a missing field).
func (raw rawResult) validate() (result, error) {
	if raw.P50Ns == nil || raw.P99Ns == nil || raw.AllocsPerOp == nil {
		return result{}, errors.New("missing p50_ns, p99_ns, or allocs_per_op")
	}
	if !isPositiveFinite(*raw.P50Ns) || !isPositiveFinite(*raw.P99Ns) {
		return result{}, fmt.Errorf("p50_ns/p99_ns must be positive and finite, got %g/%g", *raw.P50Ns, *raw.P99Ns)
	}
	if math.IsNaN(*raw.AllocsPerOp) || math.IsInf(*raw.AllocsPerOp, 0) || *raw.AllocsPerOp < 0 {
		return result{}, fmt.Errorf("allocs_per_op must be finite and non-negative, got %g", *raw.AllocsPerOp)
	}

	return result{
		Impl: raw.Impl, PayloadB: raw.PayloadB, Concurrency: raw.Concurrency,
		P50Ns: *raw.P50Ns, P99Ns: *raw.P99Ns, AllocsPerOp: *raw.AllocsPerOp,
	}, nil
}

// isPositiveFinite reports whether f is a real, strictly positive number.
func isPositiveFinite(f float64) bool {
	return f > 0 && !math.IsInf(f, 0) && !math.IsNaN(f)
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
	hard     []check
	advisory []check
	fatal    string // set when the run could not be evaluated at all (missing cell)
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
//
// Gaming-resistance scope. The ratio gates are common-mode-invariant by
// construction: scaling every cell's latency equally leaves the ratios unchanged,
// so a shared latency movement across both codebases (the styx transport and
// hashicorp's gRPC plugin) on one runner does not trip them. That is deliberate —
// a common-mode movement on a shared runner is environmental with high
// probability, is surfaced by the advisory absolute deltas, and cannot be
// hard-gated without a dedicated, quiet runner (a rolling per-runner baseline is
// the future lever if it ever matters). Identity is anchored where it is
// machine-invariant instead: allocations per operation are hard-gated on the
// reference cells too, so a changed reference implementation that would silently
// re-anchor the ratios fails the gate rather than passing.
func evaluate(bl *baseline, results []result) report {
	var rep report

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

	// The reference cells owe the full repetition count and must keep their
	// allocation identity, so regressing them cannot silently re-anchor the ratios.
	rep.hard = append(rep.hard,
		repsCheck(bl.Policy.GRPCRef, grpc.reps, bl.Policy.Reps),
		repsCheck(bl.Policy.UDSRef, uds.reps, bl.Policy.Reps),
		allocCheck(bl.Policy.GRPCRef, bl.Cells[bl.Policy.GRPCRef].AllocsPerOp, grpc.allocs),
		allocCheck(bl.Policy.UDSRef, bl.Cells[bl.Policy.UDSRef].AllocsPerOp, uds.allocs),
	)

	for _, cell := range bl.Policy.NormativeCells {
		cur, ok := cellMedianFor(results, cell, bl.Policy.PayloadB, bl.Policy.Concurrency)
		if !ok {
			rep.fatal = fmt.Sprintf("no measured rows for normative cell %q", cell)

			return rep
		}
		base := bl.Cells[cell]

		rep.hard = append(rep.hard,
			repsCheck(cell, cur.reps, bl.Policy.Reps),
			allocCheck(cell, base.AllocsPerOp, cur.allocs),
			floorCheck(cell, bl.Policy.AbsoluteFloorRatio, grpc.p50US, cur.p50US),
			ratioRegressionCheck(cell, bl.Policy.GRPCRef, bl.Policy.RelativeTolerance,
				bl.Cells[bl.Policy.GRPCRef].P50US, base.P50US, grpc.p50US, cur.p50US),
			ratioRegressionCheck(cell, bl.Policy.UDSRef, bl.Policy.RelativeTolerance,
				bl.Cells[bl.Policy.UDSRef].P50US, base.P50US, uds.p50US, cur.p50US),
		)
		rep.advisory = append(rep.advisory,
			latencyAdvisory(cell, "p50", base.P50US, cur.p50US),
			latencyAdvisory(cell, "p99", base.P99US, cur.p99US))
	}

	return rep
}

// repsCheck fails when a cell was sampled fewer times than the baseline's
// repetition count. The gate compares medians of N repetitions to absorb noise, so
// a cell with too few rows was never properly sampled; the count is enforced
// independently for every cell rather than inferred from one.
func repsCheck(cell string, got, want int) check {
	if got >= want {
		return check{name: cell + " reps", pass: true,
			message: fmt.Sprintf("%s sampled %d times (need %d)", cell, got, want)}
	}

	return check{name: cell + " reps", pass: false,
		message: fmt.Sprintf("%s sampled only %d times, need %d", cell, got, want)}
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
