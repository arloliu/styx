package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fixtures below are synthetic — round numbers chosen to exercise the gate's
// arithmetic, never presented as measured results. baselineRatio10 has a
// shm-vs-grpc baseline ratio of 10x (regression bound 9x at 10% tolerance) and
// baselineRatioNearFloor a ratio of 7.5x (bound 6.75x), so a measured ratio in
// [6.75x, 7.0x) is within tolerance yet below the 7x absolute floor.
const baselineRatio10 = `{
  "policy": {"payload_bytes":64,"concurrency":1,"reps":3,"absolute_floor_ratio":7.0,
             "relative_tolerance":0.10,"normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds"},
  "cells": {
    "shm":  {"p50_us":10,"p99_us":20,"allocs_per_op":19},
    "grpc": {"p50_us":100,"p99_us":200,"allocs_per_op":150},
    "uds":  {"p50_us":40,"p99_us":80,"allocs_per_op":17}
  }
}`

// baselineGRPCAllocBand mirrors the shape of the checked-in shm baseline's
// grpc reference cell — a 158 allocs/op mean with a 1%-or-2-allocations
// tolerance band (2.0 dominates at this base) — so tests can pin the gate's
// behavior against the real tolerance shape without touching the checked-in
// baseline file.
const baselineGRPCAllocBand = `{
  "policy": {"payload_bytes":64,"concurrency":1,"reps":3,"absolute_floor_ratio":7.0,
             "relative_tolerance":0.10,"normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds",
             "grpc_alloc_rel_tolerance":0.01,"grpc_alloc_abs_tolerance":2.0},
  "cells": {
    "shm":  {"p50_us":10,"p99_us":20,"allocs_per_op":19},
    "grpc": {"p50_us":100,"p99_us":200,"allocs_per_op":158},
    "uds":  {"p50_us":40,"p99_us":80,"allocs_per_op":17}
  }
}`

// refRowsForBand returns three clean repetitions each of the grpc and uds
// reference cells at baselineGRPCAllocBand's own values, for tests that only
// vary the grpc allocation count.
func refRowsForBand(grpcAllocs float64) []string {
	out := make([]string, 0, 6)
	for range 3 {
		out = append(out, row("grpc", 100, 200, grpcAllocs), row("uds", 40, 80, 17))
	}

	return out
}

// grpcAllocCheckFailed reports whether the grpc allocs/op hard check failed in
// rep.
func grpcAllocCheckFailed(rep report) bool {
	for _, c := range rep.hard {
		if c.name == "grpc allocs/op" && !c.pass {
			return true
		}
	}

	return false
}

// Test the exact defect this tolerance band exists to fix: a gRPC reference
// cell measured at 158.62 against a baseline of 158.0 — the observed value
// that, under the old strict rounding check, rounded to 159 and failed
// permanently every run. It must now pass.
func TestEvaluate_GRPCAllocBandAbsorbsObservedJitter(t *testing.T) {
	rows := append(refRowsForBand(158.62), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	rep := evalRows(t, baselineGRPCAllocBand, rows...)

	if grpcAllocCheckFailed(rep) {
		t.Fatalf("158.62 against a 158.0 baseline must pass under the tolerance band: %+v", rep.hard)
	}
}

// Test the old rounding cliff directly: 158.5 used to be the exact point
// where math.Round flipped the verdict (rounds to 159, above baseline's
// rounded 158). With the tolerance band, values on both sides of that old
// cliff must pass — the cliff no longer determines the outcome.
func TestEvaluate_GRPCAllocBandRemovesOldRoundingCliff(t *testing.T) {
	for _, measured := range []float64{158.49, 158.5, 158.51} {
		rows := append(refRowsForBand(measured), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
		rep := evalRows(t, baselineGRPCAllocBand, rows...)
		if grpcAllocCheckFailed(rep) {
			t.Errorf("measured %g must pass: the old rounding cliff at 158.5 must no longer decide the outcome: %+v",
				measured, rep.hard)
		}
	}
}

// Test the tolerance band's own boundary: base (158) + band (2.0) = 160.0.
// Exactly at the boundary must pass (the check is inclusive); just past it
// must fail. This is the new cliff the band creates, and it must sit where
// the policy fields say it does, not somewhere the code invented.
func TestEvaluate_GRPCAllocBandBoundaryIsBaseplusBand(t *testing.T) {
	pass := evalRows(t, baselineGRPCAllocBand,
		append(refRowsForBand(160.0), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))...)
	if grpcAllocCheckFailed(pass) {
		t.Fatalf("measured exactly at base+band (160.0) must pass: %+v", pass.hard)
	}

	fail := evalRows(t, baselineGRPCAllocBand,
		append(refRowsForBand(160.01), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))...)
	if !grpcAllocCheckFailed(fail) {
		t.Fatal("measured just past base+band (160.01) must fail")
	}
}

// Test the anti-gaming property survives the band: a genuine reference
// implementation change moves the gRPC mean far past the tolerance band (158
// -> 175, versus a 160.0 band boundary), and must still fail the gate. A
// dependency bump or implementation swap moves allocs/op by tens, not tenths,
// so the band cannot be widened enough to absorb real jitter without also
// absorbing this — proving the two are distinguishable.
func TestEvaluate_GRPCAllocBandStillCatchesRealImplementationChange(t *testing.T) {
	rows := append(refRowsForBand(175), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	rep := evalRows(t, baselineGRPCAllocBand, rows...)

	if !grpcAllocCheckFailed(rep) {
		t.Fatal("a reference allocation count that moved from 158 to 175 must still fail the gate")
	}
}

// Test that the uds reference cell keeps the strict, integer-rounded
// allocation check despite also being a reference cell: the axis that earns a
// tolerance band is third-party code we do not control, not "reference cell,"
// and uds is styx's own transport. Sub-allocation float noise (17.04) still
// passes, but a genuine extra allocation (18.1, rounding to 18) still fails —
// the same strict behavior as any normative cell, with no band applied.
func TestEvaluate_UDSRefAllocsStayStrictDespiteBeingAReferenceCell(t *testing.T) {
	rowsFor := func(udsAllocs float64) []string {
		return []string{
			row("grpc", 100, 200, 158), row("grpc", 100, 200, 158), row("grpc", 100, 200, 158),
			row("uds", 40, 80, udsAllocs), row("uds", 40, 80, udsAllocs), row("uds", 40, 80, udsAllocs),
			row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19),
		}
	}
	udsAllocCheckFailed := func(rep report) bool {
		for _, c := range rep.hard {
			if c.name == "uds allocs/op" && !c.pass {
				return true
			}
		}

		return false
	}

	pass := evalRows(t, baselineGRPCAllocBand, rowsFor(17.04)...)
	if udsAllocCheckFailed(pass) {
		t.Errorf("sub-allocation float noise (17.04) on the uds reference must still pass: %+v", pass.hard)
	}

	fail := evalRows(t, baselineGRPCAllocBand, rowsFor(18.1)...)
	if !udsAllocCheckFailed(fail) {
		t.Error("a genuine extra allocation (18.1) on the uds reference must still fail; it gets no tolerance band")
	}
}

// Test that a normative styx cell keeps the strict, integer-rounded
// allocation check unaffected by the gRPC tolerance band: sub-allocation float
// noise (19.04) passes, a genuine regression (20.1) fails.
func TestEvaluate_NormativeCellAllocsStayStrictAlongsideGRPCBand(t *testing.T) {
	rowsFor := func(shmAllocs float64) []string {
		return []string{
			row("grpc", 100, 200, 158), row("grpc", 100, 200, 158), row("grpc", 100, 200, 158),
			row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
			row("shm", 10, 20, shmAllocs), row("shm", 10, 20, shmAllocs), row("shm", 10, 20, shmAllocs),
		}
	}
	shmAllocCheckFailed := func(rep report) bool {
		for _, c := range rep.hard {
			if c.name == "shm allocs/op" && !c.pass {
				return true
			}
		}

		return false
	}

	pass := evalRows(t, baselineGRPCAllocBand, rowsFor(19.04)...)
	if shmAllocCheckFailed(pass) {
		t.Errorf("sub-allocation float noise (19.04) on a normative cell must still pass: %+v", pass.hard)
	}

	fail := evalRows(t, baselineGRPCAllocBand, rowsFor(20.1)...)
	if !shmAllocCheckFailed(fail) {
		t.Error("a genuine regression (20.1) on a normative cell must still fail; it gets no tolerance band")
	}
}

// Test that an absent tolerance field defaults to the strict check: baselines
// that predate this policy field (or simply don't set it) must not silently
// gain a tolerance band on their gRPC reference cell. baselineRatio10 has
// neither grpc_alloc_rel_tolerance nor grpc_alloc_abs_tolerance set, so a
// fractional bump that would round away (150 -> 150.4) passes, but a whole
// extra allocation (150 -> 151.1, rounding to 151) still fails exactly as it
// did before this field existed.
func TestEvaluate_AbsentGRPCAllocToleranceDefaultsToStrict(t *testing.T) {
	grpcAllocCheckFailedNamed := func(rep report) bool {
		for _, c := range rep.hard {
			if c.name == "grpc allocs/op" && !c.pass {
				return true
			}
		}

		return false
	}
	rowsFor := func(grpcAllocs float64) []string {
		return []string{
			row("grpc", 100, 200, grpcAllocs), row("grpc", 100, 200, grpcAllocs), row("grpc", 100, 200, grpcAllocs),
			row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
			row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19),
		}
	}

	noise := evalRows(t, baselineRatio10, rowsFor(150.4)...)
	if grpcAllocCheckFailedNamed(noise) {
		t.Errorf("sub-allocation float noise must still round away with no tolerance field set: %+v", noise.hard)
	}

	up := evalRows(t, baselineRatio10, rowsFor(151.1)...)
	if !grpcAllocCheckFailedNamed(up) {
		t.Error("a whole extra allocation must still fail when no tolerance field is set")
	}
}

const baselineRatioNearFloor = `{
  "policy": {"payload_bytes":64,"concurrency":1,"reps":3,"absolute_floor_ratio":7.0,
             "relative_tolerance":0.10,"normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds"},
  "cells": {
    "shm":  {"p50_us":10,"p99_us":20,"allocs_per_op":19},
    "grpc": {"p50_us":75,"p99_us":150,"allocs_per_op":150},
    "uds":  {"p50_us":40,"p99_us":80,"allocs_per_op":17}
  }
}`

// row builds one JSONL result row at the fixed 64 B / c=1 gated cell.
func row(impl string, p50us, p99us, allocs float64) string {
	return fmt.Sprintf(
		`{"impl":%q,"payload_bytes":64,"concurrency":1,"p50_ns":%g,"p99_ns":%g,"allocs_per_op":%g}`,
		impl, p50us*1000, p99us*1000, allocs)
}

// evalRows parses a baseline and JSONL rows and runs the gate.
func evalRows(t *testing.T, baselineJSON string, rows ...string) report {
	t.Helper()
	bl, err := loadBaseline([]byte(baselineJSON))
	if err != nil {
		t.Fatalf("loadBaseline: %v", err)
	}
	results, err := parseResults(strings.NewReader(strings.Join(rows, "\n")))
	if err != nil {
		t.Fatalf("parseResults: %v", err)
	}

	return evaluate(bl, results)
}

// refRows returns three clean repetitions each of the grpc and uds reference cells
// at the baselineRatio10 baseline's values, so a test only needs to vary shm.
func refRows() []string {
	out := make([]string, 0, 6)
	for range 3 {
		out = append(out, row("grpc", 100, 200, 150), row("uds", 40, 80, 17))
	}

	return out
}

// Test that a clean run — shm at its baseline ratios and allocs — passes every
// hard gate.
func TestEvaluate_CleanRunPasses(t *testing.T) {
	// Given / When
	rows := append(refRows(), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	rep := evalRows(t, baselineRatio10, rows...)

	// Then
	if rep.failed() {
		t.Fatalf("clean run must pass; report: %+v", rep.hard)
	}
}

// Test the floor's exact bound: a measured ratio of exactly the 7x floor passes
// (>= is inclusive), and just below it fails — isolated on the near-floor baseline
// whose regression bound (6.75x) sits under the floor.
func TestEvaluate_FloorExactBound(t *testing.T) {
	// grpc 70us / shm 10us = 7.0x exactly.
	pass := evalRows(t, baselineRatioNearFloor,
		row("grpc", 70, 150, 150), row("grpc", 70, 150, 150), row("grpc", 70, 150, 150),
		row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	if pass.failed() {
		t.Fatalf("a ratio exactly at the floor must pass: %+v", pass.hard)
	}

	// grpc 69.9us / shm 10us = 6.99x, just below the floor.
	fail := evalRows(t, baselineRatioNearFloor,
		row("grpc", 69.9, 150, 150), row("grpc", 69.9, 150, 150), row("grpc", 69.9, 150, 150),
		row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	if !fail.failed() {
		t.Fatal("a ratio just below the floor must fail")
	}
}

// Test the required case: a run within tolerance of the baseline ratio but below
// the absolute floor must fail. On the near-floor baseline (baseline 7.5x, bound
// 6.75x), a measured 6.99x is within tolerance yet under the 7x floor.
func TestEvaluate_WithinToleranceButBelowFloorFails(t *testing.T) {
	rep := evalRows(t, baselineRatioNearFloor,
		row("grpc", 69.9, 150, 150), row("grpc", 69.9, 150, 150), row("grpc", 69.9, 150, 150),
		row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))

	if !rep.failed() {
		t.Fatal("within-tolerance-but-below-floor must fail the gate")
	}
	var floorFailed, regressionPassed bool
	for _, c := range rep.hard {
		if strings.Contains(c.name, "absolute floor") && !c.pass {
			floorFailed = true
		}
		if strings.Contains(c.name, "ratio vs grpc") && c.pass {
			regressionPassed = true
		}
	}
	if !floorFailed {
		t.Error("the floor check must be the one that failed")
	}
	if !regressionPassed {
		t.Error("the regression check must pass (the run is within tolerance)")
	}
}

// Test that a ratio that has regressed past the tolerance fails even while it
// clears the absolute floor.
func TestEvaluate_RatioRegressionFails(t *testing.T) {
	// shm 12.5us: grpc 100/12.5 = 8x — above the 7x floor, below the 9x bound.
	rows := append(refRows(), row("shm", 12.5, 25, 19), row("shm", 12.5, 25, 19), row("shm", 12.5, 25, 19))
	rep := evalRows(t, baselineRatio10, rows...)

	if !rep.failed() {
		t.Fatal("an 8x ratio against a 9x regression bound must fail")
	}
	for _, c := range rep.hard {
		if strings.Contains(c.name, "absolute floor") && !c.pass {
			t.Error("the floor (7x) should still pass at 8x; only the regression bound should fail")
		}
	}
}

// Test allocations: an increase of a whole allocation fails, an equal count
// passes, and sub-allocation float noise does not fail.
func TestEvaluate_Allocs(t *testing.T) {
	// Equal (19 -> 19) passes.
	eq := evalRows(t, baselineRatio10, append(refRows(),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))...)
	if eq.failed() {
		t.Errorf("equal allocs must pass: %+v", eq.hard)
	}

	// Float noise (19.03) rounds to 19, passes.
	noise := evalRows(t, baselineRatio10, append(refRows(),
		row("shm", 10, 20, 19.03), row("shm", 10, 20, 19.03), row("shm", 10, 20, 19.03))...)
	if noise.failed() {
		t.Errorf("sub-allocation float noise must not fail: %+v", noise.hard)
	}

	// A whole extra allocation (20.1 -> 20) fails.
	up := evalRows(t, baselineRatio10, append(refRows(),
		row("shm", 10, 20, 20.1), row("shm", 10, 20, 20.1), row("shm", 10, 20, 20.1))...)
	if !up.failed() {
		t.Error("an added allocation must fail")
	}
}

// Test that a counter reading far below the baseline — a fresh transport counting
// from zero — is treated as a reset (an improvement), never a regression.
func TestEvaluate_CounterResetNotRegression(t *testing.T) {
	rep := evalRows(t, baselineRatio10, append(refRows(),
		row("shm", 10, 20, 0), row("shm", 10, 20, 0), row("shm", 10, 20, 0))...)

	if rep.failed() {
		t.Fatalf("a reset-to-zero allocation counter must not fail: %+v", rep.hard)
	}
	for _, c := range rep.hard {
		if strings.Contains(c.name, "allocs") && !strings.Contains(c.message, "not increased") {
			t.Errorf("reset should read as not-increased, got %q", c.message)
		}
	}
}

// Test that the median absorbs a single noisy repetition: two clean reps and one
// wild outlier still yield the clean median, so the gate passes.
func TestEvaluate_NoisyRepetitionMedianAbsorbsOutlier(t *testing.T) {
	rows := append(refRows(),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 50, 100, 19)) // one 5x-slow outlier
	rep := evalRows(t, baselineRatio10, rows...)

	if rep.failed() {
		t.Fatalf("the median of [10,10,50] is 10us and must pass; a mean would not: %+v", rep.hard)
	}
}

// Test that a missing normative cell is a fatal evaluation error, not a pass.
func TestEvaluate_MissingCellIsFatal(t *testing.T) {
	rep := evalRows(t, baselineRatio10, refRows()...) // no shm rows at all
	if !rep.failed() {
		t.Fatal("a run missing the normative cell must fail")
	}
	if rep.fatal == "" {
		t.Error("a missing normative cell should be reported as fatal")
	}
}

// Test that too few repetitions is a hard failure, enforced independently for a
// normative cell and for a reference cell — not inferred from one cell's count.
func TestEvaluate_ShortRepCountFails(t *testing.T) {
	// Normative cell short: only 2 shm rows against reps=3.
	shmShort := evalRows(t, baselineRatio10, append(refRows(),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19))...)
	if !shmShort.failed() {
		t.Error("a normative cell with fewer than N repetitions must fail")
	}

	// Reference cell short: 2 grpc rows, full shm and uds.
	grpcShort := evalRows(t, baselineRatio10,
		row("grpc", 100, 200, 150), row("grpc", 100, 200, 150),
		row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	if !grpcShort.failed() {
		t.Error("a reference cell with fewer than N repetitions must fail")
	}
}

// Test that result rows fail closed on every malformed shape: a missing
// measurement, an out-of-range literal (1e999 — encoding/json rejects it before
// decoding, itself a fail-closed rejection), a non-positive latency, or a negative
// allocation — while a present zero allocation (a legitimate reset) and a
// well-formed row are accepted. Actual non-finite floats (NaN/Inf) cannot appear
// in JSON, so the finiteness checks in validate are witnessed directly in
// TestRawResult_RejectsNonFinite.
func TestParseResults_RejectsMissingAndNonFinite(t *testing.T) {
	// rawRow builds a result row; an empty argument omits that field.
	rawRow := func(p50, p99, allocs string) string {
		parts := []string{`"impl":"shm"`, `"payload_bytes":64`, `"concurrency":1`}
		if p50 != "" {
			parts = append(parts, `"p50_ns":`+p50)
		}
		if p99 != "" {
			parts = append(parts, `"p99_ns":`+p99)
		}
		if allocs != "" {
			parts = append(parts, `"allocs_per_op":`+allocs)
		}

		return "{" + strings.Join(parts, ",") + "}"
	}
	cases := []struct {
		name    string
		row     string
		wantErr bool
	}{
		{"missing p50_ns", rawRow("", "20000", "19"), true},
		{"missing p99_ns", rawRow("10000", "", "19"), true},
		{"missing allocs", rawRow("10000", "20000", ""), true},
		{"out-of-range p50_ns (decode error)", rawRow("1e999", "20000", "19"), true},
		{"out-of-range p99_ns (decode error)", rawRow("10000", "1e999", "19"), true},
		{"out-of-range allocs (decode error)", rawRow("10000", "20000", "1e999"), true},
		{"zero p50_ns", rawRow("0", "20000", "19"), true},
		{"negative p99_ns", rawRow("10000", "-5", "19"), true},
		{"negative allocs", rawRow("10000", "20000", "-1"), true},
		{"present zero allocs (reset)", rawRow("10000", "20000", "0"), false},
		{"well-formed", rawRow("10000", "20000", "19"), false},
	}
	for _, tc := range cases {
		_, err := parseResults(strings.NewReader(tc.row))
		if tc.wantErr && err == nil {
			t.Errorf("%s: expected an error, got none", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: expected no error, got %v", tc.name, err)
		}
	}
}

// Test the finiteness branches directly: JSON cannot carry a NaN or an infinity —
// a NaN literal is a decode error and an over-range literal like 1e999 is rejected
// by encoding/json before decoding — so these values cannot reach validate through
// parseResults. Production parsing invokes the same validate, so witnessing it here
// with real non-finite floats proves the finiteness checks are load-bearing.
func TestRawResult_RejectsNonFinite(t *testing.T) {
	inf, ninf, nan, ok := math.Inf(1), math.Inf(-1), math.NaN(), 10000.0
	cases := []struct {
		name        string
		p50, p99, a float64
	}{
		{"nan p50", nan, ok, ok},
		{"nan p99", ok, nan, ok},
		{"nan allocs", ok, ok, nan},
		{"+inf p50", inf, ok, ok},
		{"+inf p99", ok, inf, ok},
		{"+inf allocs", ok, ok, inf},
		{"-inf p50", ninf, ok, ok},
		{"-inf allocs", ok, ok, ninf},
	}
	for _, tc := range cases {
		p50, p99, a := tc.p50, tc.p99, tc.a
		raw := rawResult{Impl: "shm", PayloadB: 64, Concurrency: 1, P50Ns: &p50, P99Ns: &p99, AllocsPerOp: &a}
		if _, err := raw.validate(); err == nil {
			t.Errorf("%s: a non-finite measurement must be rejected", tc.name)
		}
	}
}

// Test the baseline cell validator directly with non-finite fields, for the same
// reason: an infinity or NaN cannot be decoded from JSON into a cell, so the
// finiteness rejection is witnessed by calling validateBaselineCell — the exact
// function loadBaseline calls — with real non-finite floats.
func TestValidateBaselineCell_RejectsNonFiniteAndNonPositive(t *testing.T) {
	inf, nan := math.Inf(1), math.NaN()
	cases := []struct {
		name string
		cell cellBaseline
	}{
		{"+inf p50", cellBaseline{P50US: inf, P99US: 20, AllocsPerOp: 19}},
		{"+inf p99", cellBaseline{P50US: 10, P99US: inf, AllocsPerOp: 19}},
		{"+inf allocs", cellBaseline{P50US: 10, P99US: 20, AllocsPerOp: inf}},
		{"nan p50", cellBaseline{P50US: nan, P99US: 20, AllocsPerOp: 19}},
		{"zero p50", cellBaseline{P50US: 0, P99US: 20, AllocsPerOp: 19}},
		{"negative allocs", cellBaseline{P50US: 10, P99US: 20, AllocsPerOp: -1}},
	}
	for _, tc := range cases {
		if err := validateBaselineCell("x", tc.cell); err == nil {
			t.Errorf("%s: must be rejected", tc.name)
		}
	}
}

// Test that a reference cell whose allocation identity changed fails the gate: a
// changed reference implementation would otherwise silently re-anchor the ratios.
// Allocations per operation are machine-invariant, so this is a sound anchor.
func TestEvaluate_ReferenceAllocsAnchor(t *testing.T) {
	rep := evalRows(t, baselineRatio10,
		row("grpc", 100, 200, 200), row("grpc", 100, 200, 200), row("grpc", 100, 200, 200), // 150 -> 200
		row("uds", 40, 80, 17), row("uds", 40, 80, 17), row("uds", 40, 80, 17),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))

	if !rep.failed() {
		t.Fatal("a changed reference allocation count must fail the gate")
	}
	var grpcAllocFailed bool
	for _, c := range rep.hard {
		if c.name == "grpc allocs/op" && !c.pass {
			grpcAllocFailed = true
		}
	}
	if !grpcAllocFailed {
		t.Error("the reference (grpc) allocation check must be the one that failed")
	}
}

// Test the documented gaming-resistance scope: a common-mode latency movement —
// every cell's p50 doubled, allocations unchanged — leaves every ratio and the
// floor unchanged, so no hard gate trips; it surfaces only in the advisory absolute
// delta. This is by construction (a shared-runner environmental shift), not a hole.
func TestEvaluate_CommonModeLatencyIsAdvisoryOnly(t *testing.T) {
	rep := evalRows(t, baselineRatio10,
		row("grpc", 200, 400, 150), row("grpc", 200, 400, 150), row("grpc", 200, 400, 150),
		row("uds", 80, 160, 17), row("uds", 80, 160, 17), row("uds", 80, 160, 17),
		row("shm", 20, 40, 19), row("shm", 20, 40, 19), row("shm", 20, 40, 19))

	if rep.failed() {
		t.Fatalf("a common-mode latency movement must not trip a hard gate: %+v", rep.hard)
	}
	var sawDoubling bool
	for _, c := range rep.advisory {
		if strings.Contains(c.message, "+100.00%") {
			sawDoubling = true
		}
	}
	if !sawDoubling {
		t.Error("the common-mode movement must show up in the advisory absolute delta")
	}
}

// Test the documented designed-failure outcome for a hoist-style change to the
// uds reference: a faster uds reference (allocations unchanged) leaves the
// shm-vs-grpc ratio and floor untouched but fails the shm-vs-uds ratio check,
// since the uds reference is what got faster. This is the shape of the outcome
// a receive-timeout syscall hoist on the uds transport produces — recorded here
// so the gate's own tests document that outcome as designed, not noise.
func TestEvaluate_FasterUDSReferenceFailsShmVsUDSRatioOnly(t *testing.T) {
	rows := []string{
		row("grpc", 100, 200, 150), row("grpc", 100, 200, 150), row("grpc", 100, 200, 150),
		// uds sped up from the baseline's 40us to 20us; allocations unchanged.
		row("uds", 20, 40, 17), row("uds", 20, 40, 17), row("uds", 20, 40, 17),
		row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19),
	}
	rep := evalRows(t, baselineRatio10, rows...)

	if !rep.failed() {
		t.Fatal("a faster uds reference must fail the shm-vs-uds ratio check")
	}

	// Exactly one hard check must fail — not just "the uds ratio check is among
	// the failures" — so a regression that also breaks the grpc ratio, the
	// floor, a reps count, or an allocs/op check would be caught here too,
	// rather than passing unnoticed alongside the expected uds-ratio failure.
	var failedNames []string
	for _, c := range rep.hard {
		if !c.pass {
			failedNames = append(failedNames, c.name)
		}
	}
	if len(failedNames) != 1 {
		t.Fatalf("exactly one hard check must fail for a faster-uds-only change, got %d: %v",
			len(failedNames), failedNames)
	}
	if want := "shm ratio vs uds"; failedNames[0] != want {
		t.Errorf("the one failing check must be %q, got %q", want, failedNames[0])
	}
}

// Test that a missing or malformed baseline is an error, never a silent pass.
func TestLoadBaseline_MissingAndMalformed(t *testing.T) {
	if _, err := loadBaseline([]byte("")); err == nil {
		t.Error("an empty baseline must error")
	}
	if _, err := loadBaseline([]byte("{not json")); err == nil {
		t.Error("a malformed baseline must error")
	}
	// A baseline whose policy references a cell with no entry must error.
	bad := `{"policy":{"reps":3,"absolute_floor_ratio":7,"relative_tolerance":0.1,
	         "normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds","payload_bytes":64,"concurrency":1},
	         "cells":{"shm":{"p50_us":10,"p99_us":20,"allocs_per_op":19}}}`
	if _, err := loadBaseline([]byte(bad)); err == nil {
		t.Error("a baseline missing a referenced cell must error")
	}

	// An empty normative_cells list would run zero hard checks: reject it.
	empty := `{"policy":{"reps":3,"absolute_floor_ratio":7,"relative_tolerance":0.1,
	          "normative_cells":[],"grpc_ref":"grpc","uds_ref":"uds","payload_bytes":64,"concurrency":1},
	          "cells":{"grpc":{"p50_us":100,"p99_us":200,"allocs_per_op":150},
	                   "uds":{"p50_us":40,"p99_us":80,"allocs_per_op":17}}}`
	if _, err := loadBaseline([]byte(empty)); err == nil {
		t.Error("an empty normative_cells list must error")
	}

	// A baseline cell with a missing (zero) field must error.
	badCell := `{"policy":{"reps":3,"absolute_floor_ratio":7,"relative_tolerance":0.1,
	            "normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds","payload_bytes":64,"concurrency":1},
	            "cells":{"shm":{"p50_us":10,"p99_us":20,"allocs_per_op":19},
	                     "grpc":{"p99_us":200,"allocs_per_op":150},
	                     "uds":{"p50_us":40,"p99_us":80,"allocs_per_op":17}}}`
	if _, err := loadBaseline([]byte(badCell)); err == nil {
		t.Error("a baseline cell missing a field (p50_us) must error")
	}

	// An empty reference name (grpc_ref or uds_ref) must error.
	emptyRef := `{"policy":{"reps":3,"absolute_floor_ratio":7,"relative_tolerance":0.1,
	             "normative_cells":["shm"],"grpc_ref":"","uds_ref":"uds","payload_bytes":64,"concurrency":1},
	             "cells":{"shm":{"p50_us":10,"p99_us":20,"allocs_per_op":19},
	                      "uds":{"p50_us":40,"p99_us":80,"allocs_per_op":17}}}`
	if _, err := loadBaseline([]byte(emptyRef)); err == nil {
		t.Error("an empty grpc_ref must error")
	}

	// A baseline cell with an explicit non-positive latency must error.
	negLatency := `{"policy":{"reps":3,"absolute_floor_ratio":7,"relative_tolerance":0.1,
	               "normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds","payload_bytes":64,"concurrency":1},
	               "cells":{"shm":{"p50_us":-1,"p99_us":20,"allocs_per_op":19},
	                        "grpc":{"p50_us":100,"p99_us":200,"allocs_per_op":150},
	                        "uds":{"p50_us":40,"p99_us":80,"allocs_per_op":17}}}`
	if _, err := loadBaseline([]byte(negLatency)); err == nil {
		t.Error("a baseline cell with a negative p50_us must error")
	}

	// A baseline cell with an out-of-range latency literal (1e999) must error:
	// encoding/json rejects it while decoding the cells, a fail-closed rejection.
	// The finiteness check itself is witnessed directly in
	// TestValidateBaselineCell_RejectsNonFiniteAndNonPositive.
	outOfRange := `{"policy":{"reps":3,"absolute_floor_ratio":7,"relative_tolerance":0.1,
	               "normative_cells":["shm"],"grpc_ref":"grpc","uds_ref":"uds","payload_bytes":64,"concurrency":1},
	               "cells":{"shm":{"p50_us":1e999,"p99_us":20,"allocs_per_op":19},
	                        "grpc":{"p50_us":100,"p99_us":200,"allocs_per_op":150},
	                        "uds":{"p50_us":40,"p99_us":80,"allocs_per_op":17}}}`
	if _, err := loadBaseline([]byte(outOfRange)); err == nil {
		t.Error("a baseline cell with an out-of-range p50_us literal must error")
	}
}

// Test the regime guard: an installed 2.0-CPU quota verifies, while an absent
// quota (cpu.max = "max ...") or a mismatched one is rejected — never a silent
// pass.
func TestVerifyCPUQuota(t *testing.T) {
	if err := verifyCPUQuota(2.0, "200000 100000"); err != nil {
		t.Errorf("a 2.0-CPU quota must verify: %v", err)
	}
	if err := verifyCPUQuota(2.0, "max 100000"); err == nil {
		t.Error("an absent quota (max) must be rejected")
	}
	if err := verifyCPUQuota(2.0, "100000 100000"); err == nil {
		t.Error("a 1.0-CPU quota must be rejected when 2.0 was requested")
	}
	if err := verifyCPUQuota(2.0, "garbage"); err == nil {
		t.Error("a malformed cpu.max must be rejected")
	}
}

// Test that the regime guard, wired through realMain, fails the whole run when the
// requested cgroup quota is not installed — rather than proceeding as if it were.
func TestRealMain_RegimeGuardFailsWhenQuotaAbsent(t *testing.T) {
	dir := t.TempDir()

	baselinePath := filepath.Join(dir, "baseline.json")
	if err := os.WriteFile(baselinePath, []byte(baselineRatio10), 0o644); err != nil {
		t.Fatal(err)
	}
	resultsPath := filepath.Join(dir, "results.jsonl")
	rows := append(refRows(), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	if err := os.WriteFile(resultsPath, []byte(strings.Join(rows, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	// Point the guard at a cpu.max that reports no quota installed.
	cpuMaxPath := filepath.Join(dir, "cpu.max")
	if err := os.WriteFile(cpuMaxPath, []byte("max 100000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	orig := cgroupCPUMaxPath
	cgroupCPUMaxPath = cpuMaxPath
	defer func() { cgroupCPUMaxPath = orig }()

	err := realMain(baselinePath, 2.0, []string{resultsPath})
	if err == nil {
		t.Fatal("the regime guard must fail the run when the quota is absent, not pass")
	}
	if !strings.Contains(err.Error(), "regime guard") {
		t.Errorf("the failure should name the regime guard, got %v", err)
	}
}

// Test that the same run passes once the quota is actually installed, proving the
// guard gates on the environment rather than always failing.
func TestRealMain_PassesWhenQuotaInstalled(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	_ = os.WriteFile(baselinePath, []byte(baselineRatio10), 0o644)
	resultsPath := filepath.Join(dir, "results.jsonl")
	rows := append(refRows(), row("shm", 10, 20, 19), row("shm", 10, 20, 19), row("shm", 10, 20, 19))
	_ = os.WriteFile(resultsPath, []byte(strings.Join(rows, "\n")), 0o644)

	cpuMaxPath := filepath.Join(dir, "cpu.max")
	_ = os.WriteFile(cpuMaxPath, []byte("200000 100000\n"), 0o644)
	orig := cgroupCPUMaxPath
	cgroupCPUMaxPath = cpuMaxPath
	defer func() { cgroupCPUMaxPath = orig }()

	if err := realMain(baselinePath, 2.0, []string{resultsPath}); err != nil {
		t.Fatalf("a clean run under an installed quota must pass: %v", err)
	}
}
