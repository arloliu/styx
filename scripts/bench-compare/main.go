package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// cgroupCPUMaxPath is where cgroup v2 exposes this process's CPU quota. Overridden
// in tests via the same field.
var cgroupCPUMaxPath = "/sys/fs/cgroup/cpu.max"

func main() {
	baselinePath := flag.String("baseline", "bench/baselines/shm-baseline.json",
		"path to the curated baseline JSON")
	requireQuota := flag.Float64("require-cpu-quota", 0,
		"if > 0, fail unless the cgroup CPU quota equals this many CPUs before evaluating (regime guard)")
	flag.Parse()

	if err := realMain(*baselinePath, *requireQuota, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "bench-compare: "+err.Error())
		os.Exit(1)
	}
}

// realMain is the testable body: it loads the baseline, optionally
// verifies the CPU quota, parses the result files, evaluates the gate,
// prints the report, and returns a non-nil error when the gate fails.
func realMain(baselinePath string, requireQuota float64, resultPaths []string) error {
	blData, err := os.ReadFile(baselinePath)
	if err != nil {
		return fmt.Errorf("reading baseline: %w", err)
	}
	bl, err := loadBaseline(blData)
	if err != nil {
		return err
	}

	if requireQuota > 0 {
		cpuMax, err := os.ReadFile(cgroupCPUMaxPath)
		if err != nil {
			return fmt.Errorf("regime guard: cannot read %s: %w", cgroupCPUMaxPath, err)
		}
		if err := verifyCPUQuota(requireQuota, string(cpuMax)); err != nil {
			return fmt.Errorf("regime guard: %w", err)
		}
	}

	if len(resultPaths) == 0 {
		return errors.New("no result files given")
	}
	var all []result
	for _, p := range resultPaths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("reading results %s: %w", p, err)
		}
		rows, err := parseResults(strings.NewReader(string(data)))
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		all = append(all, rows...)
	}

	rep := evaluate(bl, all)
	printReport(rep)
	if rep.failed() {
		return errors.New("gate FAILED")
	}

	return nil
}

// printReport writes the human-readable gate result to stdout.
func printReport(rep report) {
	if rep.fatal != "" {
		fmt.Println("FATAL:", rep.fatal)

		return
	}
	fmt.Println("Hard gates:")
	for _, c := range rep.hard {
		mark := "PASS"
		if !c.pass {
			mark = "FAIL"
		}
		fmt.Printf("  [%s] %s\n", mark, c.message)
	}
	fmt.Println("Advisory (not gated):")
	for _, c := range rep.advisory {
		fmt.Printf("  [----] %s\n", c.message)
	}
	if rep.failed() {
		fmt.Println("RESULT: FAIL")
	} else {
		fmt.Println("RESULT: PASS")
	}
}
