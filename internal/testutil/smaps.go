package testutil

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// smapsPath is the per-mapping accounting /proc/self/maps deliberately does not
// carry: maps says which address ranges exist and what backs them, smaps adds how
// much of each one is actually resident.
const smapsPath = "/proc/self/smaps"

// rssPrefix is the smaps field naming a mapping's resident set, always reported
// in kB whatever the kernel's page size.
const rssPrefix = "Rss:"

// MappedResidentBytes returns the total resident bytes of every mapping in this
// process whose backing path contains pathSubstring, and how many such mappings
// there were.
//
// The mapping count is returned alongside the bytes because a substring that
// matches nothing yields zero bytes, and a zero that means "nothing matched" is
// indistinguishable from a zero that means "nothing is resident" — the second is
// a real answer and the first is a broken measurement. A caller comparing a
// before and an after sample should assert the count is the one it expects.
//
// Resident bytes, not mapped bytes, is what a memory claim about a shared-memory
// region has to be made in: the region is a sparse memfd, so its mapped SIZE is
// fixed at attach and says nothing about how many of its pages any traffic has
// actually touched. Rss is reported in kB and converted here.
func MappedResidentBytes(pathSubstring string) (bytes uint64, mappings int, err error) {
	f, err := os.Open(smapsPath)
	if err != nil {
		return 0, 0, fmt.Errorf("testutil: open %s: %w", smapsPath, err)
	}
	defer func() { _ = f.Close() }()

	// A mapping's header line is followed by its fields, so the header decides
	// whether the Rss line beneath it counts. inMatch carries that decision down.
	inMatch := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if path, isHeader := smapsHeaderPath(line); isHeader {
			inMatch = strings.Contains(path, pathSubstring)
			if inMatch {
				mappings++
			}

			continue
		}
		if !inMatch || !strings.HasPrefix(line, rssPrefix) {
			continue
		}
		kb, perr := smapsFieldKB(line)
		if perr != nil {
			return 0, 0, perr
		}
		bytes += kb * 1024
	}
	if serr := scanner.Err(); serr != nil {
		return 0, 0, fmt.Errorf("testutil: read %s: %w", smapsPath, serr)
	}

	return bytes, mappings, nil
}

// RequireMappedResidentBytes is MappedResidentBytes for a test: it fails tb if
// the sample cannot be read, or if no mapping matched pathSubstring at all —
// which would otherwise report zero resident bytes and let a comparison pass over
// a measurement of nothing.
func RequireMappedResidentBytes(tb testing.TB, pathSubstring string) uint64 {
	tb.Helper()

	bytes, mappings, err := MappedResidentBytes(pathSubstring)
	if err != nil {
		tb.Fatalf("sample resident bytes for %q: %v", pathSubstring, err)
	}
	if mappings == 0 {
		tb.Fatalf("no mapping backed by %q is present, so its resident size measures nothing", pathSubstring)
	}

	return bytes
}

// smapsHeaderPath reports whether line begins a mapping and, if so, the path
// backing it (empty for an anonymous mapping).
//
// A header is the only line whose first field is an address range, which is what
// separates it from the "Field: value" lines beneath it: a field name always ends
// in a colon, and an address range never contains one.
func smapsHeaderPath(line string) (path string, isHeader bool) {
	const headerFields = 6

	head, _, ok := strings.Cut(line, " ")
	if !ok || !strings.Contains(head, "-") || strings.Contains(head, ":") {
		return "", false
	}

	// The path is the sixth field and may itself contain spaces, so it is taken as
	// the remainder rather than by splitting the whole line.
	fields := strings.Fields(line)
	if len(fields) < headerFields {
		return "", true
	}

	return strings.Join(fields[headerFields-1:], " "), true
}

// smapsFieldKB parses the kB value of a "Name:  <n> kB" smaps field line.
func smapsFieldKB(line string) (uint64, error) {
	fields := strings.Fields(line)
	const valueField = 2
	if len(fields) < valueField {
		return 0, fmt.Errorf("testutil: %s: malformed field line %q", smapsPath, line)
	}
	kb, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("testutil: %s: field %q: %w", smapsPath, line, err)
	}

	return kb, nil
}
