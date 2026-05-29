package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/tabwriter"
)

type CompareRow struct {
	Benchmark string
	Metric    string
	OldVal    string
	NewVal    string
	Delta     string
	PValue    string
	Status    string
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: go run scripts/perf_gate.go <baseline.txt> <pr.txt>")
		os.Exit(1)
	}

	baselineFile := os.Args[1]
	prFile := os.Args[2]

	fmt.Printf("PERFORMANCE GATE ANALYSIS\n")
	fmt.Printf("Baseline File: %s\n", baselineFile)
	fmt.Printf("PR File:       %s\n\n", prFile)

	// 1. Strict Zero-Alloc check from raw PR benchmark output
	err := verifyRawZeroAlloc(prFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PASS: Zero-Alloc raw verification successful.")

	// 2. Perform CPU Regression comparison via benchstat -format csv
	regressionDetected, comparisonTable, err := runBenchstatCSVComparison(baselineFile, prFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: Benchstat CSV comparison failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nCOMPARISON REPORT")
	fmt.Println(comparisonTable)

	if regressionDetected {
		fmt.Fprintln(os.Stderr, "FAIL: CPU performance regression exceeding 12% with p-value < 0.05 detected.")
		os.Exit(1)
	}

	fmt.Println("PASS: Performance gate cleared successfully.")
}

// verifyRawZeroAlloc checks that every benchmark line in the PR file has exactly 0 B/op and 0 allocs/op
func verifyRawZeroAlloc(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open PR benchmark file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		fields := strings.Fields(line)
		var bytesVal, allocsVal int64
		var hasBytes, hasAllocs bool

		for i := 0; i < len(fields)-1; i++ {
			if fields[i+1] == "B/op" {
				val, err := strconv.ParseInt(fields[i], 10, 64)
				if err == nil {
					bytesVal = val
					hasBytes = true
				}
			}
			if fields[i+1] == "allocs/op" {
				val, err := strconv.ParseInt(fields[i], 10, 64)
				if err == nil {
					allocsVal = val
					hasAllocs = true
				}
			}
		}

		if hasBytes && bytesVal > 0 {
			return fmt.Errorf("Memory Bloat: %s allocated %d B/op (Zero-Alloc violated)", fields[0], bytesVal)
		}
		if hasAllocs && allocsVal > 0 {
			return fmt.Errorf("Memory Leak: %s triggered %d allocs/op (Zero-Alloc violated)", fields[0], allocsVal)
		}
	}

	return scanner.Err()
}

func runBenchstatCSVComparison(baseline, pr string) (bool, string, error) {
	_, err := exec.LookPath("benchstat")
	if err != nil {
		return false, "", fmt.Errorf("benchstat tool not found. Please install it using: go install golang.org/x/perf/cmd/benchstat@latest")
	}

	cmd := exec.Command("benchstat", "-format", "csv", baseline, pr)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err = cmd.Run()
	if err != nil {
		return false, "", fmt.Errorf("benchstat error: %v (stderr: %s)", err, stderrBuf.String())
	}

	regressionDetected, table := parseCSVOutput(stdoutBuf.String())
	return regressionDetected, table, nil
}

func parseCSVOutput(csvContent string) (bool, string) {
	var rows []CompareRow
	var regression bool

	reader := csv.NewReader(strings.NewReader(csvContent))
	// Configure reader to handle variable fields per line if needed
	reader.FieldsPerRecord = -1

	var currentMetric string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if len(record) == 0 {
			continue
		}

		// Detect metric headers e.g. ",sec/op,CI,sec/op,CI,vs base,P"
		if len(record) >= 2 && record[0] == "" && (record[1] == "sec/op" || record[1] == "B/op" || record[1] == "allocs/op") {
			currentMetric = record[1]
			continue
		}

		// Ignore geomean rows and comment headers
		if record[0] == "" || record[0] == "geomean" || strings.HasPrefix(record[0], "goos:") || strings.HasPrefix(record[0], "goarch:") || strings.HasPrefix(record[0], "pkg:") || strings.HasPrefix(record[0], "cpu:") {
			continue
		}

		// Process benchmark row
		// Indexes: 0=Name, 1=OldVal, 2=OldCI, 3=NewVal, 4=NewCI, 5=Delta, 6=PValue
		if len(record) >= 7 && currentMetric != "" {
			benchName := record[0]
			oldVal := formatValue(record[1], currentMetric)
			newVal := formatValue(record[3], currentMetric)
			delta := record[5]

			pValStr := ""
			pValField := record[6] // e.g. "p=0.000 n=10"
			if strings.HasPrefix(pValField, "p=") {
				fields := strings.Fields(pValField)
				pValStr = strings.TrimPrefix(fields[0], "p=")
			}

			status := "OK"

			// Check memory and CPU limits
			if currentMetric == "B/op" {
				val, parseErr := strconv.ParseFloat(record[3], 64)
				if parseErr == nil && val > 0.0 {
					status = "FAIL (Memory Bloat)"
					regression = true
				}
			} else if currentMetric == "allocs/op" {
				val, parseErr := strconv.ParseFloat(record[3], 64)
				if parseErr == nil && val > 0.0 {
					status = "FAIL (Memory Leak)"
					regression = true
				}
			} else if currentMetric == "sec/op" && delta != "~" && strings.HasPrefix(delta, "+") {
				deltaPctStr := strings.TrimSuffix(strings.TrimPrefix(delta, "+"), "%")
				deltaPct, err1 := strconv.ParseFloat(deltaPctStr, 64)
				pVal, err2 := strconv.ParseFloat(pValStr, 64)

				if err1 == nil && err2 == nil {
					if deltaPct > 12.0 && pVal < 0.05 {
						status = "FAIL (CPU Regression)"
						regression = true
					}
				}
			}

			rows = append(rows, CompareRow{
				Benchmark: benchName,
				Metric:    currentMetric,
				OldVal:    oldVal,
				NewVal:    newVal,
				Delta:     delta,
				PValue:    pValStr,
				Status:    status,
			})
		}
	}

	// Format rows to aligned CLI Columns
	var tableBuilder strings.Builder
	w := tabwriter.NewWriter(&tableBuilder, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "BENCHMARK\tMETRIC\tBASELINE\tPR\tDELTA\tP-VALUE\tSTATUS")

	for _, r := range rows {
		pValDisplay := r.PValue
		if pValDisplay == "" {
			pValDisplay = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.Benchmark, r.Metric, r.OldVal, r.NewVal, r.Delta, pValDisplay, r.Status)
	}
	w.Flush()

	return regression, tableBuilder.String()
}

func formatValue(valStr string, unit string) string {
	if valStr == "" {
		return "-"
	}
	if unit == "sec/op" {
		f, err := strconv.ParseFloat(valStr, 64)
		if err == nil {
			return fmt.Sprintf("%.2fns", f*1e9)
		}
	}
	return valStr
}
