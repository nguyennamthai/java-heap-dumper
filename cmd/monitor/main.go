package main

import (
	"encoding/json"
	"fmt"
	"java-heap-dumper/internal/types"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const targetProcessId = 1

var (
	dumpDir    = "/tmp/dumps"
	statusFile string
	reportFile string
)

func main() {
	envVars := loadEnvironmentVariables()

	if err := os.MkdirAll(dumpDir, 0777); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create dump directory %s: %v\n", dumpDir, err)
		os.Exit(1)
	}

	if err := verifyJavaProcessId(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Process verification failed: %v\n", err)
		os.Exit(1)
	}

	writeStatus("Running", "Monitor initialized and watching the java process")

	for {
		if hasPendingDump() {
			time.Sleep(1 * time.Minute)
			continue
		}

		usageKb, err := getOldGenUsage()
		if err != nil {
			writeStatus("Warning", fmt.Sprintf("jstat failed: %v", err))
			time.Sleep(5 * time.Second)
			continue
		}

		writeStatus("Running", fmt.Sprintf("Old Gen Usage: %d KB", usageKb))

		if usageKb >= envVars.ThresholdKb {
			if err := takeHeapDump(); err != nil {
				writeStatus("Error", fmt.Sprintf("Dump failed: %v", err))
			} else {
				waitForTermination()
			}
		}

		time.Sleep(5 * time.Second)
	}
}

func loadEnvironmentVariables() types.CmdEvnVars {
	if envDir := os.Getenv("DUMP_DIR"); envDir != "" {
		dumpDir = envDir
	}

	statusFile = fmt.Sprintf("%s/monitor_status.json", dumpDir)
	reportFile = fmt.Sprintf("%s/dump_report.json", dumpDir)

	envThresholdGb := os.Getenv("THRESHOLD_GB")
	if envThresholdGb == "" {
		fmt.Println("Fatal: THRESHOLD_GB environment variable is required (e.g., '1.5')")
		os.Exit(1)
	}

	thresholdGb, err := strconv.ParseFloat(envThresholdGb, 64)
	if err != nil {
		fmt.Printf("Fatal: Invalid THRESHOLD_GB %s: %v\n", envThresholdGb, err)
		os.Exit(1)
	}

	return types.CmdEvnVars{
		ThresholdKb: int64(thresholdGb * 1024 * 1024),
	}
}

func verifyJavaProcessId() error {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", targetProcessId))
	if err != nil {
		return err
	}

	if strings.TrimSpace(string(data)) != "java" {
		return fmt.Errorf("process is not java")
	}
	return nil
}

func writeStatus(state string, message string) {
	s := types.MonitorStatus{
		State:     state,
		Message:   message,
		Timestamp: time.Now().Unix(),
	}

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		fmt.Printf("Error marshalling status: %v\n", err)
		return
	}

	_ = os.WriteFile(statusFile, data, 0644)
}

func hasPendingDump() bool {
	_, err := os.Stat(reportFile)
	return err == nil
}

func getOldGenUsage() (int64, error) {
	cmd := exec.Command("jstat", "-gc", fmt.Sprintf("%d", targetProcessId))
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("unexpected jstat output format")
	}

	// The first line is header
	fields := strings.Fields(lines[1])

	// OU column, in KB, is at index 7
	const ouIndex = 7
	if len(fields) < ouIndex+1 {
		return 0, fmt.Errorf("jstat output has too few columns")
	}

	usageKb, err := strconv.ParseFloat(fields[ouIndex], 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse OU value of %v: %v", fields[ouIndex], err)
	}

	return int64(usageKb), nil
}

func takeHeapDump() error {
	timestamp := time.Now().Format("20060102_150405")
	fileName := fmt.Sprintf("heap_dump_%s.hprof", timestamp)
	fullPath := fmt.Sprintf("%s/%s", dumpDir, fileName)

	cmd := exec.Command("jcmd", fmt.Sprintf("%d", targetProcessId), "GC.heap_dump", fullPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("jcmd failed: %s: %v", string(out), err)
	}

	nodeName := os.Getenv("NODE_NAME")
	jsonContent := fmt.Sprintf(`{"file":"%s", "node":"%s"}`, fileName, nodeName)
	return os.WriteFile(reportFile, []byte(jsonContent), 0644)
}

func waitForTermination() {
	writeStatus("Completed", "Heap dump taken.")

	// Create a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)

	// Register for SIGINT (Ctrl+C) and SIGTERM (Kubernetes Kill)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block the program until the signal arrives.
	<-sigChan

	fmt.Println("Shutting down the monitor ...")
	os.Exit(0)
}
