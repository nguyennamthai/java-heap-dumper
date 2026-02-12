package main

import (
	"encoding/json"
	"fmt"
	"java-heap-dumper/internal/controller"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	targetProcessId = 1
	dumpDir         = "/tmp/dumps"
	statusFilePath  = dumpDir + "/monitor_status.json"
	reportFilePath  = dumpDir + "/dump_report.json"
)

func main() {
	envVars := loadEnvVariables()

	if err := os.MkdirAll(dumpDir, 0777); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to create dump directory %s: %v\n", dumpDir, err)
		os.Exit(1)
	}

	if err := verifyJavaProcessId(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Process verification failed: %v\n", err)
		os.Exit(1)
	}

	writeStatus("Running", "Monitor initialized")

	for {
		if hasPendingDump() {
			time.Sleep(1 * time.Minute)
			continue
		}

		usageKb, err := getOldGenUsage()
		if err != nil {
			writeStatus("Error", fmt.Sprintf("%v", err))
			time.Sleep(5 * time.Second)
			continue
		}

		writeStatus("Running", fmt.Sprintf("Old Gen Usage: %d KB", usageKb))

		if usageKb >= envVars.thresholdKb {
			if err := takeHeapDump(envVars); err != nil {
				writeStatus("Error", fmt.Sprintf("Failed to take a heap dump: %v", err))
			} else {
				time.Sleep(1 * time.Hour) // No need to take another dump immediately
			}
		}

		time.Sleep(5 * time.Second)
	}
}

func loadEnvVariables() envVars {
	envThresholdGb := os.Getenv("THRESHOLD_GB")
	if envThresholdGb == "" {
		_, _ = fmt.Fprintf(os.Stderr, "THRESHOLD_GB environment variable is required\n")
		os.Exit(1)
	}

	thresholdGb, err := strconv.ParseFloat(envThresholdGb, 64)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "THRESHOLD_GB environment variable is invalid: %v\n", err)
		os.Exit(1)
	}

	podName, err := os.Hostname()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to find pod name: %v\n", err)
		os.Exit(1)
	}

	namespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to find namespace: %v\n", err)
		os.Exit(1)
	}

	return envVars{
		namespace:     strings.TrimSpace(string(namespace)),
		podName:       podName,
		thresholdKb:   int64(thresholdGb * 1024 * 1024),
		controllerUrl: os.Getenv("CONTROLLER_URL"),
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
	s := MonitorStatus{
		State:     state,
		Message:   message,
		Timestamp: time.Now().Unix(),
	}

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error marshalling status: %v\n", err)
		return
	}

	_ = os.WriteFile(statusFilePath, data, 0644)
}

func hasPendingDump() bool {
	_, err := os.Stat(reportFilePath)
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

	// The first line is the header
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

func takeHeapDump(envVars envVars) error {
	timestamp := time.Now().Format("20060102_150405")
	fileName := fmt.Sprintf("heap_dump_%s.hprof", timestamp)
	fullPath := fmt.Sprintf("%s/%s", dumpDir, fileName)

	cmd := exec.Command("jcmd", fmt.Sprintf("%d", targetProcessId), "GC.heap_dump", fullPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("jcmd failed: %s: %v", string(out), err)
	}

	dumpLocation := controller.HeapDumpLocation{
		Namespace: envVars.namespace,
		PodName:   envVars.podName,
		LocalPath: fullPath,
	}
	jsonContent, err := json.Marshal(dumpLocation)
	if err != nil {
		return fmt.Errorf("failed to marshal dump location: %w", err)
	}
	return os.WriteFile(reportFilePath, jsonContent, 0644)
}
