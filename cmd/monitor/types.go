package main

// MonitorStatus represents the heartbeat data written by the monitor
type MonitorStatus struct {
	State     string `json:"state"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

type envVars struct {
	thresholdKb int64
}
