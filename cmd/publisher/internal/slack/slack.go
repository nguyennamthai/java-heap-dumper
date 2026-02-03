package slack

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func Run(args []string) {
	fs := flag.NewFlagSet("slack", flag.ExitOnError)
	var webhookUrl = fs.String("url", "", "Slack webhook URL")
	_ = fs.Parse(args)

	if *webhookUrl == "" {
		slog.Error("Missing required argument: url")
		os.Exit(1)
	}

	slog.Info("Sending notification to Slack")
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	if err := sendNotification(client, *webhookUrl); err != nil {
		slog.Error("Failed to send notification", "error", err)
		os.Exit(1)
	}

	slog.Info("Notification sent successfully")
}

func sendNotification(client *http.Client, url string) error {
	payload := slackPayload{
		Text: "Heap dump uploaded successfully",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create POST request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send POST request: %w", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack API returned bad status: %s", resp.Status)
	}

	return nil
}
