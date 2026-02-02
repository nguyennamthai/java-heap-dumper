package main

import (
	"java-heap-dumper/cmd/publisher/internal/s3"
	"java-heap-dumper/cmd/publisher/internal/slack"
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		slog.Error("Sub-command is required")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "s3":
		s3.Run(os.Args[2:])
	case "slack":
		slack.Run(os.Args[2:])
	default:
		slog.Error("Unknown sub-command", "subcommand", os.Args[1])
		os.Exit(1)
	}
}
