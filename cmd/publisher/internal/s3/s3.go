package s3

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

func Run(args []string) {
	fs := flag.NewFlagSet("s3", flag.ExitOnError)
	var (
		localPath = fs.String("file", "", "Path to the local file to upload")
		uploadURL = fs.String("url", "", "Presigned S3 URL for PUT request")
	)
	_ = fs.Parse(args)

	if *localPath == "" {
		slog.Error("Missing required argument", "arg", "file")
		os.Exit(1)
	}

	if *uploadURL == "" {
		slog.Error("Missing required argument", "arg", "url")
		os.Exit(1)
	}

	slog.Info("Uploading file", "file", *localPath)
	if err := streamFile(*localPath, *uploadURL); err != nil {
		slog.Error("Failed to upload file", "error", err)
		os.Exit(1)
	}
	slog.Info("File uploaded successfully")
}

func streamFile(localPath, url string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", localPath, err)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			slog.Warn("Failed to close file", "file", localPath, "error", err)
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to read metadata of file %s: %w", localPath, err)
	}

	req, err := http.NewRequest("PUT", url, file)
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %w", err)
	}

	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file %s: %w", localPath, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("Failed to close response body", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("file upload returned bad status: %s", resp.Status)
	}

	return nil
}
