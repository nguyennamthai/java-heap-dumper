package injector

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/exec"
)

const (
	localDir  = "/dist/"
	remoteDir = "/tmp/"
)

func (i *Injector) Inject(ctx context.Context, pod *corev1.Pod, opts Options) error {
	running, err := i.isProcessRunning(ctx, pod, opts.ContainerName, opts.ProcessName)
	if err != nil {
		return fmt.Errorf("failed to check status of the process %s: %w", opts.ProcessName, err)
	}

	if running {
		slog.Info("Process is already running, skipping injection", "processName", opts.ProcessName)
		return nil
	}

	slog.Info("Injecting binary into container", "processName", opts.ProcessName, "namespace", pod.Namespace, "podName", pod.Name, "containerName", opts.ContainerName)
	if err := i.exec(ctx, pod, opts.ContainerName, []string{"mkdir", "-p", remoteDir}, nil, nil, nil); err != nil {
		return fmt.Errorf("failed to create remote directory: %w", err)
	}

	var localPath = localDir + opts.ProcessName
	var remoteBinaryPath = remoteDir + opts.ProcessName
	var remoteLogPath = remoteDir + opts.ProcessName + ".log"
	if err := i.copyFile(ctx, pod, opts.ContainerName, localPath, remoteBinaryPath); err != nil {
		return fmt.Errorf("failed to inject file %s to %s: %w", localPath, remoteBinaryPath, err)
	}

	cmd := []string{"sh", "-c", fmt.Sprintf("nohup %s > %s 2>&1 &", remoteBinaryPath, remoteLogPath)}
	var stdErr bytes.Buffer
	if err := i.exec(ctx, pod, opts.ContainerName, cmd, nil, nil, &stdErr); err != nil {
		return fmt.Errorf("failed to start process: %w (stderr: %s)", err, stdErr.String())
	}

	slog.Info("Successfully injected file!", "processName", opts.ProcessName)
	return nil
}

func (i *Injector) isProcessRunning(ctx context.Context, pod *corev1.Pod, containerName string, processName string) (bool, error) {
	cmd := []string{"pgrep", "-f", processName}
	stdout := new(bytes.Buffer)

	err := i.exec(ctx, pod, containerName, cmd, nil, stdout, nil)
	if err != nil {
		var exitErr exec.CodeExitError
		if errors.As(err, &exitErr) {
			if exitErr.Code > 0 {
				// pgrep ran successfully but found nothing.
				return false, nil
			}
		}
		return false, fmt.Errorf("failed to check status of process %s: %w", processName, err)
	}

	return stdout.Len() > 0, nil
}

func (i *Injector) Remove(ctx context.Context, pod *corev1.Pod, opts Options) error {
	slog.Info("Cleaning up binary files in container ...", "namespace", pod.Namespace, "pod", pod.Name, "container", opts.ContainerName)

	killCmd := []string{"pkill", "-f", opts.ProcessName}
	if err := i.exec(ctx, pod, opts.ContainerName, killCmd, nil, nil, nil); err != nil {
		slog.Warn("Failed to kill process (might not be running)", "processName", opts.ProcessName, "error", err)
	}

	rmCmd := []string{"rm", "-f", remoteDir + opts.ProcessName, remoteDir + opts.ProcessName + ".log"}
	if err := i.exec(ctx, pod, opts.ContainerName, rmCmd, nil, nil, nil); err != nil {
		return fmt.Errorf("failed to remove files: %w", err)
	}

	slog.Info("Successfully removed binary file", "processName", opts.ProcessName)
	return nil
}

func (i *Injector) copyFile(ctx context.Context, pod *corev1.Pod, containerName string, localPath, remotePath string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %w", localPath, err)
	}

	defer func() {
		_ = file.Close()
	}()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to read metadata of local file %s: %w", localPath, err)
	}

	pr, pw := io.Pipe()
	go func() {
		err := writeToPipe(pw, file, stat, remotePath)
		slog.Error("Failed to write file to pipe", "error", err)
	}()

	destDir := filepath.Dir(remotePath)
	cmd := []string{"tar", "-xf", "-", "-C", destDir}
	var stdErr bytes.Buffer

	err = i.exec(ctx, pod, containerName, cmd, pr, nil, &stdErr)
	if err != nil {
		return fmt.Errorf("failed to copy file %s: %w (stderr: %s)", localPath, err, stdErr.String())
	}
	return nil
}

func writeToPipe(pw *io.PipeWriter, file io.Reader, stat os.FileInfo, dstPath string) error {
	defer func() {
		_ = pw.Close()
	}()

	tw := tar.NewWriter(pw)
	defer func() {
		_ = tw.Close()
	}()

	header := &tar.Header{
		Name: filepath.Base(dstPath),
		Size: stat.Size(),
		Mode: 0755,
	}

	if err := tw.WriteHeader(header); err != nil {
		return err
	}

	if _, err := io.Copy(tw, file); err != nil {
		return err
	}
	return nil
}

func (i *Injector) exec(ctx context.Context, pod *corev1.Pod, containerName string, cmd []string, stdin io.Reader, stdout, stderr io.Writer) error {
	req := i.Client.CoreV1().RESTClient().Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec")

	req.VersionedParams(
		&corev1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdin:     stdin != nil,
			Stdout:    stdout != nil,
			Stderr:    stderr != nil,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(i.RestConfig, "POST", req.URL())
	if err != nil {
		return err
	}

	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Tty:    false,
	})
}
