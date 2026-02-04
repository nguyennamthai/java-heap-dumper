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
	processName      = "java-monitor"
	localPath        = "/payload/java-monitor"
	remoteDir        = "/app/"
	remoteBinaryPath = remoteDir + "java-monitor"
	remoteLogPath    = remoteDir + "java-monitor.log"
)

func (i *Injector) Inject(ctx context.Context, pod *corev1.Pod, containerName string) error {
	running, err := i.isProcessRunning(ctx, pod, containerName, processName)
	if err != nil {
		return fmt.Errorf("failed to check process status: %w", err)
	}
	if running {
		fmt.Println("Process is already running, skipping injection")
		return nil
	}

	fmt.Printf("Injecting %s into %s/%s container...\n", processName, pod.Namespace, pod.Name)
	if err := i.exec(ctx, pod, containerName, []string{"mkdir", "-p", remoteDir}, nil, nil, nil); err != nil {
		return fmt.Errorf("failed to create remote directory: %w", err)
	}
	if err := i.copyFile(ctx, pod, containerName, localPath, remoteBinaryPath); err != nil {
		return fmt.Errorf("failed to copy file %s: %w", localPath, err)
	}

	cmd := []string{"sh", "-c", fmt.Sprintf("nohup %s > %s 2>&1 &", remoteBinaryPath, remoteLogPath)}
	var stdErr bytes.Buffer
	if err := i.exec(ctx, pod, containerName, cmd, nil, nil, &stdErr); err != nil {
		return fmt.Errorf("failed to start process: %w (stderr: %s)", err, stdErr.String())
	}

	fmt.Printf("Successfully injected %s!\n", processName)
	return nil
}

func (i *Injector) Remove(ctx context.Context, pod *corev1.Pod, containerName string) error {
	slog.Info("Cleaning up binary files in container ...", "namespace", pod.Namespace, "pod", pod.Name, "container", containerName)

	killCmd := []string{"pkill", "-f", processName}
	if err := i.exec(ctx, pod, containerName, killCmd, nil, nil, nil); err != nil {
		slog.Warn("Failed to kill process (might not be running)", "processName", processName, "error", err)
	}

	rmCmd := []string{"rm", "-f", remoteBinaryPath, remoteLogPath}
	if err := i.exec(ctx, pod, containerName, rmCmd, nil, nil, nil); err != nil {
		return fmt.Errorf("failed to remove files: %w", err)
	}

	slog.Info("Successfully remove binary files")
	return nil
}

func (i *Injector) copyFile(ctx context.Context, pod *corev1.Pod, containerName string, srcPath, dstPath string) (err error) {
	file, openErr := os.Open(srcPath)
	if openErr != nil {
		return fmt.Errorf("failed to open local file %s: %w", srcPath, openErr)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close local file %s: %w", srcPath, closeErr)
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to read metadata of local file %s: %w", srcPath, err)
	}

	pr, pw := io.Pipe()
	go func() {
		writeToPipe(pw, file, stat, dstPath)
	}()

	destDir := filepath.Dir(dstPath)
	cmd := []string{"tar", "-xf", "-", "-C", destDir}
	var stdErr bytes.Buffer

	err = i.exec(ctx, pod, containerName, cmd, pr, nil, &stdErr)
	if err != nil {
		err = fmt.Errorf("failed to copy file %s: %w (stderr: %s)", srcPath, err, stdErr.String())
	}
	return
}

func writeToPipe(pw *io.PipeWriter, file io.Reader, stat os.FileInfo, dstPath string) {
	var err error
	defer func() {
		_ = pw.CloseWithError(err)
	}()

	tw := tar.NewWriter(pw)
	defer func() {
		if closeError := tw.Close(); closeError != nil && err == nil {
			err = closeError
		}
	}()

	header := &tar.Header{
		Name: filepath.Base(dstPath),
		Size: stat.Size(),
		Mode: 0755,
	}

	if writeError := tw.WriteHeader(header); writeError != nil {
		err = writeError
		return
	}

	if _, copyError := io.Copy(tw, file); copyError != nil {
		err = copyError
	}
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
		return false, fmt.Errorf("failed to check process status: %w", err)
	}

	return stdout.Len() > 0, nil
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

	executor, err := remotecommand.NewSPDYExecutor(i.restConfig, "POST", req.URL())
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
