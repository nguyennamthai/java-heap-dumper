package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	s3Cfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"java-heap-dumper/internal/injector"
	dumperV1 "java-heap-dumper/pkg/apis/heapdumper/v1"
)

const (
	publisherAppName = "gc-publisher"
	publishDumpPath  = "/publishDump"
	maxRetries       = 3
	finalizerName    = "cleanup"
)

var (
	cmdOptions = []CmdOptions{
		{
			fileName:         "gc-monitor",
			startOnInjection: true,
		},
		{
			fileName:         publisherAppName,
			subCmd:           "s3",
			startOnInjection: false,
		},
	}
)

func New(baseClient kubernetes.Interface, crdClient rest.Interface, informer cache.SharedIndexInformer, inj *injector.Injector) *Controller {
	c := &Controller{
		baseClient: baseClient,
		crdClient:  crdClient,
		informer:   informer,
		injector:   inj,
		queue:      workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
	}

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueue(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueue(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueue(obj)
		},
	})

	if err != nil {
		panic(fmt.Errorf("failed to register event handler: %w", err))
	}

	return c
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		c.queue.Add(key)
	}
}

func (c *Controller) Run(ctx context.Context, ctrlCfg dumperV1.ControllerConfig, nbrOfWorkers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	slog.Info("Starting controller...")
	if !cache.WaitForNamedCacheSync(ctrlCfg.PodName, stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	go c.startHttpServer(ctx, ctrlCfg)

	slog.Info("Caches synced. Starting workers...")
	for i := 0; i < nbrOfWorkers; i++ {
		go wait.Until(func() { c.runWorker(ctx, ctrlCfg) }, time.Second, stopCh)
	}

	<-stopCh
	slog.Info("Stopping controller")
}

func (c *Controller) startHttpServer(ctx context.Context, ctrlCfg dumperV1.ControllerConfig) {
	s3Client, err := getS3Client(ctx)
	if err != nil {
		slog.Error("Failed to create S3 client", "error", err)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc(publishDumpPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("Failed to read request body", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		defer func() {
			_ = r.Body.Close()
		}()

		var dumpLoc HeapDumpLocation
		if err := json.Unmarshal(body, &dumpLoc); err != nil {
			slog.Error("Failed to unmarshal request body", "error", err)
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		slog.Info("Received heap dump report", "namespace", dumpLoc.Namespace, "podName", dumpLoc.PodName, "localPath", dumpLoc.LocalPath)

		pod, err := c.baseClient.CoreV1().Pods(dumpLoc.Namespace).Get(ctx, dumpLoc.PodName, metaV1.GetOptions{})
		if err != nil {
			slog.Error("Failed to get pod", "namespace", dumpLoc.Namespace, "podName", dumpLoc.PodName, "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		triggerUploadToS3(ctx, s3Client, c.injector, pod, dumpLoc)
		w.WriteHeader(http.StatusOK)
	})

	slog.Info("Starting HTTP server", "port", ctrlCfg.ControllerPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", ctrlCfg.ControllerPort), mux); err != nil {
		slog.Error("HTTP server failed", "error", err)
	}
}

func getS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := s3Cfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func triggerUploadToS3(ctx context.Context, s3Client *s3.Client, i *injector.Injector, pod *coreV1.Pod, dumpLoc HeapDumpLocation) {
	bucket := os.Getenv("S3_DUMP_BUCKET")
	objKey := fmt.Sprintf("%s/%s", os.Getenv("S3_DUMP_PREFIX"), filepath.Base(dumpLoc.LocalPath))
	presignedUrl, err := generatePresignedPutUrl(ctx, s3Client, bucket, objKey)
	if err != nil {
		slog.Error("Failed to generate presigned URL", "error", err)
		return
	}

	opts := injector.Options{
		ContainerName: "",
		FileName:      publisherAppName,
		SubCmd:        fmt.Sprintf("s3  --file %s --url %s", dumpLoc.LocalPath, presignedUrl),
	}

	err = i.ExecuteProcess(ctx, pod, opts)
	if err != nil {
		slog.Error("Failed to execute process", "error", err)
		return
	}
	slog.Info("Uploading dump to S3", "presignedUrl", presignedUrl)
}

func generatePresignedPutUrl(ctx context.Context, s3Client *s3.Client, bucket string, objKey string) (string, error) {
	presignClient := s3.NewPresignClient(s3Client)
	putInput := &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &objKey,
	}

	presignReq, err := presignClient.PresignPutObject(ctx, putInput, func(opts *s3.PresignOptions) {
		opts.Expires = 1 * time.Minute
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}
	return presignReq.URL, nil
}

func (c *Controller) runWorker(ctx context.Context, ctrlCfg dumperV1.ControllerConfig) {
	for c.processItem(ctx, ctrlCfg) {
	}
}

func (c *Controller) processItem(ctx context.Context, ctrlCfg dumperV1.ControllerConfig) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	err := c.reconcile(ctx, key, ctrlCfg)
	c.handleErr(err, key)
	return true
}

func (c *Controller) reconcile(ctx context.Context, key string, ctrlCfg dumperV1.ControllerConfig) error {
	item, exists, err := c.informer.GetIndexer().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	dumper, ok := item.(*dumperV1.HeapDumper)
	if !ok {
		return fmt.Errorf("expected HeapDumper, got %T", item)
	}

	if dumper.ObjectMeta.DeletionTimestamp.IsZero() {
		return c.handleCreation(ctx, dumper, ctrlCfg)
	}
	return c.handleDeletion(ctx, dumper)
}

func (c *Controller) handleCreation(ctx context.Context, dumper *dumperV1.HeapDumper, ctrlCfg dumperV1.ControllerConfig) error {
	if !containsString(dumper.ObjectMeta.Finalizers, finalizerName) {
		dumperCopy := dumper.DeepCopy()
		dumperCopy.ObjectMeta.Finalizers = append(dumperCopy.ObjectMeta.Finalizers, finalizerName)
		slog.Info("Adding finalizer", "name", dumper.Name)
		if err := c.updateDumper(ctx, dumperCopy); err != nil {
			return fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	for _, opts := range cmdOptions {
		if err := c.injectFile(ctx, dumper, ctrlCfg, opts); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) injectFile(ctx context.Context, dumper *dumperV1.HeapDumper, ctrlCfg dumperV1.ControllerConfig, opts CmdOptions) error {
	pods, err := c.findPods(ctx, dumper)
	if err != nil {
		return err
	}

	var errs []error
	for _, pod := range pods {
		p := pod
		containerName, err := findContainerName(&p, dumper.Spec.Container)
		if err != nil {
			slog.Warn("Skipping pod", "podName", p.Name, "error", err)
			errs = append(errs, err)
			continue
		}

		thresholdGb := strconv.FormatFloat(dumper.Spec.ThresholdGb, 'f', -1, 64)
		controllerUrl := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d%s", ctrlCfg.ServiceName, dumper.Namespace, ctrlCfg.ControllerPort, publishDumpPath)
		opts := injector.Options{
			ContainerName:    containerName,
			FileName:         opts.fileName,
			StartOnInjection: opts.startOnInjection,
			EnvVars: map[string]string{
				"THRESHOLD_GB":   thresholdGb,
				"CONTROLLER_URL": controllerUrl,
			},
		}

		if err := c.injector.Inject(ctx, &p, opts); err != nil {
			slog.Error("Failed to inject", "podName", p.Name, "error", err)
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to inject into pods: %v", errs)
	}
	return nil
}

func (c *Controller) handleDeletion(ctx context.Context, dumper *dumperV1.HeapDumper) error {
	if !containsString(dumper.ObjectMeta.Finalizers, finalizerName) {
		return nil
	}

	for _, opts := range cmdOptions {
		if err := c.removeFile(ctx, dumper, opts); err != nil {
			return err
		}
	}

	slog.Info("Removing finalizer ...", "resourceName", dumper.Name)
	dumperCopy := dumper.DeepCopy()
	dumperCopy.ObjectMeta.Finalizers = removeString(dumperCopy.ObjectMeta.Finalizers, finalizerName)
	return c.updateDumper(ctx, dumperCopy)
}

func (c *Controller) removeFile(ctx context.Context, dumper *dumperV1.HeapDumper, opts CmdOptions) error {
	pods, err := c.findPods(ctx, dumper)
	if err != nil {
		return err
	}

	var errs []error
	for _, pod := range pods {
		p := pod
		containerName, err := findContainerName(&p, dumper.Spec.Container)
		if err != nil {
			slog.Warn("Skipping pod", "podName", p.Name, "error", err)
			errs = append(errs, err)
			continue
		}

		opts := injector.Options{
			ContainerName: containerName,
			FileName:      opts.fileName,
		}
		if err := c.injector.Remove(ctx, &p, opts); err != nil {
			slog.Error("Failed to clean up pod", "podName", p.Name, "error", err)
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to clean up pods: %v", errs)
	}
	return nil
}

func (c *Controller) updateDumper(ctx context.Context, dumper *dumperV1.HeapDumper) error {
	result := &dumperV1.HeapDumper{}
	return c.crdClient.Put().
		Namespace(dumper.Namespace).
		Resource("heapdumpers").
		Name(dumper.Name).
		Body(dumper).
		Do(ctx).
		Into(result)
}

func (c *Controller) findPods(ctx context.Context, dumper *dumperV1.HeapDumper) ([]coreV1.Pod, error) {
	selector := labels.SelectorFromSet(dumper.Spec.Selector)
	listOptions := metaV1.ListOptions{
		LabelSelector: selector.String(),
	}

	podList, err := c.baseClient.CoreV1().Pods(dumper.Namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func findContainerName(pod *coreV1.Pod, targetContainerName string) (string, error) {
	containers := pod.Spec.Containers
	if len(containers) == 0 {
		return "", fmt.Errorf("no containers found in pod %s", pod.Name)
	}

	for _, container := range pod.Spec.Containers {
		if container.Name == targetContainerName {
			return targetContainerName, nil
		}
	}

	slog.Warn("Target container not found, falling back to first container", "podName", pod.Name, "targetName", targetContainerName, "containerName", containers[0].Name)
	return containers[0].Name, nil
}

func (c *Controller) handleErr(err error, key string) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	runtime.HandleError(err)
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	var result []string
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}
