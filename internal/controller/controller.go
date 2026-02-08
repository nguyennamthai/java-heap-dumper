package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
	targetContainerName = "api"
	maxRetries          = 3
	finalizerName       = "cleanup"
)

var (
	binaryNames = []string{"gc-monitor", "dump-publisher"}
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

func (c *Controller) Run(ctx context.Context, nbrOfWorkers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	slog.Info("Starting controller...")
	if !cache.WaitForNamedCacheSync("java-heap-dumper", stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	slog.Info("Caches synced. Starting workers...")
	for i := 0; i < nbrOfWorkers; i++ {
		go wait.Until(func() { c.runWorker(ctx) }, time.Second, stopCh)
	}

	<-stopCh
	slog.Info("Stopping controller")
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.processItem(ctx) {
	}
}

func (c *Controller) processItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	err := c.reconcile(ctx, key)
	c.handleErr(err, key)
	return true
}

func (c *Controller) reconcile(ctx context.Context, key string) error {
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
		return c.handleCreation(ctx, dumper)
	}
	return c.handleDeletion(ctx, dumper)
}

func (c *Controller) handleCreation(ctx context.Context, dumper *dumperV1.HeapDumper) error {
	if !containsString(dumper.ObjectMeta.Finalizers, finalizerName) {
		dumperCopy := dumper.DeepCopy()
		dumperCopy.ObjectMeta.Finalizers = append(dumperCopy.ObjectMeta.Finalizers, finalizerName)
		slog.Info("Adding finalizer", "name", dumper.Name)
		if err := c.updateDumper(ctx, dumperCopy); err != nil {
			return fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	for _, binaryName := range binaryNames {
		if err := c.injectFile(ctx, dumper, binaryName); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) injectFile(ctx context.Context, dumper *dumperV1.HeapDumper, binaryName string) error {
	pods, err := c.findPods(ctx, dumper)
	if err != nil {
		return err
	}

	var errs []error
	for _, pod := range pods {
		p := pod
		containerName, err := findContainerName(&p)
		if err != nil {
			slog.Warn("Skipping pod", "podName", p.Name, "error", err)
			errs = append(errs, err)
			continue
		}

		opts := injector.Options{
			ContainerName: containerName,
			ProcessName:   binaryName,
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
	if containsString(dumper.ObjectMeta.Finalizers, finalizerName) {
		slog.Info("Removing finalizer", "name", dumper.Name)
		dumperCopy := dumper.DeepCopy()
		dumperCopy.ObjectMeta.Finalizers = removeString(dumperCopy.ObjectMeta.Finalizers, finalizerName)
		if err := c.updateDumper(ctx, dumperCopy); err != nil {
			return fmt.Errorf("failed to remove finalizer: %w", err)
		}
	}

	for _, binaryName := range binaryNames {
		if err := c.removeFile(ctx, dumper, binaryName); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) removeFile(ctx context.Context, dumper *dumperV1.HeapDumper, binaryName string) error {
	pods, err := c.findPods(ctx, dumper)
	if err != nil {
		return err
	}

	var errs []error
	for _, pod := range pods {
		p := pod
		containerName, err := findContainerName(&p)
		if err != nil {
			slog.Warn("Skipping pod", "podName", p.Name, "error", err)
			errs = append(errs, err)
			continue
		}

		opts := injector.Options{
			ContainerName: containerName,
			ProcessName:   binaryName,
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

func findContainerName(pod *coreV1.Pod) (string, error) {
	containers := pod.Spec.Containers
	if len(containers) == 0 {
		return "", fmt.Errorf("no containers found in pod %s", pod.Name)
	}

	for _, container := range pod.Spec.Containers {
		if container.Name == targetContainerName {
			return targetContainerName, nil
		}
	}

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
