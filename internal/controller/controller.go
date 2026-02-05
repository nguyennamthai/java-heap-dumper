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
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"java-heap-dumper/internal/injector"
	dumperV1 "java-heap-dumper/pkg/apis/javaheapdumper/v1"
)

const (
	targetContainerName = "api"
	maxRetries          = 3
	finalizerName       = "heapdumper.hivecpq/cleanup"
)

func New(client kubernetes.Interface, informer cache.SharedIndexInformer, inj *injector.Injector) *Controller {
	c := &Controller{
		client:   client,
		informer: informer,
		injector: inj,
		queue:    workqueue.NewTypedRateLimitingQueue[string](workqueue.DefaultTypedControllerRateLimiter[string]()),
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

func (c *Controller) Run(nbrOfWorkers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	slog.Info("Starting controller...")
	if !cache.WaitForNamedCacheSync("java-heap-dumper", stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	slog.Info("Caches synced. Starting workers...")

	for i := 0; i < nbrOfWorkers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	slog.Info("Stopping controller")
}

func (c *Controller) runWorker() {
	for c.processItem() {
	}
}

func (c *Controller) processItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(key)
	c.handleErr(err, key)
	return true
}

func (c *Controller) reconcile(key string) error {
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

	// Check if being deleted
	if !dumper.ObjectMeta.DeletionTimestamp.IsZero() {
		return c.handleDeletion(dumper)
	}

	return c.handleCreation(dumper)
}

func (c *Controller) handleCreation(dumper *dumperV1.HeapDumper) error {
	if !containsString(dumper.ObjectMeta.Finalizers, finalizerName) {
		dumper.ObjectMeta.Finalizers = append(dumper.ObjectMeta.Finalizers, finalizerName)
		slog.Info("Adding finalizer", "name", dumper.Name)
		// TODO: Update CR
	}

	return c.injectIntoPods(dumper)
}

func (c *Controller) injectIntoPods(dumper *dumperV1.HeapDumper) error {
	pods, err := c.findPods(dumper)
	if err != nil {
		return err
	}

	for _, pod := range pods {
		containerName, err := findContainerName(&pod)
		if err != nil {
			slog.Warn("Skipping pod", "pod", pod.Name, "error", err)
			continue
		}

		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()

			if err := c.injector.Inject(ctx, &pod, containerName); err != nil {
				slog.Error("Failed to inject", "pod", pod.Name, "error", err)
			}
		}()
	}
	return nil
}

func (c *Controller) handleDeletion(dumper *dumperV1.HeapDumper) error {
	if containsString(dumper.ObjectMeta.Finalizers, finalizerName) {
		if err := c.cleanUpPods(dumper); err != nil {
			return err
		}

		slog.Info("Removing finalizer", "name", dumper.Name)
		dumper.ObjectMeta.Finalizers = removeString(dumper.ObjectMeta.Finalizers, finalizerName)
		// TODO: Client.Update()
	}
	return nil
}

func (c *Controller) cleanUpPods(dumper *dumperV1.HeapDumper) error {
	pods, err := c.findPods(dumper)
	if err != nil {
		return err
	}

	for _, pod := range pods {
		containerName, err := findContainerName(&pod)
		if err != nil {
			slog.Warn("Skipping pod", "pod", pod.Name, "error", err)
			continue
		}

		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()

			if err := c.injector.Remove(ctx, &pod, containerName); err != nil {
				slog.Error("Failed to clean up pod", "pod", pod.Name, "error", err)
			}
		}()
	}
	return nil
}

func (c *Controller) findPods(dumper *dumperV1.HeapDumper) ([]coreV1.Pod, error) {
	selector := labels.SelectorFromSet(dumper.Spec.Selector)
	listOptions := metaV1.ListOptions{
		LabelSelector: selector.String(),
	}

	podList, err := c.client.CoreV1().Pods(dumper.Namespace).List(context.Background(), listOptions)
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
