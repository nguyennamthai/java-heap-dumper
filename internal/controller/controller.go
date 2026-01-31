package controller

import (
	"context"
	"fmt"
	"java-heap-dumper/internal/injector"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	targetContainerName = "api"
	targetLabelKey      = "app"
	targetLabelValue    = "api"
	maxRetries          = 3
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
			c.enqueuePod(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueuePod(newObj)
		},
	})

	if err != nil {
		panic(fmt.Errorf("failed to register event handler: %w", err))
	}

	return c
}

func (c *Controller) enqueuePod(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		c.queue.Add(key)
	}
}

func (c *Controller) Run(nbrOfWorkers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	fmt.Println("Starting controller...")
	if !cache.WaitForNamedCacheSync("java-heap-dumper", stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	fmt.Println("Caches synced. Starting workers...")

	for i := 0; i < nbrOfWorkers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	fmt.Println("Stopping controller")
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

	err := c.executeMonitor(key)
	c.handleErr(err, key)
	return true
}

func (c *Controller) executeMonitor(key string) error {
	item, exists, err := c.informer.GetIndexer().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	pod := item.(*corev1.Pod)

	if pod.Status.Phase != corev1.PodRunning || pod.Labels[targetLabelKey] != targetLabelValue {
		return nil
	}

	containerName, err := findContainerName(pod)
	if err != nil {
		return fmt.Errorf("failed to extract container %s: %w", targetContainerName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	return c.injector.Inject(ctx, pod, containerName)
}

func findContainerName(pod *corev1.Pod) (string, error) {
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
