package controller

import (
	"java-heap-dumper/internal/injector"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	client   kubernetes.Interface
	queue    workqueue.TypedRateLimitingInterface[string]
	informer cache.SharedIndexInformer
	injector *injector.Injector
}
