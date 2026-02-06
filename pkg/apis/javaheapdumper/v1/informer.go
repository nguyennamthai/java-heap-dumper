package v1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func NewHeapDumperInformer(crdClient rest.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	lw := cache.NewListWatchFromClient(
		crdClient,
		"heapdumpers",
		metav1.NamespaceAll,
		fields.Everything(),
	)

	return cache.NewSharedIndexInformer(
		lw,
		&HeapDumper{},
		resyncPeriod,
		cache.Indexers{},
	)
}
