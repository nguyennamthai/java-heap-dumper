package injector

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Injector struct {
	Client     kubernetes.Interface
	restConfig *rest.Config
}

func NewInjector(client kubernetes.Interface, config *rest.Config) *Injector {
	return &Injector{
		Client:     client,
		restConfig: config,
	}
}
