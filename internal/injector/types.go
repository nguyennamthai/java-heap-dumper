package injector

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Injector struct {
	Client     kubernetes.Interface
	RestConfig *rest.Config
}
