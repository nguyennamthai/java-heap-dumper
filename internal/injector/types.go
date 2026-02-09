package injector

import (
	"java-heap-dumper/internal/types"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Injector struct {
	Client     kubernetes.Interface
	RestConfig *rest.Config
}

type Options struct {
	ContainerName string
	ProcessName   string
	ThresholdGb   float64
	EnvVars       types.CmdEnvVars
}
