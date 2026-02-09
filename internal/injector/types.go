package injector

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Injector struct {
	Client     kubernetes.Interface
	RestConfig *rest.Config
}

type Options struct {
	ContainerName string
	FileName      string
	SubCmd        string
	EnvVars       map[string]string
}

func (options Options) FullCmd() string {
	if options.SubCmd == "" {
		return options.FileName
	}
	return options.FileName + " " + options.SubCmd
}
