package main

import (
	"context"
	"fmt"
	"java-heap-dumper/internal/controller"
	"java-heap-dumper/internal/injector"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	dumperV1 "java-heap-dumper/pkg/apis/heapdumper/v1"
)

func main() {
	err := dumperV1.AddToScheme(scheme.Scheme)
	if err != nil {
		klog.Fatalf("Error adding custom resource scheme: %v", err.Error())
	}

	baseConfig, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to load in-cluster config. Ensure this is running inside a Pod with a ServiceAccount: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(baseConfig)
	if err != nil {
		klog.Fatalf("Error building kubernetes client: %v", err.Error())
	}

	crdConfig := *baseConfig
	crdConfig.ContentConfig.GroupVersion = &dumperV1.SchemeGroupVersion
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	crdClient, err := rest.RESTClientFor(&crdConfig)
	if err != nil {
		klog.Fatalf("Error building CRD client: %v", err.Error())
	}

	informer := dumperV1.NewInformer(crdClient, time.Minute*10)
	inj := &injector.Injector{
		Client:     kubeClient,
		RestConfig: baseConfig,
	}

	heapDumpCtr := controller.New(kubeClient, crdClient, informer, inj)
	stopCh := setUpSignalHandler()

	go informer.Run(stopCh)
	heapDumpCtr.Run(context.Background(), os.Getenv("CONTROLLER_NAME"), 2, stopCh)
}

func setUpSignalHandler() <-chan struct{} {
	shutdownCh := make(chan struct{})

	osSignalCh := make(chan os.Signal, 1)
	signal.Notify(osSignalCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-osSignalCh
		fmt.Println("Received termination signal. Shutting down gracefully...")
		close(shutdownCh)
	}()

	return shutdownCh
}
