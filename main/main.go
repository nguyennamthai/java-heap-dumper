package main

import (
	"fmt"
	"java-heap-dumper/internal/controller"
	"java-heap-dumper/internal/injector"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func main() {
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to load in-cluster config. Ensure this is running inside a Pod with a ServiceAccount: %s", err.Error())
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error building kubernetes client: %v", err.Error())
	}

	informerFactory := informers.NewSharedInformerFactory(client, time.Minute*10)
	inj := &injector.Injector{
		Client: client,
	}

	heapDumpCtr := controller.New(client, informerFactory.Core().V1().Pods().Informer(), inj)
	stopCh := setUpSignalHandler()
	informerFactory.Start(stopCh)
	heapDumpCtr.Run(2, stopCh)
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
