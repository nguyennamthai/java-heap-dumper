package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type HeapDumper struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              HeapDumperSpec   `json:"spec"`
	Status            HeapDumperStatus `json:"status"`
}

type HeapDumperSpec struct {
	Selector    map[string]string `json:"selector"`
	Container   string            `json:"container"`
	ThresholdGb float64           `json:"thresholdGb"`
}

type HeapDumperStatus struct {
	TargetPod string `json:"targetPod"`
	Injected  bool   `json:"injected"`
}

type HeapDumperList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []HeapDumper `json:"items"`
}

type ControllerConfig struct {
	ServiceName    string
	PodName        string
	ControllerPort int
}
