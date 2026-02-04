package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HeapDumper is a specification for a Java Heap Dumper
type HeapDumper struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              HeapDumperSpec   `json:"spec"`
	Status            HeapDumperStatus `json:"status"`
}

// HeapDumperSpec specifies the pod selection
type HeapDumperSpec struct {
	Selector    map[string]string `json:"selector"`
	ThresholdGb float64           `json:"thresholdGb"`
}

// HeapDumperStatus captures the current state
type HeapDumperStatus struct {
	TargetPod string `json:"targetPod"`
	Injected  bool   `json:"injected"`
}

// HeapDumperList is a list of HeapDumper resources
type HeapDumperList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []HeapDumper `json:"items"`
}
