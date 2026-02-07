package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

func (inList *HeapDumperList) DeepCopyObject() runtime.Object {
	if c := inList.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (inList *HeapDumperList) DeepCopy() *HeapDumperList {
	if inList == nil {
		return nil
	}
	out := &HeapDumperList{}
	inList.DeepCopyInto(out)
	return out
}

func (inList *HeapDumperList) DeepCopyInto(outList *HeapDumperList) {
	*outList = *inList
	outList.TypeMeta = inList.TypeMeta
	inList.ListMeta.DeepCopyInto(&outList.ListMeta)
	if inList.Items != nil {
		inItems, outItems := &inList.Items, &outList.Items
		*outItems = make([]HeapDumper, len(*inItems))
		for item := range *inItems {
			(*inItems)[item].DeepCopyInto(&(*outItems)[item])
		}
	}
	return
}

func (in *HeapDumper) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *HeapDumper) DeepCopy() *HeapDumper {
	if in == nil {
		return nil
	}
	out := &HeapDumper{}
	in.DeepCopyInto(out)
	return out
}

func (in *HeapDumper) DeepCopyInto(out *HeapDumper) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.deepCopyInto(&out.Spec)
	out.Status = in.Status
	return
}

func (inSpec *HeapDumperSpec) deepCopyInto(outSpec *HeapDumperSpec) {
	*outSpec = *inSpec
	if inSpec.Selector != nil {
		inSelector, outSelector := &inSpec.Selector, &outSpec.Selector
		*outSelector = make(map[string]string, len(*inSelector))
		for key, val := range *inSelector {
			(*outSelector)[key] = val
		}
	}
	return
}
