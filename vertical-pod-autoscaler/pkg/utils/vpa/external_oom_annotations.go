/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_api "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/typed/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// UpdateExternalOOMSignalsIfNeeded changes only the owned metadata annotation.
// Optimistic tests prevent signals being attached to a replacement VPA or
// overwriting a concurrent configuration edit. Call before the status writer,
// which would otherwise invalidate the observed resource version.
func UpdateExternalOOMSignalsIfNeeded(client vpa_api.VerticalPodAutoscalerInterface, observed *vpa_types.VerticalPodAutoscaler, signals []annotations.ExternalOOMSignal) (*vpa_types.VerticalPodAutoscaler, error) {
	if observed == nil {
		return nil, errors.New("missing VPA annotation snapshot")
	}
	value, err := annotations.EncodeExternalOOMSignals(signals, observed.Annotations)
	if err != nil {
		return nil, err
	}
	old, exists := observed.Annotations[annotations.ExternalOOMSignalsAnnotation]
	if old == value && (value != "" || !exists) {
		return nil, nil
	}
	if observed.UID == "" {
		return nil, errors.New("missing VPA annotation snapshot UID")
	}
	patches := []patchRecord{{Op: "test", Path: "/metadata/uid", Value: string(observed.UID)}}
	if observed.ResourceVersion != "" {
		patches = append(patches, patchRecord{Op: "test", Path: "/metadata/resourceVersion", Value: observed.ResourceVersion})
	}
	key := strings.ReplaceAll(strings.ReplaceAll(annotations.ExternalOOMSignalsAnnotation, "~", "~0"), "/", "~1")
	switch {
	case value == "":
		patches = append(patches, patchRecord{Op: "remove", Path: "/metadata/annotations/" + key})
	case observed.Annotations == nil:
		patches = append(patches, patchRecord{Op: "add", Path: "/metadata/annotations", Value: map[string]string{annotations.ExternalOOMSignalsAnnotation: value}})
	default:
		patches = append(patches, patchRecord{Op: "add", Path: "/metadata/annotations/" + key, Value: value})
	}
	data, err := json.Marshal(patches)
	if err != nil {
		return nil, err
	}
	return client.Patch(context.TODO(), observed.Name, types.JSONPatchType, data, metav1.PatchOptions{})
}
