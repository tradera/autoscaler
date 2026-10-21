/*
Copyright 2026 The Kubernetes Authors.

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

package restriction

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	resource_updates "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	resourcehelpers "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/resources"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

// protectExternalOOMResize checks the final resource patches, after all policy
// and limit processing, then binds the atomic write to the observed pod state.
func protectExternalOOMResize(pod *corev1.Pod, vpa *vpa_types.VerticalPodAutoscaler, patches []resource_updates.PatchRecord) ([]resource_updates.PatchRecord, error) {
	uid := vpa_api_util.ExternalOOMUpdatePodUID(vpa)
	if uid == "" {
		return patches, nil
	}
	if uid != string(pod.UID) {
		return nil, errors.New("external OOM action pod UID changed")
	}
	increasesMemoryRequest := false
	for _, p := range patches {
		parts := strings.Split(p.Path, "/")
		if len(parts) < 5 || parts[1] != "spec" || parts[2] != "containers" || parts[4] != "resources" || p.Op != "add" {
			return nil, errors.New("external OOM action attempted a non-memory resize")
		}
		i, err := strconv.Atoi(parts[3])
		if err != nil || i < 0 || i >= len(pod.Spec.Containers) {
			return nil, errors.New("invalid external OOM container patch")
		}
		encoded, err := json.Marshal(p.Value)
		if err != nil {
			return nil, err
		}
		if (len(parts) == 5 || (len(parts) == 6 && (parts[5] == "requests" || parts[5] == "limits"))) && string(encoded) == "{}" {
			continue // Only empty parent objects may be initialized.
		}
		if len(parts) != 7 || parts[6] != "memory" || (parts[5] != "requests" && parts[5] != "limits") {
			return nil, errors.New("external OOM action attempted a non-memory resize")
		}
		var target resource.Quantity
		if err := target.UnmarshalJSON(encoded); err != nil {
			return nil, err
		}
		container := pod.Spec.Containers[i]
		requests, limits := resourcehelpers.ContainerRequestsAndLimits(container.Name, pod)
		desired, effective := container.Resources.Requests.Memory(), requests.Memory()
		if parts[5] == "limits" {
			desired, effective = container.Resources.Limits.Memory(), limits.Memory()
		}
		if target.Cmp(*desired) < 0 || target.Cmp(*effective) < 0 {
			return nil, errors.New("external OOM action would decrease memory")
		}
		if parts[5] == "requests" && target.Cmp(*desired) > 0 && target.Cmp(*effective) > 0 {
			increasesMemoryRequest = true
		}
	}
	if !increasesMemoryRequest {
		return nil, errors.New("external OOM action has no memory request increase")
	}
	preconditions := []resource_updates.PatchRecord{{Op: "test", Path: "/metadata/uid", Value: uid}}
	if pod.ResourceVersion != "" {
		preconditions = append(preconditions, resource_updates.PatchRecord{Op: "test", Path: "/metadata/resourceVersion", Value: pod.ResourceVersion})
	}
	for i, container := range pod.Spec.Containers {
		for field, resources := range map[string]corev1.ResourceList{"requests": container.Resources.Requests, "limits": container.Resources.Limits} {
			if value, exists := resources[corev1.ResourceMemory]; exists {
				preconditions = append(preconditions, resource_updates.PatchRecord{Op: "test", Path: fmt.Sprintf("/spec/containers/%d/resources/%s/memory", i, field), Value: value.String()})
			}
		}
	}
	return append(preconditions, patches...), nil
}
