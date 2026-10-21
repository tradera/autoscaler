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

package restriction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	resource_updates "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func TestExternalOOMFinalPatchSafety(t *testing.T) {
	now := time.Now()
	mode := vpa_types.UpdateModeInPlace
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "ns", UID: "uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}}}}}}
	vpa := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode}}}
	setExternalOOMSignals(t, vpa, []annotations.ExternalOOMSignal{{Metric: "app_oom_total", PodName: pod.Name, PodUID: pod.UID, ContainerName: "app", ObservedAt: metav1.NewTime(now), MemoryRequest: resource.MustParse("1Gi")}})
	rec := &vpa_types.RecommendedPodResources{ContainerRecommendations: []vpa_types.RecommendedContainerResources{{ContainerName: "app", Target: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1200Mi")}}}}
	action := vpa_api_util.PrepareExternalOOMUpdate(vpa, pod, rec, now)
	require.NotNil(t, action)
	for _, tc := range []struct {
		name, path string
		value      any
	}{
		{"cpu change", "/spec/containers/0/resources/requests/cpu", "100m"},
		{"memory decrease", "/spec/containers/0/resources/requests/memory", "512Mi"},
		{"limit decrease", "/spec/containers/0/resources/limits/memory", "1Gi"},
		{"unchanged request", "/spec/containers/0/resources/requests/memory", "1Gi"},
		{"limit only", "/spec/containers/0/resources/limits/memory", "3Gi"},
		{"nonempty parent", "/spec/containers/0/resources/requests", map[string]string{"cpu": "100m"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := protectExternalOOMResize(pod, action, []resource_updates.PatchRecord{{Op: "add", Path: tc.path, Value: tc.value}})
			require.Error(t, err)
		})
	}
	t.Run("pending larger desired request", func(t *testing.T) {
		snapshot := pod.DeepCopy()
		snapshot.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1300Mi")
		_, err := protectExternalOOMResize(snapshot, action, []resource_updates.PatchRecord{{Op: "add", Path: "/spec/containers/0/resources/requests/memory", Value: "1200Mi"}})
		require.Error(t, err)
	})
}
