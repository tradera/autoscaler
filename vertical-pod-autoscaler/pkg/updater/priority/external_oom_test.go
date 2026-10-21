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

package priority

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/limitrange"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

type rejectExternalAdmission struct{}

func (*rejectExternalAdmission) LoopInit([]*corev1.Pod, map[*vpa_types.VerticalPodAutoscaler][]*corev1.Pod) {
}
func (*rejectExternalAdmission) CleanUp() {}
func (*rejectExternalAdmission) Admit(*corev1.Pod, *vpa_types.RecommendedPodResources) bool {
	return false
}

func TestExternalOOMBypassesGatesWithScopedAction(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	mode := vpa_types.UpdateModeInPlaceOrRecreate
	vpa := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Generation: 1, Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode}}, Status: vpa_types.VerticalPodAutoscalerStatus{Recommendation: &vpa_types.RecommendedPodResources{ContainerRecommendations: []vpa_types.RecommendedContainerResources{{ContainerName: "app", Target: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("1100Mi")}}}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "ns", UID: "uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}, Status: corev1.PodStatus{StartTime: &metav1.Time{Time: now.Add(-time.Minute)}}}
	processor := vpa_api_util.NewCappingRecommendationProcessor(limitrange.NewNoopLimitsCalculator())
	calculate := func(admission PodEvictionAdmission) []PodUpdate {
		calc := NewUpdatePriorityCalculator(vpa, updateconfig, processor, NewProcessor())
		calc.AddPod(pod, now, map[types.UID]*vpa_types.RecommendedPodResources{})
		return calc.GetSortedPodUpdates(admission)
	}
	require.Empty(t, calculate(NewDefaultPodEvictionAdmission()), "young pod inside bounds uses ordinary waiting rules")
	vpa.Status.ExternalOOMSignals = []vpa_types.ExternalOOMSignal{{Metric: "app_oom_total", PodName: pod.Name, PodUID: pod.UID, ContainerName: "app", ObservedAt: metav1.NewTime(now.Add(-time.Second)), MemoryRequest: resource.MustParse("1Gi"), ObservedGeneration: 1}}
	updates := calculate(NewDefaultPodEvictionAdmission())
	require.Len(t, updates, 1)
	require.True(t, updates[0].ExternalOOM)
	require.NotSame(t, vpa, updates[0].VPA)
	target := updates[0].VPA.Status.Recommendation.ContainerRecommendations[0].Target
	require.NotContains(t, target, corev1.ResourceCPU)
	require.Equal(t, "1100Mi", target.Memory().String())
	require.Empty(t, calculate(&rejectExternalAdmission{}), "quick actions still require admission")
	vpa.Spec.ResourcePolicy = &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{ContainerName: "app", MaxAllowed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}}}
	require.Empty(t, calculate(NewDefaultPodEvictionAdmission()), "external OOM cannot bypass maxAllowed")
	vpa.Spec.ResourcePolicy = nil
	// The same signal must not expedite another step after an in-place increase.
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1050Mi")
	require.Empty(t, calculate(NewDefaultPodEvictionAdmission()))
}
