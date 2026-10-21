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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

func TestPrepareExternalOOMUpdate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := func() (*vpa_types.VerticalPodAutoscaler, *corev1.Pod, *vpa_types.RecommendedPodResources) {
		mode := vpa_types.UpdateModeInPlaceOrRecreate
		vpa := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Generation: 3, Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode}}}
		setExternalOOMSignals(t, vpa, []annotations.ExternalOOMSignal{{Metric: "app_oom_total", PodName: "worker", PodUID: "uid-1", ContainerName: "app", ObservedAt: metav1.NewTime(now.Add(-time.Minute)), MemoryRequest: resource.MustParse("1Gi"), ObservedGeneration: 3}})
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "ns", UID: "uid-1"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}, {Name: "sidecar"}}}}
		rec := &vpa_types.RecommendedPodResources{ContainerRecommendations: []vpa_types.RecommendedContainerResources{{ContainerName: "app", Target: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("1200Mi")}}, {ContainerName: "sidecar", Target: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Mi")}}}}
		return vpa, pod, rec
	}
	t.Run("memory only action preserves original", func(t *testing.T) {
		vpa, pod, rec := fixture()
		action := PrepareExternalOOMUpdate(vpa, pod, rec, now)
		require.NotNil(t, action)
		require.Len(t, action.Status.Recommendation.ContainerRecommendations, 1)
		target := action.Status.Recommendation.ContainerRecommendations[0].Target
		require.Len(t, target, 1)
		require.Equal(t, "1200Mi", target.Memory().String())
		policy := GetContainerResourcePolicy("app", action.Spec.ResourcePolicy)
		require.Equal(t, []corev1.ResourceName{corev1.ResourceMemory}, *policy.ControlledResources)
		require.Equal(t, vpa_types.ContainerScalingModeOff, *GetContainerResourcePolicy("sidecar", action.Spec.ResourcePolicy).Mode)
		require.Nil(t, vpa.Status.Recommendation)
		require.Len(t, rec.ContainerRecommendations[0].Target, 2)
	})
	cases := []struct {
		name   string
		change func(*vpa_types.VerticalPodAutoscaler, *corev1.Pod, *vpa_types.RecommendedPodResources)
	}{
		{"wrong uid", func(v *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.UID = "uid-2"
		}},
		{"wrong name", func(_ *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.Name = "other"
		}},
		{"wrong namespace", func(_ *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.Namespace = "other"
		}},
		{"stale generation", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			v.Generation++
		}},
		{"expired", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			signals := annotations.ParseExternalOOMSignals(v.Annotations)
			signals[0].ObservedAt = metav1.NewTime(now.Add(-11 * time.Minute))
			setExternalOOMSignals(t, v, signals)
		}},
		{"future", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			signals := annotations.ParseExternalOOMSignals(v.Annotations)
			signals[0].ObservedAt = metav1.NewTime(now.Add(time.Second))
			setExternalOOMSignals(t, v, signals)
		}},
		{"request already increased", func(_ *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1100Mi")
		}},
		{"desired request increased before status catches up", func(_ *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", Resources: &corev1.ResourceRequirements{Requests: p.Spec.Containers[0].Resources.Requests.DeepCopy()}}}
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1100Mi")
		}},
		{"no increase", func(_ *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, r *vpa_types.RecommendedPodResources) {
			r.ContainerRecommendations[0].Target[corev1.ResourceMemory] = resource.MustParse("1Gi")
		}},
		{"decrease", func(_ *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, r *vpa_types.RecommendedPodResources) {
			r.ContainerRecommendations[0].Target[corev1.ResourceMemory] = resource.MustParse("512Mi")
		}},
		{"recreate", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			mode := vpa_types.UpdateModeRecreate
			v.Spec.UpdatePolicy.UpdateMode = &mode
		}},
		{"cpu boost owned by boost worker", func(_ *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.Annotations = map[string]string{annotations.GetStartupCPUBoostAnnotationKey("sidecar"): "{}"}
		}},
		{"changed metric", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			v.Annotations[annotations.OOMCounterMetricAnnotation] = "different_metric"
		}},
		{"unobserved container", func(_ *vpa_types.VerticalPodAutoscaler, p *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			p.Annotations = map[string]string{annotations.VpaObservedContainersLabel: "sidecar"}
		}},
		{"opt out", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			v.Annotations = nil
		}},
		{"container off", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			off := vpa_types.ContainerScalingModeOff
			v.Spec.ResourcePolicy = &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{ContainerName: "app", Mode: &off}}}
		}},
		{"memory uncontrolled", func(v *vpa_types.VerticalPodAutoscaler, _ *corev1.Pod, _ *vpa_types.RecommendedPodResources) {
			cpu := []corev1.ResourceName{corev1.ResourceCPU}
			v.Spec.ResourcePolicy = &vpa_types.PodResourcePolicy{ContainerPolicies: []vpa_types.ContainerResourcePolicy{{ContainerName: "app", ControlledResources: &cpu}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, p, r := fixture()
			tc.change(v, p, r)
			require.Nil(t, PrepareExternalOOMUpdate(v, p, r, now))
		})
	}
}

func setExternalOOMSignals(t *testing.T, vpa *vpa_types.VerticalPodAutoscaler, signals []annotations.ExternalOOMSignal) {
	t.Helper()
	value, err := annotations.EncodeExternalOOMSignals(signals, vpa.Annotations)
	require.NoError(t, err)
	vpa.Annotations[annotations.ExternalOOMSignalsAnnotation] = value
}
