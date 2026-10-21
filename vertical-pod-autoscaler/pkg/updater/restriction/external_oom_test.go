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
	"k8s.io/client-go/kubernetes/fake"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/patch"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/recommendation"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/inplace"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/limitrange"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/oomsignal"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func TestExternalOOMResizePreconditions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*corev1.Pod)
		wantError bool
	}{
		{"memory increase", func(*corev1.Pod) {}, false},
		{"replacement UID", func(p *corev1.Pod) { p.UID = "replacement" }, true},
		{"concurrent request increase", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1300Mi")
		}, true},
		{"concurrent limit increase", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("3Gi")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			mode := vpa_types.UpdateModeInPlaceOrRecreate
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "ns", UID: "uid"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")}}}}}}
			vpa := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode}}}
			signals := setExternalOOMSignals(t, vpa, []oomsignal.Signal{{MetricFingerprint: oomsignal.Fingerprint("app_oom_total"), PodName: pod.Name, PodUID: pod.UID, ContainerName: "app", ObservedAt: metav1.NewTime(now.Add(-time.Second)), MemoryRequest: resource.MustParse("1Gi")}})
			resources := &vpa_types.RecommendedPodResources{ContainerRecommendations: []vpa_types.RecommendedContainerResources{{ContainerName: "app", Target: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("1200Mi")}}}}
			action := vpa_api_util.PrepareExternalOOMUpdate(vpa, pod, resources, signals, now)
			require.NotNil(t, action)
			actual := pod.DeepCopy()
			tc.mutate(actual)
			client := fake.NewClientset(actual)
			processor := vpa_api_util.NewCappingRecommendationProcessor(limitrange.NewNoopLimitsCalculator())
			provider := recommendation.NewProvider(limitrange.NewNoopLimitsCalculator(), processor)
			cr := podReplicaCreator{Name: "controller", Namespace: "ns"}
			restriction := &PodsInPlaceRestrictionImpl{client: client, podToReplicaCreatorMap: map[string]podReplicaCreator{getPodID(pod): cr}, creatorToSingleGroupStatsMap: map[podReplicaCreator]singleGroupStats{cr: {}}, patchCalculators: []patch.Calculator{inplace.NewResourceInPlaceUpdatesCalculator(provider)}}
			err := restriction.InPlaceUpdate(pod, action, test.FakeEventRecorder())
			updated, getErr := client.CoreV1().Pods("ns").Get(t.Context(), pod.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			if tc.wantError {
				require.Error(t, err)
				require.Equal(t, actual.Spec, updated.Spec, "a failed precondition must leave all resources unchanged")
			} else {
				require.NoError(t, err)
				require.Equal(t, "1200Mi", updated.Spec.Containers[0].Resources.Requests.Memory().String())
				require.Equal(t, "2400Mi", updated.Spec.Containers[0].Resources.Limits.Memory().String())
				require.Equal(t, "500m", updated.Spec.Containers[0].Resources.Requests.Cpu().String())
				require.Equal(t, "1", updated.Spec.Containers[0].Resources.Limits.Cpu().String())
			}
		})
	}
}

func setExternalOOMSignals(t *testing.T, vpa *vpa_types.VerticalPodAutoscaler, signals []oomsignal.Signal) []oomsignal.Signal {
	t.Helper()
	if vpa.Name == "" {
		vpa.Name = "vpa"
	}
	if vpa.UID == "" {
		vpa.UID = "vpa-uid"
	}
	for i := range signals {
		signals[i].Namespace = vpa.Namespace
		signals[i].VPAName = vpa.Name
		signals[i].VPAUID = vpa.UID
	}
	return signals
}
