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

package input

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/recommendation"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_fake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	vpa_lister "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/listers/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/inplace"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/priority"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/limitrange"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func externalOOMFixture(t *testing.T) (*clusterStateFeeder, *vpa_types.VerticalPodAutoscaler, *corev1.Pod, oom.OomInfo) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	mode := vpa_types.UpdateModeInPlaceOrRecreate
	source := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vpa", Generation: 1, Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "ns", UID: "uid-1", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}, Status: corev1.PodStatus{StartTime: &metav1.Time{Time: now.Add(-time.Minute)}}}
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "sidecar", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")}}})
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, indexer.Add(pod))
	cs := model.NewClusterState(time.Hour)
	require.NoError(t, cs.AddOrUpdateVpa(source, labels.Everything()))
	id := model.ContainerID{PodID: model.PodID{Namespace: pod.Namespace, PodName: pod.Name}, ContainerName: "app"}
	cs.AddOrUpdatePod(id.PodID, labels.Set{}, corev1.PodRunning)
	require.NoError(t, cs.AddOrUpdateContainer(id, model.Resources{model.ResourceMemory: 1024 * 1024 * 1024}))
	ch := make(chan oom.OomInfo, 10)
	feeder := &clusterStateFeeder{clusterState: cs, podLister: listersv1.NewPodLister(indexer), oomChan: ch, metricsClient: fakeMetricsClient{}}
	info := oom.OomInfo{ContainerID: id, Timestamp: now.Add(-time.Second), Memory: 1024 * 1024 * 1024, PodUID: pod.UID, VpaID: model.VpaID{Namespace: "ns", VpaName: "vpa"}, VpaGeneration: 1, Metric: "app_oom_total"}
	return feeder, source, pod, info
}

func TestExternalOOMCounterToStatusAndQuickUpdate(t *testing.T) {
	feeder, source, pod, info := externalOOMFixture(t)
	var counter, queries atomic.Int64
	counter.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		value := fmt.Sprint(counter.Load())
		payload := map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": []any{map[string]any{"metric": map[string]string{"pod": pod.Name, "container": "app"}, "value": []any{float64(time.Now().UnixMilli()) / 1000, value}}}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
		queries.Add(1)
	}))
	defer server.Close()
	client, err := promapi.NewClient(promapi.Config{Address: server.URL})
	require.NoError(t, err)
	ch := make(chan oom.OomInfo, 10)
	feeder.oomChan = ch
	vpaIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, vpaIndexer.Add(source))
	observer := oom.NewPrometheusObserver(oom.PrometheusObserverConfig{API: prometheusv1.NewAPI(client), VPALister: vpa_lister.NewVerticalPodAutoscalerLister(vpaIndexer), PodLister: feeder.podLister, OomChan: ch, PollInterval: 5 * time.Millisecond, QueryTimeout: time.Second, PodLabel: "pod", ContainerLabel: "container"})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); observer.Run(ctx) }()
	defer func() { cancel(); <-done }()
	require.Eventually(t, func() bool { return queries.Load() >= 2 }, time.Second, time.Millisecond)
	counter.Store(2)
	require.Eventually(t, func() bool { return len(ch) > 0 }, time.Second, time.Millisecond)
	// Production polling must use informer snapshots, not race the main
	// loop while it records the signal and updates mutable model metadata.
	feeder.LoadRealTimeMetrics(t.Context())
	status := feeder.clusterState.VPAs()[info.VpaID].AsStatus()
	require.Len(t, status.ExternalOOMSignals, 1)
	require.Equal(t, pod.UID, status.ExternalOOMSignals[0].PodUID)
	require.NotZero(t, feeder.clusterState.GetContainer(info.ContainerID).GetMaxMemoryPeak())
	// Persist and read through the same status patch API as the recommender.
	apiClient := vpa_fake.NewSimpleClientset(source) //nolint:staticcheck // Generated client has no replacement constructor.
	_, err = vpa_api_util.UpdateVpaStatusIfNeeded(apiClient.AutoscalingV1().VerticalPodAutoscalers("ns"), "vpa", status, &source.Status)
	require.NoError(t, err)
	published, err := apiClient.AutoscalingV1().VerticalPodAutoscalers("ns").Get(t.Context(), "vpa", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, published.Status.ExternalOOMSignals, 1)
	// No artificial pod OOMKilled status is needed. A current recommendation
	// inside its bounds can now update this young pod without CPU downsizing.
	published.Status.Recommendation = &vpa_types.RecommendedPodResources{ContainerRecommendations: []vpa_types.RecommendedContainerResources{{ContainerName: "app", Target: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("1200Mi")}}}}
	calc := priority.NewUpdatePriorityCalculator(published, priority.UpdateConfig{PodLifetimeUpdateThreshold: 12 * time.Hour, MinChangePriority: 1}, vpa_api_util.NewCappingRecommendationProcessor(limitrange.NewNoopLimitsCalculator()), priority.NewProcessor())
	calc.AddPod(pod, time.Now(), nil)
	actions := calc.GetSortedPodUpdates(priority.NewDefaultPodEvictionAdmission())
	require.Len(t, actions, 1)
	require.True(t, actions[0].ExternalOOM)
	require.NotContains(t, actions[0].VPA.Status.Recommendation.ContainerRecommendations[0].Target, corev1.ResourceCPU)
	provider := recommendation.NewProvider(limitrange.NewNoopLimitsCalculator(), vpa_api_util.NewCappingRecommendationProcessor(limitrange.NewNoopLimitsCalculator()))
	patches, err := inplace.NewResourceInPlaceUpdatesCalculator(provider).CalculatePatches(pod, actions[0].VPA)
	require.NoError(t, err)
	require.Len(t, patches, 2)
	paths := []string{patches[0].Path, patches[1].Path}
	require.ElementsMatch(t, []string{"/spec/containers/0/resources/requests/memory", "/spec/containers/0/resources/limits/memory"}, paths)
}

func TestExternalOOMRejectedObservationsDoNotPublish(t *testing.T) {
	cases := []struct {
		name   string
		change func(*oom.OomInfo)
	}{
		{"wrong uid", func(i *oom.OomInfo) { i.PodUID = "different" }},
		{"wrong generation", func(i *oom.OomInfo) { i.VpaGeneration++ }},
		{"changed metric", func(i *oom.OomInfo) { i.Metric = "other_counter" }},
		{"future", func(i *oom.OomInfo) { i.Timestamp = time.Now().Add(time.Minute) }},
		{"unknown container", func(i *oom.OomInfo) { i.ContainerID.ContainerName = "missing" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, _, _, i := externalOOMFixture(t)
			tc.change(&i)
			require.Error(t, f.recordOOM(i))
			require.Empty(t, f.clusterState.VPAs()[i.VpaID].AsStatus().ExternalOOMSignals)
		})
	}
	t.Run("expired signal still contributes to histogram", func(t *testing.T) {
		f, _, _, i := externalOOMFixture(t)
		i.Timestamp = time.Now().Add(-11 * time.Minute)
		require.NoError(t, f.recordOOM(i))
		require.NotZero(t, f.clusterState.GetContainer(i.ContainerID).GetMaxMemoryPeak())
		require.Empty(t, f.clusterState.VPAs()[i.VpaID].AsStatus().ExternalOOMSignals)
	})
	t.Run("histogram rejects old observation", func(t *testing.T) {
		f, _, _, i := externalOOMFixture(t)
		// Place the model more than an aggregation interval ahead of the event.
		require.NoError(t, f.clusterState.AddSample(&model.ContainerUsageSampleWithKey{Container: i.ContainerID, ContainerUsageSample: model.ContainerUsageSample{MeasureStart: time.Now().Add(48 * time.Hour), Resource: model.ResourceMemory, Usage: 1}}))
		require.Error(t, f.recordOOM(i))
		require.Empty(t, f.clusterState.VPAs()[i.VpaID].AsStatus().ExternalOOMSignals)
	})
}
