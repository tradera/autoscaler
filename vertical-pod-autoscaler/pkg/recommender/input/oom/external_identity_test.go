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

package oom

import (
	"context"
	"testing"
	"time"

	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

func TestPrometheusObserverIdentityAndMetricChangesEstablishBaseline(t *testing.T) {
	now := time.Now()
	vpa := vpaWithOOMAnnotation("ns", "vpa", "app_oom_total")
	vpa.UID = "original-vpa"
	cs := &fakeClusterState{vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa}, pods: map[model.PodID]*model.PodState{}}
	p := podWithMemRequest("ns", "worker", "app", 1024*1024*1024)
	cs.pods[p.ID] = p
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "worker", UID: "old"}}
	pod.Spec.Containers = []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}}}
	require.NoError(t, indexer.Add(pod))
	var results []prommodel.Value
	for _, value := range []float64{1, 2, 5, 6, 99, 100, 101, 102} {
		results = append(results, prommodel.Vector{sample("worker", "app", value, now)})
	}
	api := &fakeAPI{results: results}
	ch := make(chan OomInfo, 10)
	observer := newTestObserver(api, cs, ch)
	observer.podLister = listersv1.NewPodLister(indexer)
	observer.pollOnce(context.Background())
	observer.pollOnce(context.Background())
	require.Len(t, ch, 1)
	event := <-ch
	require.Equal(t, pod.UID, event.PodUID)
	require.Equal(t, vpa.ID, event.VpaID)
	require.Equal(t, vpa.UID, event.VpaUID)
	require.Equal(t, "app_oom_total", event.Metric)
	replacement := pod.DeepCopy()
	replacement.UID = "new"
	require.NoError(t, indexer.Update(replacement))
	observer.pollOnce(context.Background())
	require.Empty(t, ch, "same name with new UID must not inherit the counter baseline")
	require.Len(t, observer.values[vpa.ID], 1, "old UID state must be pruned")
	observer.pollOnce(context.Background())
	require.Len(t, ch, 1)
	require.Equal(t, replacement.UID, (<-ch).PodUID)
	vpa.Annotations[annotations.OOMCounterMetricAnnotation] = "other_oom_total"
	observer.pollOnce(context.Background())
	require.Empty(t, ch, "a different metric establishes a baseline")
	observer.pollOnce(context.Background())
	require.Len(t, ch, 1)
	require.Equal(t, "other_oom_total", (<-ch).Metric)
	vpa.UID = "replacement-vpa"
	observer.pollOnce(context.Background())
	require.Empty(t, ch, "a replacement VPA must establish a baseline even with unchanged selector and pod")
	observer.pollOnce(context.Background())
	require.Len(t, ch, 1)
	require.Equal(t, vpa.UID, (<-ch).VpaUID)
	require.Len(t, observer.values[vpa.ID], 1)
}
