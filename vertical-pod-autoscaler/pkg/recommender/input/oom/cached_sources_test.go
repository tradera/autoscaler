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
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_lister "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/listers/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

func TestCachedSourcesRespectRecommenderAndNamespace(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	source := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "custom", Namespace: "ns", Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{{Name: "custom"}}}}
	require.NoError(t, indexer.Add(source))
	defaultSource := source.DeepCopy()
	defaultSource.Name = "default"
	defaultSource.Spec.Recommenders = nil
	require.NoError(t, indexer.Add(defaultSource))
	ignored := source.DeepCopy()
	ignored.Namespace = "ignored"
	require.NoError(t, indexer.Add(ignored))
	observer := &PrometheusObserver{vpaLister: vpa_lister.NewVerticalPodAutoscalerLister(indexer), recommenderName: "custom", ignoredNamespaces: []string{"ignored"}}
	sources := observer.sourceVPAs()
	id := model.VpaID{Namespace: "ns", VpaName: "custom"}
	require.Len(t, sources, 1)
	require.Contains(t, sources, id)
	sources[id].Annotations[annotations.OOMCounterMetricAnnotation] = "changed"
	require.Equal(t, "app_oom_total", source.Annotations[annotations.OOMCounterMetricAnnotation], "worker metadata cannot alias the informer object")
	observer.recommenderName = "default"
	sources = observer.sourceVPAs()
	require.Len(t, sources, 1)
	require.Contains(t, sources, model.VpaID{Namespace: "ns", VpaName: "default"})
}
