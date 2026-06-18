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

package metrics

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

type stubLister struct {
	items []v1beta1.PodMetrics
	err   error
}

func (s stubLister) List(_ context.Context, _ string, _ metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &v1beta1.PodMetricsList{Items: s.items}, nil
}

// stubClusterState reports a fixed VPA set and pod membership.
type stubClusterState struct {
	vpas    map[model.VpaID]*model.Vpa
	matched map[model.VpaID][]model.PodID
}

func (s stubClusterState) VPAs() map[model.VpaID]*model.Vpa { return s.vpas }
func (s stubClusterState) GetMatchingPodsForVPAs(vpas []*model.Vpa) map[model.PodID]bool {
	out := make(map[model.PodID]bool)
	for _, vpa := range vpas {
		for _, p := range s.matched[vpa.ID] {
			out[p] = true
		}
	}
	return out
}

// annotatedVPACluster builds a stubClusterState with a single VPA in ns that
// carries an external-metric annotation and matches the given pod names.
func annotatedVPACluster(ns, vpaName string, podNames ...string) stubClusterState {
	id := model.VpaID{Namespace: ns, VpaName: vpaName}
	vpa := &model.Vpa{
		ID:          id,
		Annotations: map[string]string{annotations.ExternalMemoryMetricAnnotation: "mem_metric{}"},
	}
	pods := make([]model.PodID, 0, len(podNames))
	for _, p := range podNames {
		pods = append(pods, model.PodID{Namespace: ns, PodName: p})
	}
	return stubClusterState{
		vpas:    map[model.VpaID]*model.Vpa{id: vpa},
		matched: map[model.VpaID][]model.PodID{id: pods},
	}
}

func podMetric(ns, name, container string, cpu, mem string) v1beta1.PodMetrics {
	return v1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Containers: []v1beta1.ContainerMetrics{{
			Name: container,
			Usage: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
			},
		}},
	}
}

func TestMultiSource_PrefersExternalForAnnotatedVPAPods(t *testing.T) {
	// The annotated VPA owns ns1/pod-a; only that pod is diverted to external.
	cs := annotatedVPACluster("ns1", "v1", "pod-a")
	external := stubLister{items: []v1beta1.PodMetrics{
		podMetric("ns1", "pod-a", "ctr", "100m", "256Mi"),
	}}
	def := stubLister{items: []v1beta1.PodMetrics{
		podMetric("ns1", "pod-a", "ctr", "999m", "999Mi"), // owned by annotated VPA; dropped
		podMetric("ns1", "pod-b", "ctr", "200m", "512Mi"), // not owned; kept
		podMetric("ns2", "pod-a", "ctr", "300m", "1Gi"),   // different ns; kept
	}}

	ms := NewMultiSource(def, external, cs)
	got, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, got.Items, 3)

	cpuByPod := map[string]string{}
	for _, p := range got.Items {
		cpuByPod[p.Namespace+"/"+p.Name] = p.Containers[0].Usage.Cpu().String()
	}
	assert.Equal(t, "100m", cpuByPod["ns1/pod-a"], "external should win for the annotated VPA's pod")
	assert.Equal(t, "200m", cpuByPod["ns1/pod-b"])
	assert.Equal(t, "300m", cpuByPod["ns2/pod-a"])
}

// A pod that an annotated VPA owns but Prometheus hasn't observed yet (e.g.
// freshly rolled) must not fall back to its metrics-server sample — that RSS
// leak is what poisoned the peak-based memory recommendation.
func TestMultiSource_ExcludesUnobservedAnnotatedPodFromDefault(t *testing.T) {
	cs := annotatedVPACluster("ns1", "v1", "pod-a", "pod-new")
	external := stubLister{items: []v1beta1.PodMetrics{
		podMetric("ns1", "pod-a", "ctr", "100m", "256Mi"), // committed for pod-a only
	}}
	def := stubLister{items: []v1beta1.PodMetrics{
		podMetric("ns1", "pod-a", "ctr", "999m", "999Mi"),   // owned; dropped
		podMetric("ns1", "pod-new", "ctr", "900m", "800Mi"), // owned but unobserved; dropped (no RSS leak)
		podMetric("ns1", "pod-c", "ctr", "200m", "512Mi"),   // not owned; kept
	}}

	ms := NewMultiSource(def, external, cs)
	got, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)

	memByPod := map[string]string{}
	for _, p := range got.Items {
		memByPod[p.Namespace+"/"+p.Name] = p.Containers[0].Usage.Memory().String()
	}
	assert.Len(t, got.Items, 2)
	assert.Equal(t, "256Mi", memByPod["ns1/pod-a"], "annotated pod served from Prometheus")
	assert.Equal(t, "512Mi", memByPod["ns1/pod-c"], "unowned pod served from metrics-server")
	_, leaked := memByPod["ns1/pod-new"]
	assert.False(t, leaked, "a freshly-started pod of an annotated VPA must not leak its metrics-server (RSS) sample")
}

// A non-annotated VPA owning the pod does not divert it from metrics-server.
func TestMultiSource_NonAnnotatedVPADoesNotExclude(t *testing.T) {
	id := model.VpaID{Namespace: "ns1", VpaName: "plain"}
	cs := stubClusterState{
		vpas:    map[model.VpaID]*model.Vpa{id: {ID: id}}, // no annotations
		matched: map[model.VpaID][]model.PodID{id: {{Namespace: "ns1", PodName: "pod-a"}}},
	}
	def := stubLister{items: []v1beta1.PodMetrics{podMetric("ns1", "pod-a", "ctr", "200m", "512Mi")}}

	ms := NewMultiSource(def, stubLister{}, cs)
	got, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, got.Items, 1)
	assert.Equal(t, "512Mi", got.Items[0].Containers[0].Usage.Memory().String())
}

func TestMultiSource_PassesThroughErrors(t *testing.T) {
	wantErr := errors.New("boom")
	empty := stubClusterState{}

	ms := NewMultiSource(stubLister{}, stubLister{err: wantErr}, empty)
	_, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.ErrorIs(t, err, wantErr)

	ms = NewMultiSource(stubLister{err: wantErr}, stubLister{}, empty)
	_, err = ms.List(context.Background(), "", metav1.ListOptions{})
	assert.ErrorIs(t, err, wantErr)
}
