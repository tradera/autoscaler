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

func TestMultiSource_PrefersExternalAndDropsDefaultDuplicate(t *testing.T) {
	external := stubLister{items: []v1beta1.PodMetrics{
		podMetric("ns1", "pod-a", "ctr", "100m", "256Mi"),
	}}
	def := stubLister{items: []v1beta1.PodMetrics{
		podMetric("ns1", "pod-a", "ctr", "999m", "999Mi"), // duplicate; should be dropped
		podMetric("ns1", "pod-b", "ctr", "200m", "512Mi"), // no override; kept
		podMetric("ns2", "pod-a", "ctr", "300m", "1Gi"),   // different ns; kept
	}}

	ms := NewMultiSource(def, external)
	got, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, got.Items, 3)

	cpuByPod := map[string]string{}
	for _, p := range got.Items {
		cpuByPod[p.Namespace+"/"+p.Name] = p.Containers[0].Usage.Cpu().String()
	}
	assert.Equal(t, "100m", cpuByPod["ns1/pod-a"], "external should win for ns1/pod-a")
	assert.Equal(t, "200m", cpuByPod["ns1/pod-b"])
	assert.Equal(t, "300m", cpuByPod["ns2/pod-a"])
}

func TestMultiSource_PassesThroughErrors(t *testing.T) {
	wantErr := errors.New("boom")
	ms := NewMultiSource(stubLister{}, stubLister{err: wantErr})
	_, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.ErrorIs(t, err, wantErr)

	ms = NewMultiSource(stubLister{err: wantErr}, stubLister{})
	_, err = ms.List(context.Background(), "", metav1.ListOptions{})
	assert.ErrorIs(t, err, wantErr)
}

func TestMultiSource_EmptySources(t *testing.T) {
	ms := NewMultiSource(stubLister{}, stubLister{})
	got, err := ms.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Empty(t, got.Items)
}
