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
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// fakePromAPI is a minimal stub for promQueryAPI. Tests inject canned
// per-resource results so the test reads naturally.
type fakePromAPI struct {
	results map[string]prommodel.Value
	err     error
	calls   []string
}

func (f *fakePromAPI) Query(_ context.Context, query string, _ time.Time, _ ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error) {
	f.calls = append(f.calls, query)
	if f.err != nil {
		return nil, nil, f.err
	}
	if v, ok := f.results[query]; ok {
		return v, nil, nil
	}
	return prommodel.Vector{}, nil, nil
}

type fakeClusterState struct {
	vpas map[model.VpaID]*model.Vpa
}

func (f *fakeClusterState) VPAs() map[model.VpaID]*model.Vpa { return f.vpas }

func vpaForMetrics(ns, name string, ann map[string]string) *model.Vpa {
	return &model.Vpa{
		ID:          model.VpaID{Namespace: ns, VpaName: name},
		Annotations: ann,
		PodCount:    1,
	}
}

func promSample(podLabel, podName, ctrLabel, ctrName string, v float64, ts time.Time) *prommodel.Sample {
	return &prommodel.Sample{
		Metric: prommodel.Metric{
			prommodel.LabelName(podLabel): prommodel.LabelValue(podName),
			prommodel.LabelName(ctrLabel): prommodel.LabelValue(ctrName),
		},
		Value:     prommodel.SampleValue(v),
		Timestamp: prommodel.TimeFromUnixNano(ts.UnixNano()),
	}
}

func TestPrometheusMetricsClient_QueriesPerResource(t *testing.T) {
	vpa := vpaForMetrics("ns", "vpa1", map[string]string{
		annotations.ExternalCPUMetricAnnotation:    `cpu_metric{deployment="api"}`,
		annotations.ExternalMemoryMetricAnnotation: `mem_metric{deployment="api"}`,
	})

	api := &fakePromAPI{
		results: map[string]prommodel.Value{
			`sum by (pod, container) (cpu_metric{deployment="api"})`: prommodel.Vector{
				promSample("pod", "pod-a", "container", "ctr", 0.25, time.Now()),
			},
			`sum by (pod, container) (mem_metric{deployment="api"})`: prommodel.Vector{
				promSample("pod", "pod-a", "container", "ctr", 256<<20, time.Now()),
			},
		},
	}

	cs := &fakeClusterState{vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa}}
	c := &prometheusMetricsClient{
		promAPI:      api,
		clusterState: cs,
		options: PrometheusClientOptions{
			PodNameLabel:       "pod",
			ContainerNameLabel: "container",
			QueryTimeout:       time.Second,
		},
	}

	got, err := c.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, api.calls, 2, "one query per resource")
	assert.Len(t, got.Items, 1, "one pod metrics result")
	pm := got.Items[0]
	assert.Equal(t, "pod-a", pm.Name)
	assert.Len(t, pm.Containers, 1)
	cpu := pm.Containers[0].Usage[corev1.ResourceCPU]
	mem := pm.Containers[0].Usage[corev1.ResourceMemory]
	// CPU 0.25 cores → 250m
	assert.Equal(t, int64(250), cpu.MilliValue())
	// Memory 256 MiB → bytes
	assert.Equal(t, int64(256<<20), mem.Value())
	// SI format must match k8s convention for each resource.
	assert.Equal(t, resource.DecimalSI, cpu.Format)
	assert.Equal(t, resource.BinarySI, mem.Format)
}

func TestPrometheusMetricsClient_FallsBackToGlobalDefault(t *testing.T) {
	// VPA without per-VPA annotation. Global ResourceMetrics defaults apply.
	vpa := vpaForMetrics("ns", "vpa1", nil)
	api := &fakePromAPI{
		results: map[string]prommodel.Value{
			`sum by (pod, container) (default_mem{namespace="ns"})`: prommodel.Vector{
				promSample("pod", "pod-a", "container", "ctr", 42, time.Now()),
			},
		},
	}
	cs := &fakeClusterState{vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa}}
	c := &prometheusMetricsClient{
		promAPI:      api,
		clusterState: cs,
		options: PrometheusClientOptions{
			ResourceMetrics: map[corev1.ResourceName]string{
				corev1.ResourceMemory: `default_mem{namespace="ns"}`,
			},
			PodNameLabel:       "pod",
			ContainerNameLabel: "container",
			QueryTimeout:       time.Second,
		},
	}

	got, err := c.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, got.Items, 1)
	mem := got.Items[0].Containers[0].Usage[corev1.ResourceMemory]
	assert.Equal(t, int64(42), mem.Value())
}

func TestPrometheusMetricsClient_AnnotatedOnlySkipsUnannotated(t *testing.T) {
	plain := vpaForMetrics("ns", "plain", nil)
	annotated := vpaForMetrics("ns", "annotated", map[string]string{
		annotations.ExternalMemoryMetricAnnotation: `m{}`,
	})
	api := &fakePromAPI{
		results: map[string]prommodel.Value{
			`sum by (pod, container) (m{})`: prommodel.Vector{
				promSample("pod", "pod-a", "container", "ctr", 1, time.Now()),
			},
		},
	}
	cs := &fakeClusterState{vpas: map[model.VpaID]*model.Vpa{plain.ID: plain, annotated.ID: annotated}}
	c := &prometheusMetricsClient{
		promAPI:      api,
		clusterState: cs,
		options: PrometheusClientOptions{
			PodNameLabel:       "pod",
			ContainerNameLabel: "container",
			QueryTimeout:       time.Second,
			AnnotatedVPAsOnly:  true,
		},
	}

	got, err := c.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, api.calls, 1, "plain VPA must not trigger a query")
	assert.Len(t, got.Items, 1)
}

func TestPrometheusMetricsClient_QueryErrorIsSwallowed(t *testing.T) {
	// A failing PromQL query for one resource must not abort the whole
	// poll: a CPU error shouldn't drop a successful memory observation.
	vpa := vpaForMetrics("ns", "vpa1", map[string]string{
		annotations.ExternalCPUMetricAnnotation:    `broken`,
		annotations.ExternalMemoryMetricAnnotation: `ok`,
	})
	api := &fakePromAPI{
		err: errors.New("server unavailable"),
	}
	cs := &fakeClusterState{vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa}}
	c := &prometheusMetricsClient{
		promAPI:      api,
		clusterState: cs,
		options: PrometheusClientOptions{
			PodNameLabel:       "pod",
			ContainerNameLabel: "container",
			QueryTimeout:       time.Second,
		},
	}

	got, err := c.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err, "client.List itself must not error on query failure")
	assert.Empty(t, got.Items)
}

func TestPrometheusMetricsClient_SkipsZeroPodCount(t *testing.T) {
	vpa := vpaForMetrics("ns", "vpa1", map[string]string{
		annotations.ExternalMemoryMetricAnnotation: `m`,
	})
	vpa.PodCount = 0
	api := &fakePromAPI{}
	cs := &fakeClusterState{vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa}}
	c := &prometheusMetricsClient{
		promAPI:      api,
		clusterState: cs,
		options: PrometheusClientOptions{
			PodNameLabel:       "pod",
			ContainerNameLabel: "container",
			QueryTimeout:       time.Second,
		},
	}

	_, err := c.List(context.Background(), "", metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Empty(t, api.calls, "VPA with no pods must not be queried")
}
