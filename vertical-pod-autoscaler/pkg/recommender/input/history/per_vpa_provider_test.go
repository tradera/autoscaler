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

package history

import (
	"context"
	"errors"
	"testing"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

type fakeRangeAPI struct {
	queries []string
	results map[string]prommodel.Value
	err     error
}

func (f *fakeRangeAPI) QueryRange(_ context.Context, query string, _ prometheusv1.Range, _ ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error) {
	f.queries = append(f.queries, query)
	if f.err != nil {
		return nil, nil, f.err
	}
	if v, ok := f.results[query]; ok {
		return v, nil, nil
	}
	return prommodel.Matrix{}, nil, nil
}

func newProvider(api perVPAQueryAPI) *PerVPAProvider {
	return &PerVPAProvider{
		api: api,
		opts: PerVPAProviderOpts{
			QueryTimeout:      5 * time.Second,
			HistoryDuration:   24 * time.Hour,
			HistoryResolution: time.Hour,
			PodLabel:          "pod",
			ContainerLabel:    "container",
		},
	}
}

func vpa(ns, name string, ann map[string]string) *model.Vpa {
	return &model.Vpa{
		ID:          model.VpaID{Namespace: ns, VpaName: name},
		Annotations: ann,
	}
}

// fixedTestTime keeps the matrix helper deterministic.
var fixedTestTime = time.Unix(1_700_000_000, 0)

func matrix(podName, ctrName string, values ...float64) prommodel.Matrix {
	pairs := make([]prommodel.SamplePair, len(values))
	for i, v := range values {
		pairs[i] = prommodel.SamplePair{
			Timestamp: prommodel.TimeFromUnixNano(fixedTestTime.Add(-time.Duration(len(values)-i) * time.Hour).UnixNano()),
			Value:     prommodel.SampleValue(v),
		}
	}
	return prommodel.Matrix{{
		Metric: prommodel.Metric{
			"pod":       prommodel.LabelValue(podName),
			"container": prommodel.LabelValue(ctrName),
		},
		Values: pairs,
	}}
}

func TestPerVPAProvider_NoAnnotations(t *testing.T) {
	api := &fakeRangeAPI{}
	p := newProvider(api)

	got, err := p.GetVPAHistory(context.Background(), vpa("ns", "v", nil))
	assert.NoError(t, err)
	assert.Empty(t, got)
	assert.Empty(t, api.queries, "no queries should be issued for VPAs without history annotations")
}

func TestPerVPAProvider_MemoryOnly(t *testing.T) {
	memQuery := `max_over_time(dotnet_gc_total_bytes[5m])`
	api := &fakeRangeAPI{
		results: map[string]prommodel.Value{
			memQuery: matrix("pod-a", "ctr", 100, 200, 300),
		},
	}
	p := newProvider(api)
	v := vpa("ns", "v", map[string]string{
		annotations.HistoryQueryMemoryAnnotation: memQuery,
	})

	got, err := p.GetVPAHistory(context.Background(), v)
	assert.NoError(t, err)
	assert.Equal(t, []string{memQuery}, api.queries, "only the memory query should be issued")

	podHist, ok := got[model.PodID{Namespace: "ns", PodName: "pod-a"}]
	assert.True(t, ok, "pod history should be present")
	assert.Len(t, podHist.Samples["ctr"], 3)
	for _, s := range podHist.Samples["ctr"] {
		assert.Equal(t, model.ResourceMemory, s.Resource)
	}
}

func TestPerVPAProvider_BothQueries(t *testing.T) {
	cpuQuery := "rate(cpu[5m])"
	memQuery := "mem"
	api := &fakeRangeAPI{
		results: map[string]prommodel.Value{
			cpuQuery: matrix("pod-a", "ctr", 0.5),
			memQuery: matrix("pod-a", "ctr", 1024),
		},
	}
	p := newProvider(api)
	v := vpa("ns", "v", map[string]string{
		annotations.HistoryQueryCPUAnnotation:    cpuQuery,
		annotations.HistoryQueryMemoryAnnotation: memQuery,
	})

	got, err := p.GetVPAHistory(context.Background(), v)
	assert.NoError(t, err)
	assert.Len(t, api.queries, 2, "both queries issued")
	assert.Contains(t, api.queries, cpuQuery)
	assert.Contains(t, api.queries, memQuery)

	podHist := got[model.PodID{Namespace: "ns", PodName: "pod-a"}]
	assert.Len(t, podHist.Samples["ctr"], 2, "samples for both resources merged into the same container")
}

func TestPerVPAProvider_QueryError(t *testing.T) {
	api := &fakeRangeAPI{err: errors.New("boom")}
	p := newProvider(api)
	v := vpa("ns", "v", map[string]string{
		annotations.HistoryQueryMemoryAnnotation: "x",
	})
	_, err := p.GetVPAHistory(context.Background(), v)
	assert.Error(t, err)
}

func TestPerVPAProvider_DropsSeriesWithoutLabels(t *testing.T) {
	memQuery := "m"
	api := &fakeRangeAPI{
		results: map[string]prommodel.Value{
			memQuery: prommodel.Matrix{
				{
					Metric: prommodel.Metric{"pod": "p"}, // missing container
					Values: []prommodel.SamplePair{{Timestamp: 0, Value: 1}},
				},
				{
					Metric: prommodel.Metric{"container": "c"}, // missing pod
					Values: []prommodel.SamplePair{{Timestamp: 0, Value: 1}},
				},
				{
					Metric: prommodel.Metric{"pod": "ok-pod", "container": "ok-ctr"},
					Values: []prommodel.SamplePair{{Timestamp: 0, Value: 42}},
				},
			},
		},
	}
	p := newProvider(api)
	v := vpa("ns", "v", map[string]string{annotations.HistoryQueryMemoryAnnotation: memQuery})

	got, _ := p.GetVPAHistory(context.Background(), v)
	assert.Len(t, got, 1, "only the fully-labeled series should produce a pod history")
	assert.Contains(t, got, model.PodID{Namespace: "ns", PodName: "ok-pod"})
}
