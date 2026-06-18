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
	"errors"
	"testing"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// fakeAPI is a stub for promQueryAPI that returns a queue of canned results.
type fakeAPI struct {
	calls   int
	results []prommodel.Value
	err     error
}

func (f *fakeAPI) Query(_ context.Context, _ string, _ time.Time, _ ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error) {
	f.calls++
	if f.err != nil {
		return nil, nil, f.err
	}
	if len(f.results) == 0 {
		return prommodel.Vector{}, nil, nil
	}
	v := f.results[0]
	if len(f.results) > 1 {
		f.results = f.results[1:]
	}
	return v, nil, nil
}

// fakeClusterState implements just the slice of ClusterState the observer
// reads.
type fakeClusterState struct {
	vpas map[model.VpaID]*model.Vpa
	pods map[model.PodID]*model.PodState
}

func (f *fakeClusterState) VPAs() map[model.VpaID]*model.Vpa      { return f.vpas }
func (f *fakeClusterState) Pods() map[model.PodID]*model.PodState { return f.pods }

func vpaWithOOMAnnotation(ns, name, metric string) *model.Vpa {
	return &model.Vpa{
		ID:          model.VpaID{Namespace: ns, VpaName: name},
		Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: metric},
	}
}

func podWithMemRequest(ns, name, ctr string, memBytes int64) *model.PodState {
	return &model.PodState{
		ID: model.PodID{Namespace: ns, PodName: name},
		Containers: map[string]*model.ContainerState{
			ctr: {
				Request: model.Resources{
					model.ResourceMemory: model.ResourceAmount(memBytes),
				},
			},
		},
	}
}

func sample(podName, ctrName string, value float64, ts time.Time) *prommodel.Sample {
	return &prommodel.Sample{
		Metric: prommodel.Metric{
			"pod":       prommodel.LabelValue(podName),
			"container": prommodel.LabelValue(ctrName),
		},
		Value:     prommodel.SampleValue(value),
		Timestamp: prommodel.TimeFromUnixNano(ts.UnixNano()),
	}
}

func newTestObserver(api promQueryAPI, cs clusterStateView, ch chan<- OomInfo) *PrometheusObserver {
	return &PrometheusObserver{
		promAPI:        api,
		clusterState:   cs,
		oomChan:        ch,
		pollInterval:   time.Minute,
		queryTimeout:   5 * time.Second,
		podLabel:       "pod",
		containerLabel: "container",
		values:         make(map[model.VpaID]map[podCtrKey]float64),
	}
}

func TestPrometheusObserver_FirstPollEstablishesBaseline(t *testing.T) {
	// The first observation of a (pod, container) is the baseline. No
	// events are emitted; the value is recorded so subsequent polls can
	// compute deltas.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "dotnet_oome")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 256<<20)

	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{
		results: []prommodel.Value{
			prommodel.Vector{sample("pod-a", "ctr", 5, time.Now())},
			prommodel.Vector{sample("pod-a", "ctr", 8, time.Now())},
		},
	}
	ch := make(chan OomInfo, 4)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	assert.Equal(t, 1, api.calls)
	assert.Empty(t, ch, "first poll establishes the baseline; no events emitted")

	o.pollOnce(context.Background())
	assert.Equal(t, 2, api.calls)
	assert.Len(t, ch, 3, "second poll should emit delta = 8 - 5 = 3 events")

	got := <-ch
	assert.Equal(t, "ns", got.ContainerID.Namespace)
	assert.Equal(t, "pod-a", got.ContainerID.PodName)
	assert.Equal(t, "ctr", got.ContainerID.ContainerName)
	assert.Equal(t, model.ResourceAmount(256<<20), got.Memory)
}

func TestPrometheusObserver_FractionalDeltaFloored(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 0, time.Now())},
		prommodel.Vector{sample("pod-a", "ctr", 2.7, time.Now())},
	}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Len(t, ch, 2, "delta 2.7 should floor to 2 events")
}

func TestPrometheusObserver_SkipsZeroOrNegativeDelta(t *testing.T) {
	// Counter unchanged between two polls — no events.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 7, time.Now())},
		prommodel.Vector{sample("pod-a", "ctr", 7, time.Now())},
	}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Empty(t, ch, "unchanged counter should not produce events")
}

func TestPrometheusObserver_CounterResetEmitsAbsoluteValue(t *testing.T) {
	// Counter went down (pod scrape restart or metric reset). The new
	// absolute value is the count since reset — emit those, accepting a
	// small under-count at the reset boundary rather than double-counting.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 20, time.Now())},
		prommodel.Vector{sample("pod-a", "ctr", 3, time.Now())},
	}}
	ch := make(chan OomInfo, 32)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Len(t, ch, 3, "reset (20 -> 3) should emit the new absolute value as events")
}

func TestPrometheusObserver_PreservesStateAcrossQueryFailures(t *testing.T) {
	// State must survive transient Prometheus failures. Two successful
	// polls bracket a failing one; the delta is computed across the
	// failure, no events lost.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 10, time.Now())},
	}}
	ch := make(chan OomInfo, 32)
	o := newTestObserver(api, cs, ch)

	// Baseline at 10.
	o.pollOnce(context.Background())
	assert.Empty(t, ch)

	// Prometheus glitch.
	api.err = errors.New("prometheus down")
	o.pollOnce(context.Background())
	assert.Empty(t, ch)

	// Recovered, counter at 15 — should emit 5 (delta from the pre-glitch
	// baseline of 10).
	api.err = nil
	api.results = []prommodel.Value{prommodel.Vector{sample("pod-a", "ctr", 15, time.Now())}}
	o.pollOnce(context.Background())
	assert.Len(t, ch, 5, "delta must be computed against the last successful poll, not reset on glitch")
}

func TestPrometheusObserver_PreservesStateAcrossEmptyQueryResults(t *testing.T) {
	// A query that returns an empty vector (no series at all) must not
	// reset the per-pod baselines. The pod might just be quietly not
	// generating samples in this window, or Prometheus might be slow to
	// scrape — either way, dropping state would cause the next non-empty
	// poll to silently lose events.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 4, time.Now())},
		prommodel.Vector{},
		prommodel.Vector{sample("pod-a", "ctr", 7, time.Now())},
	}}
	ch := make(chan OomInfo, 32)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background()) // baseline 4
	o.pollOnce(context.Background()) // empty result, state preserved
	o.pollOnce(context.Background()) // 7, delta = 3

	assert.Len(t, ch, 3, "state must survive empty queries; delta = 7 - 4 = 3")
}

func TestPrometheusObserver_PerPodBaseline(t *testing.T) {
	// Each pod's first observation is its own baseline. Pods appearing
	// later don't get retroactive events when they first show up.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	podA := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	podB := podWithMemRequest("ns", "pod-b", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{podA.ID: podA, podB.ID: podB},
	}
	now := time.Now()
	api := &fakeAPI{results: []prommodel.Value{
		// First poll: only pod-a present.
		prommodel.Vector{sample("pod-a", "ctr", 5, now)},
		// Second poll: pod-a grew by 2, pod-b appears for first time.
		prommodel.Vector{
			sample("pod-a", "ctr", 7, now),
			sample("pod-b", "ctr", 11, now),
		},
		// Third poll: pod-a grew by 1, pod-b grew by 4.
		prommodel.Vector{
			sample("pod-a", "ctr", 8, now),
			sample("pod-b", "ctr", 15, now),
		},
	}}
	ch := make(chan OomInfo, 32)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	assert.Empty(t, ch, "pod-a first observation = baseline")

	o.pollOnce(context.Background())
	assert.Len(t, ch, 2, "pod-a delta 5->7 = 2 events; pod-b first observation = baseline")

	// Drain pod-a's events.
	for range 2 {
		<-ch
	}

	o.pollOnce(context.Background())
	assert.Len(t, ch, 5, "pod-a delta 7->8 = 1; pod-b delta 11->15 = 4")
}

func TestPrometheusObserver_SkipsUnannotatedVPAs(t *testing.T) {
	vpaPlain := &model.Vpa{ID: model.VpaID{Namespace: "ns", VpaName: "plain"}}
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpaPlain.ID: vpaPlain},
	}
	api := &fakeAPI{}
	o := newTestObserver(api, cs, make(chan OomInfo, 1))
	o.pollOnce(context.Background())
	assert.Equal(t, 0, api.calls, "no query should be issued for VPA without annotation")
}

func TestPrometheusObserver_DoesNotFilterByVPASelector(t *testing.T) {
	// The user's annotation selector is the source of scoping; the observer
	// MUST NOT additionally filter results by VPA pod selector. If the user's
	// matchers leak across workloads, that's their responsibility.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	known := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	alsoKnown := podWithMemRequest("ns", "pod-other", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{
			known.ID:     known,
			alsoKnown.ID: alsoKnown,
		},
	}
	now := time.Now()
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{
			sample("pod-a", "ctr", 0, now),
			sample("pod-other", "ctr", 0, now),
		},
		prommodel.Vector{
			sample("pod-a", "ctr", 1, now),
			sample("pod-other", "ctr", 5, now),
		},
	}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Len(t, ch, 6, "events from every pod returned by the user's selector should be emitted")
}

func TestPrometheusObserver_SkipsUnknownPods(t *testing.T) {
	// Pods present in Prometheus results but not in clusterState (e.g.
	// already deleted, or in a different namespace the observer doesn't
	// track) get dropped because we can't resolve their memory request.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	known := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{known.ID: known},
	}
	now := time.Now()
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{
			sample("pod-a", "ctr", 0, now),
			sample("pod-ghost", "ctr", 0, now),
		},
		prommodel.Vector{
			sample("pod-a", "ctr", 1, now),
			sample("pod-ghost", "ctr", 5, now),
		},
	}}
	ch := make(chan OomInfo, 8)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Len(t, ch, 1, "only the known pod should yield events")
	got := <-ch
	assert.Equal(t, "pod-a", got.ContainerID.PodName)
}

func TestPrometheusObserver_SkipsContainerWithNoMemRequest(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := &model.PodState{
		ID: model.PodID{Namespace: "ns", PodName: "pod-a"},
		Containers: map[string]*model.ContainerState{
			"ctr": {Request: model.Resources{}},
		},
	}
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 0, time.Now())},
		prommodel.Vector{sample("pod-a", "ctr", 1, time.Now())},
	}}
	ch := make(chan OomInfo, 4)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	o.pollOnce(context.Background())
	assert.Empty(t, ch, "containers without a memory request must be skipped")
}

func TestPrometheusObserver_PrunesStateForDeletedPods(t *testing.T) {
	// Pods that disappear from ClusterState must have their per-pod state
	// pruned, otherwise the map grows unboundedly on pod churn.
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	podA := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{podA.ID: podA},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 5, time.Now())},
	}}
	ch := make(chan OomInfo, 4)
	o := newTestObserver(api, cs, ch)

	o.pollOnce(context.Background())
	assert.Len(t, o.values[vpa.ID], 1, "pod-a baseline recorded")

	// Pod removed from ClusterState. Empty query result simulates
	// Prometheus also no longer scraping it.
	delete(cs.pods, podA.ID)
	api.results = []prommodel.Value{prommodel.Vector{}}
	o.pollOnce(context.Background())
	assert.Empty(t, o.values[vpa.ID], "state for absent pod must be pruned")
}

func TestPrometheusObserver_PrunesStateForDeletedVPAs(t *testing.T) {
	vpa := vpaWithOOMAnnotation("ns", "vpa1", "m")
	pod := podWithMemRequest("ns", "pod-a", "ctr", 1<<20)
	cs := &fakeClusterState{
		vpas: map[model.VpaID]*model.Vpa{vpa.ID: vpa},
		pods: map[model.PodID]*model.PodState{pod.ID: pod},
	}
	api := &fakeAPI{results: []prommodel.Value{
		prommodel.Vector{sample("pod-a", "ctr", 5, time.Now())},
	}}
	o := newTestObserver(api, cs, make(chan OomInfo, 4))

	o.pollOnce(context.Background())
	assert.Contains(t, o.values, vpa.ID)

	delete(cs.vpas, vpa.ID)
	o.pollOnce(context.Background())
	assert.NotContains(t, o.values, vpa.ID, "state for deleted VPA must be pruned")
}
