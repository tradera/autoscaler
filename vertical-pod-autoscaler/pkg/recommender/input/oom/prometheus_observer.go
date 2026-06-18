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
	"fmt"
	"math"
	"sync"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	metrics_recommender "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/metrics/recommender"
)

// promQueryAPI is the slice of the Prometheus v1 API that this observer
// needs. Defined locally so tests can stub it without implementing the full
// prometheusv1.API surface.
type promQueryAPI interface {
	Query(ctx context.Context, query string, ts time.Time, opts ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error)
}

// clusterStateView is the slice of model.ClusterState the observer reads.
// Same rationale as promQueryAPI: keeps tests light and decouples from
// future ClusterState additions.
type clusterStateView interface {
	VPAs() map[model.VpaID]*model.Vpa
	Pods() map[model.PodID]*model.PodState
}

// podCtrKey is the per-(pod, container) key for tracked counter values.
type podCtrKey struct {
	pod       string
	container string
}

// PrometheusObserver polls a per-VPA Prometheus counter and emits OomInfo
// records into a shared channel for each observed counter increase.
//
// It exists for runtimes that catch out-of-memory conditions internally and
// don't trigger a container OOMKill — most notably .NET, where an
// OutOfMemoryException is caught and the pod keeps running, leaving the
// recommender blind to memory pressure. By exposing a counter that the
// runtime increments on each OOM event, those events can drive VPA's
// existing OOM bump-up logic.
//
// The observer queries the counter's absolute value on each poll and
// tracks the last-seen value per (pod, container) in process. Deltas are
// computed in-process, NOT via PromQL increase(). Rationale: increase()
// extrapolates across a [range] window, which silently returns nothing
// when samples don't land at both window boundaries (a common pattern
// with OTLP-pushed counters where exporter intervals don't align with
// the observer's poll interval). Each OOM event drives a VPA bump-up,
// so dropped events translate directly to under-sized recommendations.
//
// The observer does NOT implement the Observer interface intentionally:
// Observer is a Kubernetes event/informer pattern, while this is a
// timer-driven poller. They coexist by writing to the same OomInfo channel.
type PrometheusObserver struct {
	promAPI        promQueryAPI
	clusterState   clusterStateView
	oomChan        chan<- OomInfo
	pollInterval   time.Duration
	queryTimeout   time.Duration
	podLabel       string
	containerLabel string

	// values holds the most recent counter value observed for each
	// (VPA, pod, container). The first observation of a pod establishes
	// a baseline (no events emitted); subsequent observations emit
	// floor(current - previous) events.
	mu     sync.Mutex
	values map[model.VpaID]map[podCtrKey]float64
}

// PrometheusObserverConfig is the configuration for PrometheusObserver.
type PrometheusObserverConfig struct {
	// API is the Prometheus client. Reuse history.NewPrometheusAPI to share
	// the same instrumentation as the history provider.
	API prometheusv1.API
	// ClusterState is read on every poll to discover VPAs with the OOM
	// counter annotation and to look up matching pods and container memory
	// requests.
	ClusterState model.ClusterState
	// OomChan receives synthetic OomInfo records, typically the same channel
	// the existing event-driven Observer writes to.
	OomChan chan<- OomInfo
	// PollInterval is how often we query Prometheus. Each poll computes the
	// delta against the previous poll's observed counter value, so the
	// interval controls reaction latency only — it does not affect event
	// count fidelity.
	PollInterval time.Duration
	// QueryTimeout bounds each poll's PromQL execution.
	QueryTimeout time.Duration
	// PodLabel and ContainerLabel are the Prometheus label names used to
	// identify the pod and container. Match the labels emitted by the
	// counter exporter (commonly "pod" and "container").
	PodLabel       string
	ContainerLabel string
}

// NewPrometheusObserver constructs a PrometheusObserver from cfg.
func NewPrometheusObserver(cfg PrometheusObserverConfig) *PrometheusObserver {
	return &PrometheusObserver{
		promAPI:        cfg.API,
		clusterState:   cfg.ClusterState,
		oomChan:        cfg.OomChan,
		pollInterval:   cfg.PollInterval,
		queryTimeout:   cfg.QueryTimeout,
		podLabel:       cfg.PodLabel,
		containerLabel: cfg.ContainerLabel,
		values:         make(map[model.VpaID]map[podCtrKey]float64),
	}
}

// Run polls Prometheus on the configured interval until ctx is done. Intended
// to be launched in its own goroutine.
func (o *PrometheusObserver) Run(ctx context.Context) {
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.pollOnce(ctx)
		}
	}
}

func (o *PrometheusObserver) pollOnce(ctx context.Context) {
	vpas := o.clusterState.VPAs()
	for _, vpa := range vpas {
		selector := annotations.OOMCounterMetric(vpa.Annotations)
		if selector == "" {
			continue
		}
		o.pollVPA(ctx, vpa, selector)
	}
	o.pruneVPAs(vpas)
}

// pruneVPAs drops per-pod state for VPAs that no longer exist. Without this
// the map grows unboundedly on VPA churn. Also drops the corresponding
// metric label series so the per-VPA observability surface stays in sync.
func (o *PrometheusObserver) pruneVPAs(current map[model.VpaID]*model.Vpa) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for id := range o.values {
		if _, ok := current[id]; !ok {
			delete(o.values, id)
			metrics_recommender.DeletePrometheusObserverSeries(id.Namespace, id.VpaName)
		}
	}
}

// pollVPA queries Prometheus for the counter selector annotated on this VPA.
// The selector is the user's instant vector selector (e.g.
// `dotnet_oome{deployment="api"}`); we only wrap it with sum-by. We do NOT
// additionally filter by VPA pod selector — the user's matchers own scoping.
func (o *PrometheusObserver) pollVPA(ctx context.Context, vpa *model.Vpa, selector string) {
	queryCtx, cancel := context.WithTimeout(ctx, o.queryTimeout)
	defer cancel()

	// sum-by collapses orthogonal labels (e.g. exception type) into one
	// value per (pod, container). The bare selector returns the most recent
	// sample in Prometheus's lookback window, which is exactly what
	// in-process delta tracking wants.
	query := fmt.Sprintf(
		"sum by (%s, %s) (%s)",
		o.podLabel, o.containerLabel, selector,
	)

	val, warns, err := o.promAPI.Query(queryCtx, query, time.Now())
	if err != nil {
		// State is preserved on query failure: the next successful poll
		// computes against the last-known value, so transient Prometheus
		// gaps don't lose events.
		metrics_recommender.RecordPrometheusObserverPoll(vpa.ID.Namespace, vpa.ID.VpaName, metrics_recommender.PollError)
		klog.V(2).InfoS("Prometheus OOM query failed", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "err", err)
		return
	}
	for _, w := range warns {
		klog.V(4).InfoS("Prometheus OOM query warning", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "warning", w)
	}

	samples, ok := val.(prommodel.Vector)
	if !ok {
		metrics_recommender.RecordPrometheusObserverPoll(vpa.ID.Namespace, vpa.ID.VpaName, metrics_recommender.PollUnexpectedType)
		klog.V(2).InfoS("Prometheus OOM query returned unexpected type", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "type", fmt.Sprintf("%T", val))
		return
	}
	metrics_recommender.RecordPrometheusObserverPoll(vpa.ID.Namespace, vpa.ID.VpaName, metrics_recommender.PollOK)

	pods := o.clusterState.Pods()

	o.mu.Lock()
	vpaState, ok := o.values[vpa.ID]
	if !ok {
		vpaState = make(map[podCtrKey]float64)
		o.values[vpa.ID] = vpaState
	}
	o.mu.Unlock()

	totalEmitted := 0
	for _, sample := range samples {
		podName := string(sample.Metric[prommodel.LabelName(o.podLabel)])
		ctrName := string(sample.Metric[prommodel.LabelName(o.containerLabel)])
		if podName == "" || ctrName == "" {
			continue
		}
		current := float64(sample.Value)
		if math.IsNaN(current) || math.IsInf(current, 0) {
			continue
		}

		key := podCtrKey{pod: podName, container: ctrName}

		o.mu.Lock()
		previous, hadPrevious := vpaState[key]
		vpaState[key] = current
		o.mu.Unlock()

		if !hadPrevious {
			klog.V(4).InfoS("Prometheus OOM observer: storing baseline", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "pod", podName, "container", ctrName, "value", current)
			continue
		}

		delta := current - previous
		if delta < 0 {
			// Counter reset (pod scrape restarted, or the metric source
			// itself reset). The new absolute value is the count of events
			// since the reset; emit those. Mild under-count beats the
			// alternative of emitting `previous + current` and double-
			// counting the reset boundary.
			metrics_recommender.IncPrometheusObserverReset(vpa.ID.Namespace, vpa.ID.VpaName)
			klog.V(2).InfoS("Prometheus OOM observer: counter reset detected", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "pod", podName, "container", ctrName, "from", previous, "to", current)
			delta = current
		}

		// Floor: fractional values can appear when the counter is exposed
		// as a float (sum-by, or a rate-derived counter). Each emitted
		// OomInfo represents one discrete event.
		count := int64(math.Floor(delta))
		if count <= 0 {
			continue
		}

		podID := model.PodID{Namespace: vpa.ID.Namespace, PodName: podName}
		podState, ok := pods[podID]
		if !ok {
			continue
		}
		container, ok := podState.Containers[ctrName]
		if !ok {
			continue
		}
		memory := container.Request[model.ResourceMemory]
		if memory == 0 {
			klog.V(4).InfoS("Prometheus OOM observer: skipping container with no memory request", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "pod", podName, "container", ctrName)
			continue
		}

		ts := sample.Timestamp.Time().UTC()
		for range count {
			o.oomChan <- OomInfo{
				Timestamp: ts,
				Memory:    memory,
				ContainerID: model.ContainerID{
					PodID:         podID,
					ContainerName: ctrName,
				},
			}
		}
		totalEmitted += int(count)
		klog.V(2).InfoS("Prometheus OOM observer: emitted events", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "pod", podName, "container", ctrName, "count", count)
	}
	// Always record (even zero) to keep the counter and gauge series
	// present so dashboards can show "observer up, no events".
	metrics_recommender.AddPrometheusObserverEvents(vpa.ID.Namespace, vpa.ID.VpaName, totalEmitted)

	// Drop per-pod state for pods that no longer exist in ClusterState.
	// We intentionally do NOT drop state for pods that are merely absent
	// from this query result: that absence is just as likely to be a
	// transient Prometheus gap, and dropping would reset the baseline and
	// silently lose events on the next successful query.
	o.mu.Lock()
	for key := range vpaState {
		podID := model.PodID{Namespace: vpa.ID.Namespace, PodName: key.pod}
		if _, ok := pods[podID]; !ok {
			delete(vpaState, key)
		}
	}
	tracked := len(vpaState)
	o.mu.Unlock()
	metrics_recommender.SetPrometheusObserverTrackedPods(vpa.ID.Namespace, vpa.ID.VpaName, tracked)
}
