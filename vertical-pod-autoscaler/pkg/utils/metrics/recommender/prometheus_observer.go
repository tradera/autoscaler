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

// Fork-specific (tradera/autoscaler) metrics for the per-VPA Prometheus
// OOM observer. Lets operators evaluate whether the observer is doing
// its job — polling, emitting events, and tracking the expected number
// of pods — without correlating across logs and the underlying counter.
//
// Labels are kept at the VPA level deliberately: per-pod labels would
// explode cardinality for VPAs targeting Deployments with many pods,
// and the source counter (whatever the user annotates) already carries
// pod-level detail for the cases where that resolution is needed.

package recommender

import (
	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusObserverPollResult labels values for the polls counter.
type PrometheusObserverPollResult string

const (
	// PollOK means the query returned a valid (possibly empty) vector
	// and was processed.
	PollOK PrometheusObserverPollResult = "ok"
	// PollError means the Prometheus query itself failed (transport,
	// timeout, parse error).
	PollError PrometheusObserverPollResult = "error"
	// PollUnexpectedType means the query returned a value that wasn't
	// a vector. Indicates a broken selector.
	PollUnexpectedType PrometheusObserverPollResult = "unexpected_type"
)

var (
	oomObserverPollsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "oom_observer_polls_total",
			Help:      "Per-VPA count of OOM observer polls, by result. Lets you check that the observer is running and queries are succeeding.",
		},
		[]string{"namespace", "vpa_name", "result"},
	)

	oomObserverEventsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "oom_observer_events_total",
			Help:      "Per-VPA count of synthetic OOM events emitted by the Prometheus observer. Each event drives the recommender's OOM bump-up path.",
		},
		[]string{"namespace", "vpa_name"},
	)

	oomObserverResetsCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "oom_observer_resets_total",
			Help:      "Per-VPA count of counter resets the OOM observer detected (current < previous). Typically pod restart or scrape restart.",
		},
		[]string{"namespace", "vpa_name"},
	)

	oomObserverTrackedPods = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "oom_observer_tracked_pods",
			Help:      "Per-VPA number of (pod, container) tuples with a stored baseline counter value.",
		},
		[]string{"namespace", "vpa_name"},
	)
)

// RegisterPrometheusObserver registers the fork-specific OOM observer
// metrics. Called from the recommender's Register() so the metrics share
// the same /metrics endpoint as the upstream ones.
func RegisterPrometheusObserver() {
	prometheus.MustRegister(
		oomObserverPollsCount,
		oomObserverEventsCount,
		oomObserverResetsCount,
		oomObserverTrackedPods,
	)
}

// RecordPrometheusObserverPoll increments the poll counter for vpa with
// the given result. Call exactly once per pollVPA invocation.
func RecordPrometheusObserverPoll(namespace, vpaName string, result PrometheusObserverPollResult) {
	oomObserverPollsCount.WithLabelValues(namespace, vpaName, string(result)).Inc()
}

// AddPrometheusObserverEvents records the number of synthetic OOM events
// emitted on this poll. Pass 0 when no events were emitted to keep the
// label series alive.
func AddPrometheusObserverEvents(namespace, vpaName string, count int) {
	oomObserverEventsCount.WithLabelValues(namespace, vpaName).Add(float64(count))
}

// IncPrometheusObserverReset records one detected counter reset.
func IncPrometheusObserverReset(namespace, vpaName string) {
	oomObserverResetsCount.WithLabelValues(namespace, vpaName).Inc()
}

// SetPrometheusObserverTrackedPods reports the size of the per-VPA
// state map after a poll. Useful to spot stuck state pruning.
func SetPrometheusObserverTrackedPods(namespace, vpaName string, count int) {
	oomObserverTrackedPods.WithLabelValues(namespace, vpaName).Set(float64(count))
}

// DeletePrometheusObserverSeries drops the label-value series for a VPA
// that no longer exists. Without this the counters and gauge keep
// reporting stale namespace/vpa_name pairs.
func DeletePrometheusObserverSeries(namespace, vpaName string) {
	oomObserverPollsCount.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "vpa_name": vpaName})
	oomObserverEventsCount.DeleteLabelValues(namespace, vpaName)
	oomObserverResetsCount.DeleteLabelValues(namespace, vpaName)
	oomObserverTrackedPods.DeleteLabelValues(namespace, vpaName)
}
