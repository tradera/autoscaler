/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"math"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// PodMetricsLister wraps both metrics-client and the per-VPA Prometheus
// metrics source.
type PodMetricsLister interface {
	List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error)
}

// podMetricsSource is the metrics-client source of metrics.
type podMetricsSource struct {
	metricsGetter resourceclient.PodMetricsesGetter
}

// NewPodMetricsesSource Returns a Source-wrapper around PodMetricsesGetter.
func NewPodMetricsesSource(source resourceclient.PodMetricsesGetter) PodMetricsLister {
	return podMetricsSource{metricsGetter: source}
}

func (s podMetricsSource) List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	podMetricsInterface := s.metricsGetter.PodMetricses(namespace)
	return podMetricsInterface.List(ctx, opts)
}

// promQueryAPI is the slice of the Prometheus v1 API the client needs.
// Defined locally so tests can stub it without implementing the full
// prometheusv1.API surface.
type promQueryAPI interface {
	Query(ctx context.Context, query string, ts time.Time, opts ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error)
}

// clusterStateView is the slice of model.ClusterState this client reads.
// Defined locally so tests can stub it without implementing the full
// ClusterState interface.
type clusterStateView interface {
	VPAs() map[model.VpaID]*model.Vpa
}

// prometheusMetricsClient is a PodMetricsLister that synthesizes pod metrics
// from Prometheus queries. The metric to query is per-VPA via the
// `external.vpa.k8s.io/{cpu,memory}-metric` annotations, with a cluster-wide
// fallback in PrometheusClientOptions.ResourceMetrics.
//
// Naming note: the annotations are still under the `external.vpa.k8s.io/`
// prefix because that's where the fork's per-VPA metric overrides live in
// general; "external" here means "external to VPA's default observation
// source", not "via the Kubernetes External Metrics API". This client
// bypasses that API entirely and queries Prometheus directly — same
// transport the per-VPA OOM observer uses, so clusters that don't run an
// external-metrics adapter (prometheus-adapter, KEDA configured for the
// metric, …) work out of the box.
type prometheusMetricsClient struct {
	promAPI      promQueryAPI
	options      PrometheusClientOptions
	clusterState clusterStateView
}

// PrometheusClientOptions specifies parameters for the Prometheus metrics
// source.
type PrometheusClientOptions struct {
	// ResourceMetrics is the cluster-wide default metric selector per
	// resource, applied when a VPA does not set the per-resource
	// annotation. The value is parsed as a Prometheus instant vector
	// selector (e.g. `metric_name{matcher,...}`); a bare metric name is
	// equivalent to `metric_name{}`.
	ResourceMetrics map[corev1.ResourceName]string
	// PodNameLabel and ContainerNameLabel are the Prometheus label names
	// used to attribute returned samples back to a (pod, container).
	// The sum-by groups by these labels, so additional labels on the
	// source series (exception type, etc.) are collapsed.
	PodNameLabel       string
	ContainerNameLabel string
	// QueryTimeout bounds each PromQL query.
	QueryTimeout time.Duration
	// AnnotatedVPAsOnly, when true, restricts the client to only iterate
	// VPAs that opt into per-VPA metric overrides via annotations. Used
	// when the client is composed inside a multiSource alongside
	// metrics-server, so non-annotated VPAs are served by metrics-server
	// instead of being silently skipped or double-counted.
	AnnotatedVPAsOnly bool
}

// NewPrometheusClient returns a PodMetricsLister backed by direct Prometheus
// queries. Shares the same prometheusv1.API instance used by the per-VPA
// OOM observer and history backfiller so they all observe the same source.
func NewPrometheusClient(promAPI prometheusv1.API, clusterState model.ClusterState, options PrometheusClientOptions) PodMetricsLister {
	return &prometheusMetricsClient{
		promAPI:      promAPI,
		options:      options,
		clusterState: clusterState,
	}
}

func (s *prometheusMetricsClient) List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	result := v1beta1.PodMetricsList{}

	for _, vpa := range s.clusterState.VPAs() {
		if vpa.PodCount == 0 {
			continue
		}

		if namespace != "" && vpa.ID.Namespace != namespace {
			continue
		}

		if s.options.AnnotatedVPAsOnly && !annotations.HasExternalMetricOverride(vpa.Annotations) {
			continue
		}

		s.appendVPASamples(ctx, vpa, &result)
	}
	return &result, nil
}

// appendVPASamples issues one Prometheus query per opted-in resource (CPU,
// memory) for this VPA. No per-pod fan-out and no VPA pod-selector filtering
// — the caller's PromQL selector, or the global flag default, owns scoping.
// Returned samples are bucketed into PodMetrics keyed by the configured
// pod label.
func (s *prometheusMetricsClient) appendVPASamples(ctx context.Context, vpa *model.Vpa, out *v1beta1.PodMetricsList) {
	type ctrKey struct{ pod, container string }
	usage := make(map[ctrKey]corev1.ResourceList)
	var timestamp metav1.Time

	for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		raw := s.selectorStringFor(vpa, res)
		if raw == "" {
			continue
		}

		// sum-by mirrors PrometheusObserver: groups by exactly the labels
		// we need so any orthogonal labels on the source series collapse
		// into one value per (pod, container).
		query := fmt.Sprintf(
			"sum by (%s, %s) (%s)",
			s.options.PodNameLabel, s.options.ContainerNameLabel, raw,
		)

		queryCtx, cancel := context.WithTimeout(ctx, s.options.QueryTimeout)
		val, warns, err := s.promAPI.Query(queryCtx, query, time.Now())
		cancel()
		if err != nil {
			klog.V(2).InfoS("Prometheus metrics query failed", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", res, "err", err)
			continue
		}
		for _, w := range warns {
			klog.V(4).InfoS("Prometheus metrics query warning", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "warning", w)
		}
		samples, ok := val.(prommodel.Vector)
		if !ok {
			klog.V(2).InfoS("Prometheus metrics query returned unexpected type", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", res, "type", fmt.Sprintf("%T", val))
			continue
		}
		if len(samples) == 0 {
			klog.V(4).InfoS("Prometheus metrics query returned no samples", "vpa", klog.KRef(vpa.ID.Namespace, vpa.ID.VpaName), "resource", res, "query", query)
			continue
		}

		for _, sample := range samples {
			podName := string(sample.Metric[prommodel.LabelName(s.options.PodNameLabel)])
			ctrName := string(sample.Metric[prommodel.LabelName(s.options.ContainerNameLabel)])
			if podName == "" || ctrName == "" {
				continue
			}
			v := float64(sample.Value)
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			ts := sample.Timestamp.Time().UTC()
			if timestamp.IsZero() || ts.After(timestamp.Time) {
				timestamp = metav1.NewTime(ts)
			}
			key := ctrKey{pod: podName, container: ctrName}
			if usage[key] == nil {
				usage[key] = make(corev1.ResourceList)
			}
			usage[key][res] = sampleToQuantity(res, v)
		}
	}

	if len(usage) == 0 {
		return
	}
	perPod := make(map[string]*v1beta1.PodMetrics)
	for key, usageList := range usage {
		pm, ok := perPod[key.pod]
		if !ok {
			pm = &v1beta1.PodMetrics{
				ObjectMeta: metav1.ObjectMeta{Namespace: vpa.ID.Namespace, Name: key.pod},
				Timestamp:  timestamp,
			}
			perPod[key.pod] = pm
		}
		pm.Containers = append(pm.Containers, v1beta1.ContainerMetrics{Name: key.container, Usage: usageList})
	}
	for _, pm := range perPod {
		out.Items = append(out.Items, *pm)
	}
}

// selectorStringFor returns the raw instant-vector-selector string for the
// given resource, falling back from VPA annotation to the global flag.
func (s *prometheusMetricsClient) selectorStringFor(vpa *model.Vpa, res corev1.ResourceName) string {
	if v := annotations.ExternalMetricForResource(vpa.Annotations, res); v != "" {
		return v
	}
	return s.options.ResourceMetrics[res]
}

// sampleToQuantity converts a Prometheus float sample to the resource.Quantity
// shape PodMetrics expects:
//   - Memory: integer bytes (BinarySI). cAdvisor's
//     container_memory_working_set_bytes is the canonical source.
//   - CPU: millicores (DecimalSI), scaled from cores. PromQL counter
//     metrics typically already report rate-per-second of cores
//     (e.g. rate(container_cpu_usage_seconds_total[1m])).
func sampleToQuantity(res corev1.ResourceName, v float64) resource.Quantity {
	switch res {
	case corev1.ResourceMemory:
		if v < 0 {
			v = 0
		}
		return *resource.NewQuantity(int64(v), resource.BinarySI)
	case corev1.ResourceCPU:
		if v < 0 {
			v = 0
		}
		return *resource.NewMilliQuantity(int64(v*1000), resource.DecimalSI)
	default:
		return resource.Quantity{}
	}
}
