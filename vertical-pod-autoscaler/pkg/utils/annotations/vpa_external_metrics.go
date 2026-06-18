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

package annotations

import (
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	// ExternalMetricsAnnotationPrefix is the namespace for per-VPA external
	// metric source overrides. Pick a domain at fork time.
	ExternalMetricsAnnotationPrefix = "external.vpa.k8s.io/"

	// ExternalCPUMetricAnnotation names the external.metrics.k8s.io metric
	// used as CPU usage for the annotated VPA's pods.
	ExternalCPUMetricAnnotation = ExternalMetricsAnnotationPrefix + "cpu-metric"

	// ExternalMemoryMetricAnnotation names the external.metrics.k8s.io
	// metric used as memory usage for the annotated VPA's pods.
	ExternalMemoryMetricAnnotation = ExternalMetricsAnnotationPrefix + "memory-metric"

	// OOMCounterMetricAnnotation names a Prometheus counter metric whose
	// increases are treated as OOM events for the annotated VPA's pods.
	// Use case: .NET OutOfMemoryException, where the runtime catches the
	// failure and the container does not OOMKill.
	OOMCounterMetricAnnotation = ExternalMetricsAnnotationPrefix + "oom-counter-metric"

	// HistoryQueryCPUAnnotation is a PromQL query used for one-shot CPU
	// history backfill on the annotated VPA's first observation.
	HistoryQueryCPUAnnotation = ExternalMetricsAnnotationPrefix + "history-query-cpu"

	// HistoryQueryMemoryAnnotation is a PromQL query used for one-shot
	// memory history backfill on the annotated VPA's first observation.
	HistoryQueryMemoryAnnotation = ExternalMetricsAnnotationPrefix + "history-query-memory"
)

// ExternalMetricForResource returns the per-VPA external-metrics metric name
// for the given resource, or "" if no annotation is set. Only ResourceCPU and
// ResourceMemory are recognized.
func ExternalMetricForResource(annotations map[string]string, resource corev1.ResourceName) string {
	if annotations == nil {
		return ""
	}
	switch resource {
	case corev1.ResourceCPU:
		return annotations[ExternalCPUMetricAnnotation]
	case corev1.ResourceMemory:
		return annotations[ExternalMemoryMetricAnnotation]
	}
	return ""
}

// HasExternalMetricOverride reports whether a VPA opts into per-VPA external
// metrics for at least one resource.
func HasExternalMetricOverride(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[ExternalCPUMetricAnnotation] != "" ||
		annotations[ExternalMemoryMetricAnnotation] != ""
}

// OOMCounterMetric returns the per-VPA Prometheus OOM counter metric name, or
// "" if not set.
func OOMCounterMetric(annotations map[string]string) string {
	if annotations == nil {
		return ""
	}
	return annotations[OOMCounterMetricAnnotation]
}

// HistoryQueryForResource returns the per-VPA PromQL backfill query for the
// given resource, or "" if not set. Only ResourceCPU and ResourceMemory are
// recognized.
func HistoryQueryForResource(annotations map[string]string, resource corev1.ResourceName) string {
	if annotations == nil {
		return ""
	}
	switch resource {
	case corev1.ResourceCPU:
		return annotations[HistoryQueryCPUAnnotation]
	case corev1.ResourceMemory:
		return annotations[HistoryQueryMemoryAnnotation]
	}
	return ""
}

// HasHistoryQuery reports whether a VPA opts into per-VPA history backfill
// for at least one resource.
func HasHistoryQuery(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[HistoryQueryCPUAnnotation] != "" ||
		annotations[HistoryQueryMemoryAnnotation] != ""
}

// ParseInstantVectorSelector parses a Prometheus-style instant vector
// selector — `metric_name{label="value",label!="value",...}` — into a metric
// name and a Kubernetes labels.Selector. A bare metric name (no `{...}`)
// yields labels.Everything().
//
// The Kubernetes External Metrics API takes (metric_name, labels.Selector)
// rather than raw PromQL, so this conversion is the bridge. PromQL regex
// match operators (=~, !~) are rejected because labels.Selector cannot
// express them.
func ParseInstantVectorSelector(s string) (metricName string, selector labels.Selector, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil, errors.New("empty selector")
	}
	open := strings.IndexByte(s, '{')
	if open < 0 {
		return s, labels.Everything(), nil
	}
	if !strings.HasSuffix(s, "}") {
		return "", nil, fmt.Errorf("malformed selector %q: missing closing brace", s)
	}
	metricName = strings.TrimSpace(s[:open])
	if metricName == "" {
		return "", nil, fmt.Errorf("malformed selector %q: empty metric name", s)
	}
	matchers := s[open+1 : len(s)-1]
	if strings.Contains(matchers, "=~") || strings.Contains(matchers, "!~") {
		return "", nil, errors.New("regex matchers (=~, !~) not supported; use exact = / != matches")
	}
	// PromQL quotes label values; labels.Selector does not. Strip the quotes.
	matchers = stripQuotes(matchers)
	if strings.TrimSpace(matchers) == "" {
		return metricName, labels.Everything(), nil
	}
	selector, err = labels.Parse(matchers)
	if err != nil {
		return "", nil, fmt.Errorf("parse matchers from %q: %w", s, err)
	}
	return metricName, selector, nil
}

func stripQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '"' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
