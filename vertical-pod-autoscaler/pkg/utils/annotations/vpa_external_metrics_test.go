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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestExternalMetricForResource(t *testing.T) {
	cases := []struct {
		name        string
		annotations map[string]string
		resource    corev1.ResourceName
		want        string
	}{
		{name: "nil map", annotations: nil, resource: corev1.ResourceCPU, want: ""},
		{name: "empty map", annotations: map[string]string{}, resource: corev1.ResourceCPU, want: ""},
		{
			name:        "cpu set",
			annotations: map[string]string{ExternalCPUMetricAnnotation: "dotnet_cpu_usage"},
			resource:    corev1.ResourceCPU,
			want:        "dotnet_cpu_usage",
		},
		{
			name:        "memory set",
			annotations: map[string]string{ExternalMemoryMetricAnnotation: "dotnet_gc_total_bytes"},
			resource:    corev1.ResourceMemory,
			want:        "dotnet_gc_total_bytes",
		},
		{
			name:        "cpu set, memory queried",
			annotations: map[string]string{ExternalCPUMetricAnnotation: "x"},
			resource:    corev1.ResourceMemory,
			want:        "",
		},
		{
			name:        "unknown resource",
			annotations: map[string]string{ExternalCPUMetricAnnotation: "x"},
			resource:    corev1.ResourceStorage,
			want:        "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExternalMetricForResource(tc.annotations, tc.resource)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHasExternalMetricOverride(t *testing.T) {
	cases := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{name: "nil", annotations: nil, want: false},
		{name: "empty", annotations: map[string]string{}, want: false},
		{name: "unrelated", annotations: map[string]string{"other": "v"}, want: false},
		{name: "cpu only", annotations: map[string]string{ExternalCPUMetricAnnotation: "x"}, want: true},
		{name: "memory only", annotations: map[string]string{ExternalMemoryMetricAnnotation: "y"}, want: true},
		{name: "both", annotations: map[string]string{ExternalCPUMetricAnnotation: "x", ExternalMemoryMetricAnnotation: "y"}, want: true},
		{name: "empty value", annotations: map[string]string{ExternalCPUMetricAnnotation: ""}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasExternalMetricOverride(tc.annotations); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOOMCounterMetric(t *testing.T) {
	if got := OOMCounterMetric(nil); got != "" {
		t.Errorf("nil: got %q, want empty", got)
	}
	if got := OOMCounterMetric(map[string]string{OOMCounterMetricAnnotation: "dotnet_oome_total"}); got != "dotnet_oome_total" {
		t.Errorf("got %q, want dotnet_oome_total", got)
	}
}

func TestHasHistoryQuery(t *testing.T) {
	cases := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{name: "nil", annotations: nil, want: false},
		{name: "empty", annotations: map[string]string{}, want: false},
		{name: "only cpu", annotations: map[string]string{HistoryQueryCPUAnnotation: "x"}, want: true},
		{name: "only memory", annotations: map[string]string{HistoryQueryMemoryAnnotation: "x"}, want: true},
		{name: "both", annotations: map[string]string{HistoryQueryCPUAnnotation: "a", HistoryQueryMemoryAnnotation: "b"}, want: true},
		{name: "empty value", annotations: map[string]string{HistoryQueryCPUAnnotation: ""}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasHistoryQuery(tc.annotations); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseInstantVectorSelector(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		wantMetric  string
		wantMatches map[string]string // labels and the matching value
		wantNoMatch map[string]string // labels and a value that should NOT match
		wantErr     bool
	}{
		{name: "empty", input: "", wantErr: true},
		{name: "bare metric", input: "dotnet_gc_total_bytes", wantMetric: "dotnet_gc_total_bytes"},
		{name: "whitespace", input: "  metric  ", wantMetric: "metric"},
		{
			name:        "single equality matcher",
			input:       `metric{deployment="api"}`,
			wantMetric:  "metric",
			wantMatches: map[string]string{"deployment": "api"},
			wantNoMatch: map[string]string{"deployment": "other"},
		},
		{
			name:        "multiple matchers",
			input:       `metric{deployment="api",env="prod"}`,
			wantMetric:  "metric",
			wantMatches: map[string]string{"deployment": "api", "env": "prod"},
			wantNoMatch: map[string]string{"deployment": "api", "env": "qa"},
		},
		{
			name:        "inequality matcher",
			input:       `metric{tier!="frontend"}`,
			wantMetric:  "metric",
			wantMatches: map[string]string{"tier": "backend"},
			wantNoMatch: map[string]string{"tier": "frontend"},
		},
		{name: "regex match rejected", input: `metric{deployment=~"api.*"}`, wantErr: true},
		{name: "regex non-match rejected", input: `metric{deployment!~"api.*"}`, wantErr: true},
		{name: "missing closing brace", input: `metric{deployment="api"`, wantErr: true},
		{name: "empty metric name", input: `{deployment="api"}`, wantErr: true},
		{name: "empty matcher block", input: `metric{}`, wantMetric: "metric"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotMetric, sel, err := ParseInstantVectorSelector(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil; metric=%q sel=%v", gotMetric, sel)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotMetric != tc.wantMetric {
				t.Errorf("metric: got %q, want %q", gotMetric, tc.wantMetric)
			}
			if sel == nil {
				t.Fatal("selector is nil")
			}
			if tc.wantMatches != nil && !sel.Matches(labels.Set(tc.wantMatches)) {
				t.Errorf("selector %q should match %v", sel.String(), tc.wantMatches)
			}
			if tc.wantNoMatch != nil && sel.Matches(labels.Set(tc.wantNoMatch)) {
				t.Errorf("selector %q should NOT match %v", sel.String(), tc.wantNoMatch)
			}
		})
	}
}

func TestHistoryQueryForResource(t *testing.T) {
	ann := map[string]string{
		HistoryQueryCPUAnnotation:    "rate(cpu[5m])",
		HistoryQueryMemoryAnnotation: "max_over_time(mem[1h])",
	}
	if got := HistoryQueryForResource(ann, corev1.ResourceCPU); got != "rate(cpu[5m])" {
		t.Errorf("cpu: got %q", got)
	}
	if got := HistoryQueryForResource(ann, corev1.ResourceMemory); got != "max_over_time(mem[1h])" {
		t.Errorf("memory: got %q", got)
	}
	if got := HistoryQueryForResource(nil, corev1.ResourceCPU); got != "" {
		t.Errorf("nil: got %q", got)
	}
	if got := HistoryQueryForResource(ann, corev1.ResourceStorage); got != "" {
		t.Errorf("storage: got %q", got)
	}
}
