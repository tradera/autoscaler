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

package model

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func TestExternalOOMSignalLifecycle(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	source := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "vpa", Namespace: "ns", Generation: 1, Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}}}
	cluster := NewClusterState(time.Hour)
	require.NoError(t, cluster.AddOrUpdateVpa(source, labels.Everything()))
	v := cluster.VPAs()[VpaID{Namespace: "ns", VpaName: "vpa"}]
	signal := vpa_types.ExternalOOMSignal{Metric: "app_oom_total", PodName: "worker", PodUID: "uid", ContainerName: "app", ObservedAt: metav1.NewTime(now.Add(-time.Minute)), MemoryRequest: resource.MustParse("1Gi"), ObservedGeneration: 1}
	v.MergeExternalOOMSignals([]vpa_types.ExternalOOMSignal{signal, signal}, now)
	require.Len(t, v.AsStatus().ExternalOOMSignals, 1)
	require.Equal(t, signal.ObservedAt, v.AsStatus().ExternalOOMSignals[0].ObservedAt)
	// Status is an independent copy, not an alias of retained model state.
	exported := v.AsStatus()
	exported.ExternalOOMSignals[0].MemoryRequest.Set(1)
	require.Equal(t, "1Gi", v.AsStatus().ExternalOOMSignals[0].MemoryRequest.String())
	// Restore only through the API status as a new recommender would.
	source.Status = *v.AsStatus()
	restarted := NewClusterState(time.Hour)
	require.NoError(t, restarted.AddOrUpdateVpa(source, labels.Everything()))
	restored := restarted.VPAs()[v.ID]
	require.Len(t, restored.AsStatus().ExternalOOMSignals, 1)
	restored.MergeExternalOOMSignals(nil, now.Add(11*time.Minute))
	require.Empty(t, restored.AsStatus().ExternalOOMSignals)
	source.Generation++
	require.NoError(t, cluster.AddOrUpdateVpa(source, labels.Everything()))
	require.Empty(t, v.AsStatus().ExternalOOMSignals)
}

func TestExternalOOMSignalsBoundedDeterministicAndOptOut(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	v := NewVpa(VpaID{}, labels.Everything(), now)
	v.Annotations = map[string]string{annotations.OOMCounterMetricAnnotation: "app_oom_total"}
	var signals []vpa_types.ExternalOOMSignal
	for i := range 1200 {
		signals = append(signals, vpa_types.ExternalOOMSignal{Metric: "app_oom_total", PodName: "worker", PodUID: types.UID(fmt.Sprint(i)), ContainerName: "app", ObservedAt: metav1.NewTime(now.Add(-time.Duration(i) * time.Millisecond)), MemoryRequest: resource.MustParse("1Gi")})
	}
	v.MergeExternalOOMSignals(signals, now)
	first := v.AsStatus().ExternalOOMSignals
	require.Len(t, first, vpa_api_util.MaxExternalOOMSignals)
	v.MergeExternalOOMSignals(signals, now)
	require.Equal(t, first, v.AsStatus().ExternalOOMSignals)
	v.Annotations = nil
	v.MergeExternalOOMSignals(signals, now)
	require.Empty(t, v.AsStatus().ExternalOOMSignals)
}
