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
	"cmp"
	"slices"
	"time"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

// MergeExternalOOMSignals retains accepted observations across annotation writes and
// recommender restarts. Identical observations never refresh their timestamp.
func (vpa *Vpa) MergeExternalOOMSignals(signals []annotations.ExternalOOMSignal, now time.Time) {
	vpa.mutex.Lock()
	defer vpa.mutex.Unlock()
	if annotations.OOMCounterMetric(vpa.Annotations) == "" {
		vpa.externalOOMSignals = nil
		return
	}
	type key struct{ uid, container string }
	latest := make(map[key]annotations.ExternalOOMSignal)
	add := func(signal annotations.ExternalOOMSignal) {
		if signal.Metric != annotations.OOMCounterMetric(vpa.Annotations) || !vpa_api_util.ValidExternalOOMSignal(signal, vpa.Generation, now) {
			return
		}
		k := key{string(signal.PodUID), signal.ContainerName}
		previous, found := latest[k]
		if !found || signal.ObservedAt.After(previous.ObservedAt.Time) {
			latest[k] = *signal.DeepCopy()
		}
	}
	for _, signal := range vpa.externalOOMSignals {
		add(signal)
	}
	for _, signal := range signals {
		add(signal)
	}
	vpa.externalOOMSignals = nil
	for _, signal := range latest {
		vpa.externalOOMSignals = append(vpa.externalOOMSignals, signal)
	}
	// Cap by recency before sorting for deterministic status serialization.
	slices.SortFunc(vpa.externalOOMSignals, func(a, b annotations.ExternalOOMSignal) int {
		if c := b.ObservedAt.Compare(a.ObservedAt.Time); c != 0 {
			return c
		}
		if c := cmp.Compare(a.PodUID, b.PodUID); c != 0 {
			return c
		}
		return cmp.Compare(a.ContainerName, b.ContainerName)
	})
	if len(vpa.externalOOMSignals) > vpa_api_util.MaxExternalOOMSignals {
		vpa.externalOOMSignals = vpa.externalOOMSignals[:vpa_api_util.MaxExternalOOMSignals]
	}
	slices.SortFunc(vpa.externalOOMSignals, func(a, b annotations.ExternalOOMSignal) int {
		if c := cmp.Compare(a.PodUID, b.PodUID); c != 0 {
			return c
		}
		return cmp.Compare(a.ContainerName, b.ContainerName)
	})
}

// ExternalOOMSignals returns independent annotation evidence, never VPA status.
func (vpa *Vpa) ExternalOOMSignals() []annotations.ExternalOOMSignal {
	vpa.mutex.RLock()
	defer vpa.mutex.RUnlock()
	return vpa.externalOOMSignalsLocked(time.Now())
}
func (vpa *Vpa) externalOOMSignalsLocked(now time.Time) []annotations.ExternalOOMSignal {
	var result []annotations.ExternalOOMSignal
	for _, signal := range vpa.externalOOMSignals {
		if signal.Metric == annotations.OOMCounterMetric(vpa.Annotations) && vpa_api_util.ValidExternalOOMSignal(signal, vpa.Generation, now) {
			result = append(result, *signal.DeepCopy())
		}
	}
	return result
}
