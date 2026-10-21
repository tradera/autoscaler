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

package logic

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/oomsignal"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

const (
	externalOOMReadTimeout      = 2 * time.Second
	maxExternalOOMEventsPerPass = 10000
)

// readExternalOOMEvents is an optional, bounded read on the ordinary updater
// pass. It adds no informer, persistent cache or startup synchronization barrier.
// A read failure sacrifices only expedited eligibility, never ordinary updates.
func (u *updater) readExternalOOMEvents(ctx context.Context, vpas []*vpa_api_util.VpaWithSelector) map[types.UID][]oomsignal.Signal {
	if u.externalOOMClient == nil {
		return nil
	}
	active := make(map[types.UID]*vpa_api_util.VpaWithSelector)
	for _, vpa := range vpas {
		mode := vpa_api_util.GetUpdateMode(vpa.Vpa)
		if vpa.Vpa.UID != "" && annotations.OOMCounterMetric(vpa.Vpa.Annotations) != "" && (mode == vpa_types.UpdateModeInPlace || mode == vpa_types.UpdateModeInPlaceOrRecreate) {
			active[vpa.Vpa.UID] = vpa
		}
	}
	if len(active) == 0 {
		return nil
	}
	budget := externalOOMReadTimeout
	if deadline, ok := ctx.Deadline(); ok {
		// Leave time for ordinary work even with short updater intervals.
		budget = min(budget, time.Until(deadline)/4)
	}
	if budget <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	options := metav1.ListOptions{FieldSelector: "reason=" + oomsignal.Reason, Limit: 500}
	type key struct {
		pod       types.UID
		container string
	}
	latest := make(map[types.UID]map[key]oomsignal.Signal)
	examined := 0
	now := time.Now()
	for {
		page, err := u.externalOOMClient.CoreV1().Events(u.externalOOMNamespace).List(ctx, options)
		if err != nil {
			klog.ErrorS(err, "Cannot read external OOM Events; using ordinary updater rules")
			return nil
		}
		for i := range page.Items {
			if ctx.Err() != nil {
				return nil
			}
			if examined >= maxExternalOOMEventsPerPass {
				break
			}
			examined++
			signal, ok := oomsignal.Decode(&page.Items[i], now)
			if !ok {
				continue
			}
			source := active[signal.VPAUID]
			if source == nil || signal.Namespace != source.Vpa.Namespace || signal.VPAName != source.Vpa.Name || signal.ObservedGeneration != source.Vpa.Generation ||
				signal.MetricFingerprint != oomsignal.Fingerprint(annotations.OOMCounterMetric(source.Vpa.Annotations)) {
				continue
			}
			bucket := latest[signal.VPAUID]
			if bucket == nil {
				bucket = make(map[key]oomsignal.Signal)
				latest[signal.VPAUID] = bucket
			}
			id := key{signal.PodUID, signal.ContainerName}
			if previous, found := bucket[id]; found && !signal.ObservedAt.After(previous.ObservedAt.Time) {
				continue
			}
			bucket[id] = signal
			if len(bucket) > oomsignal.MaxSignalsPerVPA {
				oldest := id
				for candidate, value := range bucket {
					prior := bucket[oldest]
					if value.ObservedAt.Before(&prior.ObservedAt) || (value.ObservedAt.Equal(&prior.ObservedAt) && (candidate.pod < oldest.pod || (candidate.pod == oldest.pod && candidate.container < oldest.container))) {
						oldest = candidate
					}
				}
				delete(bucket, oldest)
			}
		}
		if page.Continue == "" || examined >= maxExternalOOMEventsPerPass {
			break
		}
		options.Continue = page.Continue
	}
	result := make(map[types.UID][]oomsignal.Signal, len(latest))
	for uid, bucket := range latest {
		for _, signal := range bucket {
			result[uid] = append(result[uid], signal)
		}
	}
	return result
}
