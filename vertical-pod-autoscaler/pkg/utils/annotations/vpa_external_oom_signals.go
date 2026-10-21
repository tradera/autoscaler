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
	"cmp"
	"encoding/json"
	"slices"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ExternalOOMSignalsAnnotation is recommender-owned evidence, not configuration.
	ExternalOOMSignalsAnnotation = ExternalMetricsAnnotationPrefix + "oom-signals"
	// MaxExternalOOMSignals bounds retained observations per VPA.
	MaxExternalOOMSignals = 128
	// MaxExternalOOMSignalBytes leaves room for other annotations.
	MaxExternalOOMSignalBytes = 32 * 1024
)

// ExternalOOMSignal is fork-private annotation data, not part of the VPA API.
type ExternalOOMSignal struct {
	Metric             string            `json:"metric"`
	PodName            string            `json:"podName"`
	PodUID             types.UID         `json:"podUID"`
	ContainerName      string            `json:"containerName"`
	ObservedAt         metav1.Time       `json:"observedAt"`
	MemoryRequest      resource.Quantity `json:"memoryRequest"`
	ObservedGeneration int64             `json:"observedGeneration"`
}

// DeepCopy returns independent quantity storage for retained observations.
func (s ExternalOOMSignal) DeepCopy() *ExternalOOMSignal {
	s.MemoryRequest = s.MemoryRequest.DeepCopy()
	return &s
}

type externalOOMEnvelope struct {
	Version int                 `json:"version"`
	Signals []ExternalOOMSignal `json:"signals"`
}

// ParseExternalOOMSignals fails closed for malformed, oversized or unknown data.
func ParseExternalOOMSignals(values map[string]string) []ExternalOOMSignal {
	raw := values[ExternalOOMSignalsAnnotation]
	if len(raw) == 0 || len(raw) > MaxExternalOOMSignalBytes {
		return nil
	}
	var envelope externalOOMEnvelope
	if json.Unmarshal([]byte(raw), &envelope) != nil || envelope.Version != 1 || len(envelope.Signals) > MaxExternalOOMSignals {
		return nil
	}
	return envelope.Signals
}

// EncodeExternalOOMSignals retains newest observations within both the payload
// cap and Kubernetes' total annotation budget, without mutating caller state.
// An empty result means the owned annotation should be removed.
func EncodeExternalOOMSignals(signals []ExternalOOMSignal, existing map[string]string) (string, error) {
	if len(signals) == 0 {
		return "", nil
	}
	budget := 256*1024 - len(ExternalOOMSignalsAnnotation)
	for key, value := range existing {
		if key != ExternalOOMSignalsAnnotation {
			budget -= len(key) + len(value)
		}
	}
	budget = min(budget, MaxExternalOOMSignalBytes)
	ordered := slices.Clone(signals)
	slices.SortFunc(ordered, func(a, b ExternalOOMSignal) int {
		if c := b.ObservedAt.Compare(a.ObservedAt.Time); c != 0 {
			return c
		}
		if c := cmp.Compare(a.PodUID, b.PodUID); c != 0 {
			return c
		}
		return cmp.Compare(a.ContainerName, b.ContainerName)
	})
	ordered = ordered[:min(len(ordered), MaxExternalOOMSignals)]
	const prefix = "{\"version\":1,\"signals\":["
	payload := []byte(prefix)
	for _, signal := range ordered {
		data, err := json.Marshal(signal)
		if err != nil {
			return "", err
		}
		separator := 0
		if len(payload) > len(prefix) {
			separator = 1
		}
		if len(payload)+separator+len(data)+2 > budget {
			continue
		}
		if separator != 0 {
			payload = append(payload, ',')
		}
		payload = append(payload, data...)
	}
	if len(payload) == len(prefix) {
		return "", nil
	}
	payload = append(payload, ']', '}')
	return string(payload), nil
}
