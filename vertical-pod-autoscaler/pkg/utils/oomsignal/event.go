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

// Package oomsignal defines the fork-private, advisory external-OOM Event contract.
package oomsignal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Event protocol identifiers and retention bounds.
const (
	Reason           = "ExternalOOMObserved"
	Component        = "vpa-external-oom-observer"
	Lifetime         = 10 * time.Minute
	MaxSignalsPerVPA = 128
	MaxMessageBytes  = 1024
)

// Signal is accepted sizing evidence, not a recommendation or update authority.
// Pod identity is carried by the Event's involvedObject, not its message.
type Signal struct {
	Namespace          string            `json:"-"`
	PodName            string            `json:"-"`
	PodUID             types.UID         `json:"-"`
	VPAName            string            `json:"vpaName"`
	VPAUID             types.UID         `json:"vpaUID"`
	ContainerName      string            `json:"containerName"`
	MetricFingerprint  string            `json:"metricFingerprint"`
	ObservedGeneration int64             `json:"observedGeneration"`
	ObservedAt         metav1.Time       `json:"observedAt"`
	MemoryRequest      resource.Quantity `json:"memoryRequest"`
}

type payload struct {
	Version int `json:"version"`
	Signal
}

// Fingerprint binds evidence to the configured selector without copying long
// selectors into Events or exposing them to additional Event readers.
func Fingerprint(selector string) string {
	sum := sha256.Sum256([]byte(selector))
	return hex.EncodeToString(sum[:])
}

// Valid rejects incomplete, future and expired evidence.
func (s Signal) Valid(now time.Time) bool {
	_, err := hex.DecodeString(s.MetricFingerprint)
	return s.Namespace != "" && s.PodName != "" && s.PodUID != "" && s.VPAName != "" && s.VPAUID != "" &&
		s.ContainerName != "" && len(s.MetricFingerprint) == 64 && err == nil &&
		s.ObservedGeneration >= 0 && s.MemoryRequest.Sign() > 0 && !s.ObservedAt.IsZero() &&
		!s.ObservedAt.After(now) && now.Sub(s.ObservedAt.Time) < Lifetime
}

// Event builds the exact protocol object used by tests and the recorder.
func Event(s Signal) (*corev1.Event, error) {
	if !s.Valid(time.Now()) {
		return nil, errors.New("invalid external OOM observation")
	}
	message, err := json.Marshal(payload{Version: 1, Signal: s})
	if err != nil {
		return nil, err
	}
	if len(message) > MaxMessageBytes {
		return nil, fmt.Errorf("external OOM Event message exceeds %d bytes", MaxMessageBytes)
	}
	id := sha256.Sum256(append([]byte(s.Namespace+"/"+string(s.PodUID)+"/"), message...))
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "external-oom-" + hex.EncodeToString(id[:16]), Namespace: s.Namespace},
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: s.Namespace, Name: s.PodName, UID: s.PodUID},
		Type:           corev1.EventTypeWarning, Reason: Reason, Source: corev1.EventSource{Component: Component},
		Message: string(message), FirstTimestamp: s.ObservedAt, LastTimestamp: s.ObservedAt, Count: 1,
	}, nil
}

// Decode ignores unrelated and unknown Events. Never derive identity or age
// from free text, Event counts, or lastTimestamp (which retries can refresh).
func Decode(event *corev1.Event, now time.Time) (Signal, bool) {
	if event == nil || event.Reason != Reason || event.Source.Component != Component || event.Type != corev1.EventTypeWarning ||
		event.InvolvedObject.APIVersion != "v1" || event.InvolvedObject.Kind != "Pod" ||
		event.Namespace != event.InvolvedObject.Namespace || len(event.Message) > MaxMessageBytes {
		return Signal{}, false
	}
	var p payload
	if json.Unmarshal([]byte(event.Message), &p) != nil || p.Version != 1 {
		return Signal{}, false
	}
	s := p.Signal
	s.Namespace = event.Namespace
	s.PodName = event.InvolvedObject.Name
	s.PodUID = event.InvolvedObject.UID
	return s, s.Valid(now)
}
