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

package oomsignal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

// The default sink starts requests with context.Background. Bind HTTP writes to
// the owner as well as the broadcaster, so cancellation stops in-flight writes.
type ownedEventSink struct {
	ctx    context.Context
	client typedcorev1.EventInterface
}

func (s *ownedEventSink) Create(event *corev1.Event) (*corev1.Event, error) {
	return s.client.CreateWithEventNamespaceWithContext(s.ctx, event)
}

func (s *ownedEventSink) Update(event *corev1.Event) (*corev1.Event, error) {
	return s.client.UpdateWithEventNamespaceWithContext(s.ctx, event)
}

func (s *ownedEventSink) Patch(event *corev1.Event, data []byte) (*corev1.Event, error) {
	return s.client.PatchWithEventNamespaceWithContext(s.ctx, event, data)
}

// NewRecorder uses client-go's bounded, retrying best-effort delivery. The owner
// must cancel ctx; no independent process-global broadcaster is retained.
func NewRecorder(ctx context.Context, client kubeclient.Interface) record.EventRecorder {
	broadcaster := record.NewBroadcaster(record.WithContext(ctx), record.WithCorrelatorOptions(record.CorrelatorOptions{
		// Default cross-message aggregation rewrites JSON into human summaries.
		// Keep distinct observations separate, while preserving duplicate counting
		// and the standard per-source/object spam limits.
		KeyFunc: func(event *corev1.Event) (string, string) {
			sum := sha256.Sum256([]byte(event.Message))
			key := string(event.InvolvedObject.UID) + "/" + hex.EncodeToString(sum[:])
			return key, key
		},
	}))
	broadcaster.StartRecordingToSink(&ownedEventSink{ctx: ctx, client: client.CoreV1().Events("")})
	return broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: Component})
}

// Record enqueues only an already accepted observation. A dropped Event loses
// expedited eligibility, not the histogram evidence or ordinary recommendation.
func Record(recorder record.EventRecorder, s Signal) error {
	if recorder == nil {
		return nil
	}
	event, err := Event(s)
	if err != nil {
		return err
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: s.PodName, Namespace: s.Namespace, UID: s.PodUID}}
	recorder.Event(pod, event.Type, event.Reason, event.Message)
	return nil
}
