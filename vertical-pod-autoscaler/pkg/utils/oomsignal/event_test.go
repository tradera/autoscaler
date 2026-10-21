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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func fixtureSignal(now time.Time) Signal {
	return Signal{Namespace: "ns", PodName: "worker", PodUID: "pod-uid", VPAName: "vpa", VPAUID: "vpa-uid", ContainerName: "app", MetricFingerprint: Fingerprint("oom_total"), ObservedGeneration: 3, ObservedAt: metav1.NewTime(now.Add(-time.Second)), MemoryRequest: resource.MustParse("1Gi")}
}

func TestEventProtocolRoundTripAndFiltering(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	signal := fixtureSignal(now)
	event, err := Event(signal)
	require.NoError(t, err)
	decoded, ok := Decode(event, now)
	require.True(t, ok)
	require.Equal(t, signal.PodUID, decoded.PodUID)
	require.Equal(t, signal.VPAUID, decoded.VPAUID)
	require.Equal(t, "1Gi", decoded.MemoryRequest.String())
	require.WithinDuration(t, signal.ObservedAt.Time, decoded.ObservedAt.Time, time.Nanosecond)
	for _, tc := range []struct {
		name   string
		change func(*corev1.Event)
	}{
		{"native OOM reason", func(e *corev1.Event) { e.Reason = "OOMKilled" }},
		{"unrecognized reporter", func(e *corev1.Event) { e.Source.Component = "another-controller" }},
		{"wrong object kind", func(e *corev1.Event) { e.InvolvedObject.Kind = "Node" }},
		{"wrong namespace", func(e *corev1.Event) { e.InvolvedObject.Namespace = "other" }},
		{"missing pod UID", func(e *corev1.Event) { e.InvolvedObject.UID = "" }},
		{"malformed message", func(e *corev1.Event) { e.Message = "ordinary human message" }},
		{"unknown version", func(e *corev1.Event) { e.Message = strings.Replace(e.Message, `"version":1`, `"version":2`, 1) }},
		{"oversized message", func(e *corev1.Event) { e.Message = strings.Repeat("x", MaxMessageBytes+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventCopy := event.DeepCopy()
			tc.change(eventCopy)
			_, ok := Decode(eventCopy, now)
			require.False(t, ok)
		})
	}
	event.Count = 999
	event.LastTimestamp = metav1.NewTime(now.Add(Lifetime))
	_, ok = Decode(event, now.Add(Lifetime))
	require.False(t, ok, "Event count/retry timestamps cannot refresh the original observation")
	future := signal
	future.ObservedAt = metav1.NewTime(now.Add(time.Minute))
	data, err := json.Marshal(payload{Version: 1, Signal: future})
	require.NoError(t, err)
	event.Message = string(data)
	_, ok = Decode(event, now)
	require.False(t, ok)
	signal.VPAName = strings.Repeat("x", MaxMessageBytes)
	_, err = Event(signal)
	require.Error(t, err)
}

func TestRecorderPreservesStructuredObservationsInsteadOfAggregatingMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := fake.NewClientset()
	recorder := NewRecorder(ctx, client)
	now := time.Now().UTC().Truncate(time.Second)
	// The default client-go aggregator rewrites distinct messages after ten
	// similar Events. Every observation here must remain a decodable Event.
	for i := range 15 {
		signal := fixtureSignal(now)
		signal.ObservedAt = metav1.NewTime(now.Add(-time.Duration(i+1) * time.Second))
		require.NoError(t, Record(recorder, signal))
	}
	require.Eventually(t, func() bool {
		events, err := client.CoreV1().Events("ns").List(ctx, metav1.ListOptions{})
		if err != nil || len(events.Items) != 15 {
			return false
		}
		for i := range events.Items {
			if _, ok := Decode(&events.Items[i], time.Now()); !ok {
				return false
			}
		}
		return true
	}, 2*time.Second, time.Millisecond)
}

func TestRecorderCancellationStopsInFlightPublication(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	recorder := NewRecorder(ctx, client)
	require.NoError(t, Record(recorder, fixtureSignal(time.Now().UTC().Truncate(time.Second))))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Event publication did not start")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Event publication ignored owner cancellation")
	}
}

func TestRecorderCountsExactDuplicatesWithoutRefreshingEvidence(t *testing.T) {
	client := fake.NewClientset()
	recorder := NewRecorder(t.Context(), client)
	signal := fixtureSignal(time.Now().UTC().Truncate(time.Second))
	require.NoError(t, Record(recorder, signal))
	require.NoError(t, Record(recorder, signal))
	require.Eventually(t, func() bool {
		events, err := client.CoreV1().Events("ns").List(t.Context(), metav1.ListOptions{})
		return err == nil && len(events.Items) == 1 && events.Items[0].Count == 2
	}, 2*time.Second, time.Millisecond)
}
