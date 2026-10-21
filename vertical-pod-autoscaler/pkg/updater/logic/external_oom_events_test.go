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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/oomsignal"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func eventReaderFixture(t *testing.T) (*vpa_types.VerticalPodAutoscaler, *corev1.Event) {
	t.Helper()
	mode := vpa_types.UpdateModeInPlaceOrRecreate
	vpa := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "vpa", Namespace: "ns", UID: "vpa-uid", Generation: 3, Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "oom_total"}}, Spec: vpa_types.VerticalPodAutoscalerSpec{UpdatePolicy: &vpa_types.PodUpdatePolicy{UpdateMode: &mode}}}
	event, err := oomsignal.Event(oomsignal.Signal{Namespace: "ns", VPAName: vpa.Name, VPAUID: vpa.UID, PodName: "worker", PodUID: "pod-uid", ContainerName: "app", MetricFingerprint: oomsignal.Fingerprint("oom_total"), ObservedAt: metav1.NewTime(time.Now().UTC().Truncate(time.Second).Add(-time.Second)), ObservedGeneration: 3, MemoryRequest: resource.MustParse("1Gi")})
	require.NoError(t, err)
	return vpa, event
}

func TestExternalOOMEventReaderRelistDedupAndIdentity(t *testing.T) {
	vpa, event := eventReaderFixture(t)
	duplicate := event.DeepCopy()
	duplicate.Name = "duplicate"
	duplicate.Count = 17
	wrongSource := event.DeepCopy()
	wrongSource.Name = "wrong"
	wrongSource.Source.Component = "other"
	client := fake.NewClientset(event, duplicate, wrongSource)
	read := func() []oomsignal.Signal {
		// A fresh updater has no cursors/state, and recovers only recent evidence.
		updater := &updater{externalOOMClient: client}
		return updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: vpa}})[vpa.UID]
	}
	first := read()
	require.Len(t, first, 1)
	require.ElementsMatch(t, first, read(), "relist/restart must not invent or refresh observations")
	changed := vpa.DeepCopy()
	changed.UID = "replacement"
	updater := &updater{externalOOMClient: client}
	require.Empty(t, updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: changed}}))
	changed = vpa.DeepCopy()
	changed.Generation++
	require.Empty(t, updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: changed}}))
	changed = vpa.DeepCopy()
	changed.Annotations[annotations.OOMCounterMetricAnnotation] = "other_selector"
	require.Empty(t, updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: changed}}))
}

func TestExternalOOMEventReaderPaginationAndFailures(t *testing.T) {
	vpa, event := eventReaderFixture(t)
	client := fake.NewClientset()
	calls := 0
	client.PrependReactor("list", "events", func(action clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, &corev1.EventList{ListMeta: metav1.ListMeta{Continue: "next"}, Items: []corev1.Event{*event}}, nil
		}
		duplicate := event.DeepCopy()
		duplicate.Count = 20
		return true, &corev1.EventList{Items: []corev1.Event{*duplicate}}, nil
	})
	updater := &updater{externalOOMClient: client}
	result := updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: vpa}})
	require.Equal(t, 2, calls)
	require.Len(t, result[vpa.UID], 1)
	client.PrependReactor("list", "events", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("Events forbidden")
	})
	require.Empty(t, updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: vpa}}))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.Empty(t, updater.readExternalOOMEvents(canceled, []*vpa_api_util.VpaWithSelector{{Vpa: vpa}}))
	client.ClearActions()
	vpa.Annotations = nil
	require.Empty(t, updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: vpa}}))
	require.Empty(t, client.Actions(), "non-opted-in VPAs must not require Event reads")
}

func TestSlowEventReadsLeaveOrdinaryReconciliationBudget(t *testing.T) {
	vpa, _ := eventReaderFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	updater := &updater{externalOOMClient: client}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.Empty(t, updater.readExternalOOMEvents(ctx, []*vpa_api_util.VpaWithSelector{{Vpa: vpa}}))
	require.NoError(t, ctx.Err(), "a slow optional Event endpoint must not use the entire ordinary-work deadline")
}

func TestExternalOOMEventReaderBounds(t *testing.T) {
	vpa, event := eventReaderFixture(t)
	var events []runtime.Object
	for i := range oomsignal.MaxSignalsPerVPA + 20 {
		eventCopy := event.DeepCopy()
		eventCopy.Name = fmt.Sprint("event-", i)
		eventCopy.InvolvedObject.UID = types.UID(fmt.Sprint(i))
		events = append(events, eventCopy)
	}
	client := fake.NewClientset(events...)
	updater := &updater{externalOOMClient: client}
	result := updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: vpa}})
	require.Len(t, result[vpa.UID], oomsignal.MaxSignalsPerVPA)
	// A server that keeps returning continuation tokens must not monopolize a
	// reconciliation pass. Duplicate pages must not grow the retained state.
	calls := 0
	client.PrependReactor("list", "events", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		page := &corev1.EventList{ListMeta: metav1.ListMeta{Continue: "more"}}
		for range 500 {
			page.Items = append(page.Items, *event)
		}
		return true, page, nil
	})
	result = updater.readExternalOOMEvents(t.Context(), []*vpa_api_util.VpaWithSelector{{Vpa: vpa}})
	require.LessOrEqual(t, calls, maxExternalOOMEventsPerPass/500+1)
	require.Len(t, result[vpa.UID], 1)
}
