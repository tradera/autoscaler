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

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_fake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

func TestExternalOOMMetadataWriteAndConcurrency(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*vpa_types.VerticalPodAutoscaler)
		reject bool
	}{
		{"unchanged", func(*vpa_types.VerticalPodAutoscaler) {}, false},
		{"replacement", func(v *vpa_types.VerticalPodAutoscaler) { v.UID = "replacement" }, true},
		{"concurrent config edit", func(v *vpa_types.VerticalPodAutoscaler) { v.ResourceVersion = "2"; v.Annotations["other"] = "new" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "vpa", Namespace: "ns", UID: "vpa-uid", ResourceVersion: "1", Annotations: map[string]string{"other": "keep"}}}
			actual := source.DeepCopy()
			tc.change(actual)
			client := vpa_fake.NewSimpleClientset(actual) //nolint:staticcheck // Generated fake mirrors the installed VPA API.
			signals := []annotations.ExternalOOMSignal{{Metric: "oom", PodName: "pod", PodUID: "pod-uid", ContainerName: "app", MemoryRequest: resource.MustParse("1Gi"), ObservedAt: metav1.NewTime(time.Now())}}
			result, err := UpdateExternalOOMSignalsIfNeeded(client.AutoscalingV1().VerticalPodAutoscalers("ns"), source, signals)
			if tc.reject {
				require.Error(t, err)
				result, err = client.AutoscalingV1().VerticalPodAutoscalers("ns").Get(t.Context(), "vpa", metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, actual.Annotations, result.Annotations)
				return
			}
			require.NoError(t, err)
			require.Equal(t, source.Spec, result.Spec)
			require.Equal(t, source.Status, result.Status, "signal transport must not depend on any custom status fields")
			require.Equal(t, "keep", result.Annotations["other"])
			require.Len(t, annotations.ParseExternalOOMSignals(result.Annotations), 1)
			client.ClearActions()
			unchanged, err := UpdateExternalOOMSignalsIfNeeded(client.AutoscalingV1().VerticalPodAutoscalers("ns"), result, signals)
			require.NoError(t, err)
			require.Nil(t, unchanged)
			require.Empty(t, client.Actions(), "unchanged evidence must not write again")
			removed, err := UpdateExternalOOMSignalsIfNeeded(client.AutoscalingV1().VerticalPodAutoscalers("ns"), result, nil)
			require.NoError(t, err)
			require.NotContains(t, removed.Annotations, annotations.ExternalOOMSignalsAnnotation)
			require.Equal(t, "keep", removed.Annotations["other"])
		})
	}
}
