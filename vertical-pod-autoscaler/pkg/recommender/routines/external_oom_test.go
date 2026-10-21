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

package routines

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_fake "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/fake"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

func TestExternalOOMAnnotationFailureDoesNotBlockStatus(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	source := &vpa_types.VerticalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "vpa", Namespace: "ns", UID: "uid", Annotations: map[string]string{annotations.OOMCounterMetricAnnotation: "oom"}}}
	state := model.NewClusterState(time.Hour)
	require.NoError(t, state.AddOrUpdateVpa(source, labels.Everything()))
	vpa := state.VPAs()[model.VpaID{Namespace: "ns", VpaName: "vpa"}]
	vpa.MergeExternalOOMSignals([]annotations.ExternalOOMSignal{{Metric: "oom", PodName: "pod", PodUID: "pod-uid", ContainerName: "app", MemoryRequest: resource.MustParse("1Gi"), ObservedAt: metav1.NewTime(now)}}, now)
	client := vpa_fake.NewSimpleClientset(source) //nolint:staticcheck // Generated fake mirrors the installed VPA API.
	metadataWrites, statusWrites := 0, 0
	client.PrependReactor("patch", "verticalpodautoscalers", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "" {
			metadataWrites++
			return true, nil, errors.New("metadata patch forbidden")
		}
		statusWrites++
		return false, nil, nil
	})
	r := &recommender{clusterState: state, vpaClient: client.AutoscalingV1(), podResourceRecommender: &mockPodResourceRecommender{}}
	processVPAUpdate(r, vpa, source)
	require.Equal(t, 1, metadataWrites)
	require.Equal(t, 1, statusWrites, "metadata permission failures must not disable ordinary recommendations")
	require.Len(t, vpa.ExternalOOMSignals(), 1, "retain unpublished evidence for the next loop")
}
