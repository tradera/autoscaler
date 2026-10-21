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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/utils"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
)

// Unlike the generic restriction mock, inspect the per-action VPA passed to
// execution. Admission alone cannot prove that CPU downsizes are excluded.
type checkingExternalOOMRestriction struct {
	test.PodsInPlaceRestrictionMock
	check func(*vpa_types.VerticalPodAutoscaler)
}

func (r *checkingExternalOOMRestriction) InPlaceUpdate(pod *corev1.Pod, vpa *vpa_types.VerticalPodAutoscaler, recorder record.EventRecorder) error {
	if r.check != nil {
		r.check(vpa)
	}
	return r.PodsInPlaceRestrictionMock.InPlaceUpdate(pod, vpa, recorder)
}

func TestRunOnceExternalOOMDoesNotExpediteEviction(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision utils.InPlaceDecision
		fail     bool
		attempts int
	}{
		{"resize", utils.InPlaceApproved, false, 5},
		{"resize error", utils.InPlaceApproved, true, 5},
		{"eviction requested", utils.InPlaceEvict, false, 0},
		{"deferred", utils.InPlaceDeferred, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testRunOnceBase(t, vpa_types.UpdateModeInPlaceOrRecreate, tc.fail, newFakeValidator(true), true, 0, tc.attempts, tc.decision, false, true)
		})
	}
}
