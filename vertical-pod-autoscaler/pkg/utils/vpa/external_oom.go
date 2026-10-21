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
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	resourcehelpers "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/resources"
)

const (
	externalOOMActionUIDAnnotation = "external.vpa.k8s.io/quick-update-pod-uid"
	// ExternalOOMSignalLifetime bounds expedited eligibility, independently of
	// the native quick-OOM startup threshold.
	ExternalOOMSignalLifetime = 10 * time.Minute
	// MaxExternalOOMSignals bounds persisted and in-memory signal state per VPA.
	MaxExternalOOMSignals = annotations.MaxExternalOOMSignals
)

// ValidExternalOOMSignal rejects incomplete, future or expired observations.
func ValidExternalOOMSignal(signal annotations.ExternalOOMSignal, generation int64, now time.Time) bool {
	return signal.Metric != "" && len(signal.Metric) <= 1024 && signal.PodName != "" && signal.PodUID != "" && signal.ContainerName != "" &&
		signal.MemoryRequest.Sign() > 0 && signal.ObservedGeneration == generation &&
		!signal.ObservedAt.IsZero() && !signal.ObservedAt.After(now) &&
		now.Sub(signal.ObservedAt.Time) < ExternalOOMSignalLifetime
}

// PrepareExternalOOMUpdate builds a per-action VPA, preserving policy while
// freezing all resources except increasing memory for signaled containers.
// The caller must carry this snapshot through admission and resize execution;
// re-reading the ordinary cohort recommendation would allow unrelated downsizes.
func PrepareExternalOOMUpdate(vpa *vpa_types.VerticalPodAutoscaler, pod *corev1.Pod, processed *vpa_types.RecommendedPodResources, now time.Time) *vpa_types.VerticalPodAutoscaler {
	if vpa == nil || pod == nil || processed == nil || pod.UID == "" ||
		pod.Namespace != vpa.Namespace || annotations.OOMCounterMetric(vpa.Annotations) == "" || PodHasCPUBoostInProgressAnnotation(pod) {
		return nil
	}
	mode := GetUpdateMode(vpa)
	if mode != vpa_types.UpdateModeInPlace && mode != vpa_types.UpdateModeInPlaceOrRecreate {
		return nil
	}
	signaled := make(map[string]bool)
	for _, signal := range annotations.ParseExternalOOMSignals(vpa.Annotations) {
		if signal.Metric != annotations.OOMCounterMetric(vpa.Annotations) || !ValidExternalOOMSignal(signal, vpa.Generation, now) || signal.PodUID != pod.UID || signal.PodName != pod.Name {
			continue
		}
		container := getContainer(signal.ContainerName, pod)
		if container == nil {
			continue
		}
		requests, _ := resourcehelpers.ContainerRequestsAndLimits(signal.ContainerName, pod)
		current := requests[corev1.ResourceMemory]
		desired := container.Resources.Requests[corev1.ResourceMemory]
		// A submitted increase consumes the signal before kubelet status catches
		// up. Checking only applied resources could expedite another increase.
		if current.Sign() > 0 && current.Cmp(signal.MemoryRequest) <= 0 && desired.Cmp(signal.MemoryRequest) <= 0 {
			signaled[signal.ContainerName] = true
		}
	}
	if len(signaled) == 0 {
		return nil
	}
	observed, observedContainers := parseExternalOOMObservedContainers(pod)
	action := vpa.DeepCopy()
	// This marker is local to the action snapshot; it is never written to the VPA.
	action.Annotations[externalOOMActionUIDAnnotation] = string(pod.UID)
	action.Status.Recommendation = &vpa_types.RecommendedPodResources{}
	action.Spec.ResourcePolicy = &vpa_types.PodResourcePolicy{}
	memory := []corev1.ResourceName{corev1.ResourceMemory}
	for _, container := range pod.Spec.Containers {
		policy := vpa_types.ContainerResourcePolicy{ContainerName: container.Name}
		if original := GetContainerResourcePolicy(container.Name, vpa.Spec.ResourcePolicy); original != nil {
			policy = *original.DeepCopy()
			policy.ContainerName = container.Name
		}
		allowed := policy.Mode == nil || *policy.Mode != vpa_types.ContainerScalingModeOff
		controlled := policy.ControlledResources == nil || slices.Contains(*policy.ControlledResources, corev1.ResourceMemory)
		rec := GetRecommendationForContainer(container.Name, processed)
		requests, _ := resourcehelpers.ContainerRequestsAndLimits(container.Name, pod)
		current := requests[corev1.ResourceMemory]
		if signaled[container.Name] && allowed && controlled && (!observed || observedContainers[container.Name]) && rec != nil && rec.Target.Memory().Cmp(current) > 0 {
			copyRec := rec.DeepCopy()
			copyRec.Target = corev1.ResourceList{corev1.ResourceMemory: rec.Target.Memory().DeepCopy()}
			// Bounds are not update targets. Dropping unrelated resources here keeps
			// every later processor on the memory-only contract.
			copyRec.LowerBound = nil
			copyRec.UpperBound = nil
			copyRec.UncappedTarget = nil
			action.Status.Recommendation.ContainerRecommendations = append(action.Status.Recommendation.ContainerRecommendations, *copyRec)
			policy.ControlledResources = &memory
		} else {
			off := vpa_types.ContainerScalingModeOff
			policy.Mode = &off
		}
		action.Spec.ResourcePolicy.ContainerPolicies = append(action.Spec.ResourcePolicy.ContainerPolicies, policy)
	}
	if len(action.Status.Recommendation.ContainerRecommendations) == 0 {
		return nil
	}
	return action
}

// ExternalOOMUpdatePodUID identifies a scoped, in-memory resize action.
// The resize writer uses it to enforce pod identity and resource preconditions.
func ExternalOOMUpdatePodUID(vpa *vpa_types.VerticalPodAutoscaler) string {
	if vpa == nil {
		return ""
	}
	return vpa.Annotations[externalOOMActionUIDAnnotation]
}
func parseExternalOOMObservedContainers(pod *corev1.Pod) (bool, map[string]bool) {
	raw, present := pod.Annotations[annotations.VpaObservedContainersLabel]
	if !present {
		return false, nil
	}
	names, err := annotations.ParseVpaObservedContainersValue(raw)
	if err != nil {
		return true, nil
	} // malformed identity metadata fails closed
	result := make(map[string]bool, len(names))
	for _, name := range names {
		result[name] = true
	}
	return true, result
}
