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

package input

import (
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/oomsignal"
)

func (feeder *clusterStateFeeder) recordOOM(info oom.OomInfo) error {
	if info.PodUID == "" {
		return feeder.clusterState.RecordOOM(info.ContainerID, info.Timestamp, info.Memory)
	}
	vpa, found := feeder.clusterState.VPAs()[info.VpaID]
	if !found || info.VpaUID == "" || vpa.UID != info.VpaUID || vpa.Generation != info.VpaGeneration || annotations.OOMCounterMetric(vpa.Annotations) == "" || info.Metric != annotations.OOMCounterMetric(vpa.Annotations) || feeder.podLister == nil {
		return errors.New("external OOM source is no longer active")
	}
	pod, err := feeder.podLister.Pods(info.ContainerID.Namespace).Get(info.ContainerID.PodName)
	if err != nil {
		return err
	}
	if pod.UID != info.PodUID || pod.Namespace != vpa.ID.Namespace || !vpa.PodSelector.Matches(labels.Set(pod.Labels)) {
		return errors.New("external OOM pod identity or ownership changed")
	}
	signal := oomsignal.Signal{
		Namespace: vpa.ID.Namespace, VPAName: vpa.ID.VpaName, VPAUID: vpa.UID,
		MetricFingerprint: oomsignal.Fingerprint(info.Metric),
		PodName:           pod.Name, PodUID: pod.UID, ContainerName: info.ContainerID.ContainerName,
		ObservedAt: metav1.NewTime(info.Timestamp), MemoryRequest: *resource.NewQuantity(int64(info.Memory), resource.BinarySI),
		ObservedGeneration: info.VpaGeneration,
	}
	now := time.Now()
	if info.Timestamp.After(now) || info.Timestamp.Before(pod.CreationTimestamp.Time) {
		return errors.New("external OOM observation timestamp is invalid")
	}
	if err := feeder.clusterState.RecordOOM(info.ContainerID, info.Timestamp, info.Memory); err != nil {
		return err
	}
	// Signal retention is deliberately narrower than the histogram window.
	// Ineligible quick-update evidence must not suppress ordinary OOM sizing.
	if !signal.Valid(now) {
		return nil
	}
	if err := oomsignal.Record(feeder.externalOOMRecorder, signal); err != nil {
		klog.ErrorS(err, "Cannot emit accepted external OOM Event", "pod", klog.KObj(pod))
	}
	klog.V(2).InfoS("Accepted external OOM observation", "pod", klog.KObj(pod), "container", info.ContainerID.ContainerName, "vpa", vpa.ID.VpaName)
	return nil
}
