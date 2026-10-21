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

package oom

import (
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// sourceVPAs returns isolated metadata, never mutable live model objects when
// informer sources are configured. The legacy model provider remains usable by
// embedders that do not need identity-bound updater signals.
func (o *PrometheusObserver) sourceVPAs() map[model.VpaID]*model.Vpa {
	if o.vpaLister == nil {
		return o.clusterState.VPAs()
	}
	sources, err := o.vpaLister.List(labels.Everything())
	if err != nil {
		klog.ErrorS(err, "Cannot list external OOM sources")
		return nil
	}
	result := make(map[model.VpaID]*model.Vpa)
	name := o.recommenderName
	if name == "" {
		name = "default"
	}
	for _, source := range sources {
		if slices.Contains(o.ignoredNamespaces, source.Namespace) || annotations.OOMCounterMetric(source.Annotations) == "" {
			continue
		}
		selected := len(source.Spec.Recommenders) == 0 && name == "default"
		for _, selector := range source.Spec.Recommenders {
			if selector.Name == name {
				selected = true
			}
		}
		if !selected {
			continue
		}
		id := model.VpaID{Namespace: source.Namespace, VpaName: source.Name}
		result[id] = &model.Vpa{ID: id, Annotations: maps.Clone(source.Annotations), Generation: source.Generation}
	}
	return result
}
