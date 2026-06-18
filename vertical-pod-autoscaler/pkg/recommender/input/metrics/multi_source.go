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

package metrics

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// multiSourceClusterState is the slice of model.ClusterState multiSource needs
// to decide which pods belong to an annotated VPA. model.ClusterState satisfies
// it.
type multiSourceClusterState interface {
	VPAs() map[model.VpaID]*model.Vpa
	GetMatchingPodsForVPAs(vpas []*model.Vpa) map[model.PodID]bool
}

// multiSource composes a default PodMetricsLister (typically metrics-server)
// with an external-metrics PodMetricsLister that only handles VPAs opted in
// via annotations. Every pod owned by an annotated VPA is served exclusively
// from the external source and removed from the default source's results.
type multiSource struct {
	defaultSource  PodMetricsLister
	externalSource PodMetricsLister
	clusterState   multiSourceClusterState
}

// NewMultiSource returns a PodMetricsLister that serves opted-in VPAs from
// externalSource and everything else from defaultSource. externalSource MUST
// be configured with AnnotatedVPAsOnly so it returns metrics only for VPAs
// that have opted in. clusterState is used to determine pod ownership.
func NewMultiSource(defaultSource, externalSource PodMetricsLister, clusterState multiSourceClusterState) PodMetricsLister {
	return &multiSource{
		defaultSource:  defaultSource,
		externalSource: externalSource,
		clusterState:   clusterState,
	}
}

func (s *multiSource) List(ctx context.Context, namespace string, opts metav1.ListOptions) (*v1beta1.PodMetricsList, error) {
	externalList, err := s.externalSource.List(ctx, namespace, opts)
	if err != nil {
		return nil, err
	}
	defaultList, err := s.defaultSource.List(ctx, namespace, opts)
	if err != nil {
		return nil, err
	}

	// Exclude every pod owned by an annotated VPA from the default
	// (metrics-server) source — by VPA pod membership, NOT by which pods the
	// external (Prometheus) query happened to return. A freshly-started pod has
	// a metrics-server sample within seconds but no Prometheus sample until its
	// exporter is first scraped (for .NET, after the app boots, runs a GC, and
	// is scraped — a few minutes). Keying the exclusion on the external result
	// would let that pod's metrics-server (RSS) sample leak through in the
	// meantime; for a workload that rolls frequently the recommender's
	// peak-based memory model then locks onto those startup-RSS spikes,
	// defeating the per-VPA Prometheus signal (e.g. GC-committed memory).
	// Until Prometheus observes a freshly-started pod it simply contributes no
	// sample, which is correct: no data beats wrong (RSS) data.
	excluded := s.annotatedVPAPods(namespace)

	merged := v1beta1.PodMetricsList{
		Items: make([]v1beta1.PodMetrics, 0, len(externalList.Items)+len(defaultList.Items)),
	}
	merged.Items = append(merged.Items, externalList.Items...)
	for _, p := range defaultList.Items {
		if _, override := excluded[p.Namespace+"/"+p.Name]; override {
			continue
		}
		merged.Items = append(merged.Items, p)
	}
	return &merged, nil
}

// annotatedVPAPods returns the set of "namespace/name" keys for the pods that
// match a VPA carrying external-metric annotations. Restricted to namespace
// when it is non-empty, mirroring the List filter.
func (s *multiSource) annotatedVPAPods(namespace string) map[string]struct{} {
	var annotated []*model.Vpa
	for _, vpa := range s.clusterState.VPAs() {
		if namespace != "" && vpa.ID.Namespace != namespace {
			continue
		}
		if annotations.HasExternalMetricOverride(vpa.Annotations) {
			annotated = append(annotated, vpa)
		}
	}
	excluded := make(map[string]struct{})
	if len(annotated) == 0 {
		return excluded
	}
	// One pass over the pod set for all annotated VPAs. List runs ~once per
	// recommender interval; GetMatchingPodsForVPAs collapses what would be a
	// full pod scan per annotated VPA into a single traversal.
	for podID := range s.clusterState.GetMatchingPodsForVPAs(annotated) {
		excluded[podID.Namespace+"/"+podID.PodName] = struct{}{}
	}
	return excluded
}
