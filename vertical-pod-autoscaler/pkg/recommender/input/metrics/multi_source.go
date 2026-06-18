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
)

// multiSource composes a default PodMetricsLister (typically metrics-server)
// with an external-metrics PodMetricsLister that only handles VPAs opted in
// via annotations. Pods served by the external source are removed from the
// default source's results to avoid double-counting downstream.
type multiSource struct {
	defaultSource  PodMetricsLister
	externalSource PodMetricsLister
}

// NewMultiSource returns a PodMetricsLister that serves opted-in VPAs from
// externalSource and everything else from defaultSource. externalSource MUST
// be configured with AnnotatedVPAsOnly so it returns metrics only for VPAs
// that have opted in.
func NewMultiSource(defaultSource, externalSource PodMetricsLister) PodMetricsLister {
	return &multiSource{
		defaultSource:  defaultSource,
		externalSource: externalSource,
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

	external := make(map[string]struct{}, len(externalList.Items))
	for _, p := range externalList.Items {
		external[p.Namespace+"/"+p.Name] = struct{}{}
	}

	merged := v1beta1.PodMetricsList{
		Items: make([]v1beta1.PodMetrics, 0, len(externalList.Items)+len(defaultList.Items)),
	}
	merged.Items = append(merged.Items, externalList.Items...)
	for _, p := range defaultList.Items {
		if _, override := external[p.Namespace+"/"+p.Name]; override {
			continue
		}
		merged.Items = append(merged.Items, p)
	}
	return &merged, nil
}
