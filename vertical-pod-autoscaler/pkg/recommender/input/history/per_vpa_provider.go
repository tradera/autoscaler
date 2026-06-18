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

package history

import (
	"context"
	"fmt"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
)

// PerVPAProvider runs per-VPA Prometheus range queries (driven by the VPA's
// history-query annotations) and returns the result as a PodHistory map ready
// to be applied to clusterState the same way InitFromHistoryProvider applies
// the cluster-wide bootstrap query.
//
// Unlike the cluster-wide history provider, queries here are user-supplied
// PromQL strings — the operator setting the annotation is responsible for
// scoping the query to the workload (e.g. via label matchers). The provider
// only requires that result series carry pod and container labels under the
// configured names.
type PerVPAProvider struct {
	api  perVPAQueryAPI
	opts PerVPAProviderOpts
}

// PerVPAProviderOpts configures a PerVPAProvider.
type PerVPAProviderOpts struct {
	// QueryTimeout bounds each individual range query.
	QueryTimeout time.Duration
	// HistoryDuration is how far back from now() to query.
	HistoryDuration time.Duration
	// HistoryResolution is the step size for the range query.
	HistoryResolution time.Duration
	// PodLabel and ContainerLabel are the label names on the result metrics
	// that identify pod and container respectively.
	PodLabel       string
	ContainerLabel string
}

// perVPAQueryAPI is the slice of prometheusv1.API this provider uses.
// Defined locally so tests can stub it.
type perVPAQueryAPI interface {
	QueryRange(ctx context.Context, query string, r prometheusv1.Range, opts ...prometheusv1.Option) (prommodel.Value, prometheusv1.Warnings, error)
}

// NewPerVPAProvider constructs a PerVPAProvider.
func NewPerVPAProvider(api prometheusv1.API, opts PerVPAProviderOpts) *PerVPAProvider {
	return &PerVPAProvider{api: api, opts: opts}
}

// GetVPAHistory issues range queries for whichever resources the VPA opted
// into via annotations and returns a per-pod sample history. Returns an
// empty map (and no error) for VPAs without any history annotations.
func (p *PerVPAProvider) GetVPAHistory(ctx context.Context, vpa *model.Vpa) (map[model.PodID]*PodHistory, error) {
	res := make(map[model.PodID]*PodHistory)
	resources := []struct {
		query    string
		resource model.ResourceName
	}{
		{annotations.HistoryQueryForResource(vpa.Annotations, corev1.ResourceCPU), model.ResourceCPU},
		{annotations.HistoryQueryForResource(vpa.Annotations, corev1.ResourceMemory), model.ResourceMemory},
	}
	for _, r := range resources {
		if r.query == "" {
			continue
		}
		if err := p.readQueryRange(ctx, res, vpa.ID.Namespace, r.query, r.resource); err != nil {
			return nil, fmt.Errorf("backfill %s: %w", r.resource, err)
		}
	}
	return res, nil
}

func (p *PerVPAProvider) readQueryRange(ctx context.Context, out map[model.PodID]*PodHistory, namespace, query string, resource model.ResourceName) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.opts.QueryTimeout)
	defer cancel()

	end := time.Now()
	start := end.Add(-p.opts.HistoryDuration)
	result, _, err := p.api.QueryRange(queryCtx, query, prometheusv1.Range{
		Start: start,
		End:   end,
		Step:  p.opts.HistoryResolution,
	})
	if err != nil {
		return err
	}
	matrix, ok := result.(prommodel.Matrix)
	if !ok {
		return fmt.Errorf("expected matrix; got %T", result)
	}

	for _, ts := range matrix {
		podName := string(ts.Metric[prommodel.LabelName(p.opts.PodLabel)])
		ctrName := string(ts.Metric[prommodel.LabelName(p.opts.ContainerLabel)])
		if podName == "" || ctrName == "" {
			continue
		}
		podID := model.PodID{Namespace: namespace, PodName: podName}
		ph, ok := out[podID]
		if !ok {
			ph = newEmptyHistory()
			out[podID] = ph
		}
		ph.Samples[ctrName] = append(ph.Samples[ctrName], getContainerUsageSamplesFromSamples(ts.Values, resource)...)
	}
	return nil
}
