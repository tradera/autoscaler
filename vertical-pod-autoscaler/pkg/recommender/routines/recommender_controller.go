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
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"

	vpa_clientset "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/checkpoint"
	recommender_config "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/config"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/history"
	input_metrics "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/logic"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/metrics"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

const (
	// aggregateContainerStateGCInterval defines how often expired AggregateContainerStates are garbage collected.
	aggregateContainerStateGCInterval               = 1 * time.Hour
	scaleCacheEntryLifetime           time.Duration = time.Hour
	scaleCacheEntryFreshnessTime      time.Duration = 10 * time.Minute
	scaleCacheEntryJitterFactor       float64       = 1.
	scaleCacheLoopPeriod                            = 7 * time.Second
)

// RecommenderController wraps the Recommender with the full lifecycle
// (informer setup, ticker loop, health checks) needed to run as a standalone controller.
// It implements controllercontext.Controller.
type RecommenderController struct {
	recommender Recommender
	interval    time.Duration
	healthCheck *metrics.HealthCheck
}

// NewRecommenderController creates a RecommenderController
func NewRecommenderController(
	ctx context.Context,
	kubeConfig *rest.Config,
	kubeClient kube_client.Interface,
	vpaClient *vpa_clientset.Clientset,
	factory informers.SharedInformerFactory,
	config *recommender_config.RecommenderConfig,
	healthCheck *metrics.HealthCheck,
	stopCh <-chan struct{},
) (*RecommenderController, error) {
	commonFlags := config.CommonFlags

	clusterState := model.NewClusterState(aggregateContainerStateGCInterval)
	controllerFetcher := controllerfetcher.NewControllerFetcher(kubeConfig, kubeClient, factory, scaleCacheEntryFreshnessTime, scaleCacheEntryLifetime, scaleCacheEntryJitterFactor, stopCh)
	podLister, oomObserver := input.NewPodListerAndOOMObserver(ctx, kubeClient, commonFlags.VpaObjectNamespace, stopCh)

	// One Prometheus client serves both the per-VPA OOM observer and the
	// per-VPA history backfiller. Both features are opt-in via VPA
	// annotations, so no annotated VPAs means no requests.
	var promAPI prometheusv1.API
	if config.PrometheusAddress != "" {
		var err error
		promAPI, err = history.NewPrometheusAPI(config.PrometheusAddress, config.PrometheusInsecure, history.PrometheusCredentials{
			BearerToken: config.PrometheusBearerToken,
			Username:    config.Username,
			Password:    config.Password,
		})
		if err != nil {
			return nil, fmt.Errorf("init prometheus client: %w", err)
		}
	}

	if err := startPrometheusOOMObserver(ctx, config, clusterState, oomObserver.GetObservedOomsChannel(), promAPI); err != nil {
		return nil, err
	}

	perVPABackfiller, err := newPerVPABackfiller(config, promAPI)
	if err != nil {
		return nil, err
	}

	model.InitializeAggregationsConfig(model.NewAggregationsConfig(
		config.MemoryAggregationInterval,
		config.MemoryAggregationIntervalCount,
		config.MemoryHistogramDecayHalfLife,
		config.CpuHistogramDecayHalfLife,
		config.OOMBumpUpRatio,
		config.OOMMinBumpUp,
	))

	useCheckpoints := config.Storage != "prometheus"

	var postProcessors []RecommendationPostProcessor
	if config.PostProcessorCPUasInteger {
		postProcessors = append(postProcessors, &IntegerCPUPostProcessor{})
	}

	globalMaxAllowed := initGlobalMaxAllowed(config)
	postProcessors = append(postProcessors, NewCappingRecommendationProcessor(globalMaxAllowed))

	queryTimeout, err := time.ParseDuration(config.QueryTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse query-timeout for prometheus metrics client: %w", err)
	}

	var source input_metrics.PodMetricsLister
	if config.UseExternalMetrics {
		if promAPI == nil {
			return nil, errors.New("--use-external-metrics requires --prometheus-address: per-VPA metric annotations now query Prometheus directly")
		}
		resourceMetrics := map[corev1.ResourceName]string{}
		if config.ExternalCpuMetric != "" {
			resourceMetrics[corev1.ResourceCPU] = config.ExternalCpuMetric
		}
		if config.ExternalMemoryMetric != "" {
			resourceMetrics[corev1.ResourceMemory] = config.ExternalMemoryMetric
		}
		prometheusClientOptions := input_metrics.PrometheusClientOptions{
			ResourceMetrics:    resourceMetrics,
			PodNameLabel:       config.CtrPodNameLabel,
			ContainerNameLabel: config.CtrNameLabel,
			QueryTimeout:       queryTimeout,
		}
		klog.V(1).InfoS("Using Prometheus metrics source", "options", prometheusClientOptions)
		source = input_metrics.NewPrometheusClient(promAPI, clusterState, prometheusClientOptions)
	} else {
		// Mixed mode: metrics-server is the global default. VPAs that opt in
		// via external.vpa.k8s.io/{cpu,memory}-metric annotations are served
		// by the Prometheus client. A nil promAPI here means no Prometheus
		// address was configured — those annotations silently no-op back to
		// metrics-server for those VPAs.
		defaultSource := input_metrics.NewPodMetricsesSource(resourceclient.NewForConfigOrDie(kubeConfig))
		if promAPI == nil {
			klog.V(1).InfoS("Using Metrics Server only (no --prometheus-address; per-VPA metric annotations disabled)")
			source = defaultSource
		} else {
			prometheusClientOptions := input_metrics.PrometheusClientOptions{
				PodNameLabel:       config.CtrPodNameLabel,
				ContainerNameLabel: config.CtrNameLabel,
				QueryTimeout:       queryTimeout,
				AnnotatedVPAsOnly:  true,
			}
			externalSource := input_metrics.NewPrometheusClient(promAPI, clusterState, prometheusClientOptions)
			klog.V(1).InfoS("Using Metrics Server with per-VPA Prometheus overrides")
			source = input_metrics.NewMultiSource(defaultSource, externalSource)
		}
	}

	ignoredNamespaces := strings.Split(commonFlags.IgnoredVpaObjectNamespaces, ",")

	clusterStateFeeder := input.ClusterStateFeederFactory{
		PodLister:           podLister,
		OOMObserver:         oomObserver,
		KubeClient:          kubeClient,
		MetricsClient:       input_metrics.NewMetricsClient(source, commonFlags.VpaObjectNamespace, "default-metrics-client"),
		VpaCheckpointClient: vpaClient.AutoscalingV1(),
		VpaLister:           vpa_api_util.NewVpasLister(vpaClient, stopCh, commonFlags.VpaObjectNamespace),
		VpaCheckpointLister: vpa_api_util.NewVpaCheckpointLister(vpaClient, stopCh, commonFlags.VpaObjectNamespace),
		ClusterState:        clusterState,
		SelectorFetcher:     target.NewVpaTargetSelectorFetcher(kubeConfig, kubeClient, factory, stopCh),
		MemorySaveMode:      config.MemorySaver,
		ControllerFetcher:   controllerFetcher,
		RecommenderName:     config.RecommenderName,
		IgnoredNamespaces:   ignoredNamespaces,
		VpaObjectNamespace:  commonFlags.VpaObjectNamespace,
		PerVPABackfiller:    perVPABackfiller,
	}.Make()
	controllerFetcher.Start(ctx, scaleCacheLoopPeriod)

	recommender := RecommenderFactory{
		ClusterState:       clusterState,
		ClusterStateFeeder: clusterStateFeeder,
		ControllerFetcher:  controllerFetcher,
		CheckpointWriter:   checkpoint.NewCheckpointWriter(clusterState, vpaClient.AutoscalingV1()),
		VpaClient:          vpaClient.AutoscalingV1(),
		PodResourceRecommender: logic.CreatePodResourceRecommender(logic.RecommendationConfig{
			SafetyMarginFraction:       config.SafetyMarginFraction,
			PodMinCPUMillicores:        config.PodMinCPUMillicores,
			PodMinMemoryMb:             config.PodMinMemoryMb,
			TargetCPUPercentile:        config.TargetCPUPercentile,
			LowerBoundCPUPercentile:    config.LowerBoundCPUPercentile,
			UpperBoundCPUPercentile:    config.UpperBoundCPUPercentile,
			ConfidenceIntervalCPU:      config.ConfidenceIntervalCPU,
			TargetMemoryPercentile:     config.TargetMemoryPercentile,
			LowerBoundMemoryPercentile: config.LowerBoundMemoryPercentile,
			UpperBoundMemoryPercentile: config.UpperBoundMemoryPercentile,
			ConfidenceIntervalMemory:   config.ConfidenceIntervalMemory,
		}),
		RecommendationFormat: logic.RecommendationFormat{
			HumanizeMemory:     config.HumanizeMemory,
			RoundCPUMillicores: config.RoundCPUMillicores,
			RoundMemoryBytes:   config.RoundMemoryBytes,
		},
		RecommendationPostProcessors: postProcessors,
		CheckpointsGCInterval:        config.CheckpointsGCInterval,
		CheckpointsWriteTimeout:      config.CheckpointsWriteTimeout,
		UseCheckpoints:               useCheckpoints,
		UpdateWorkerCount:            config.UpdateWorkerCount,
	}.Make()

	if err := initHistoryProvider(ctx, recommender, config); err != nil {
		return nil, err
	}

	return &RecommenderController{
		recommender: recommender,
		interval:    config.MetricsFetcherInterval,
		healthCheck: healthCheck,
	}, nil
}

// Run starts the recommender loop and blocks until ctx is cancelled.
// It implements controllercontext.Controller.
func (c *RecommenderController) Run(ctx context.Context) error {
	c.healthCheck.StartMonitoring()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.recommender.RunOnce()
			c.healthCheck.UpdateLastActivity()
		}
	}
}

// startPrometheusOOMObserver wires a Prometheus-backed OOM observer that
// complements the event-driven one. It polls the configured Prometheus
// instance for per-VPA OOM-counter annotations and writes synthetic OomInfo
// events into the shared channel, so VPA's bump-up logic can react to
// runtime-internal OOMs that don't trigger a container OOMKill (e.g. .NET
// OutOfMemoryException).
//
// promAPI is supplied by the caller (nil = disabled). Without any annotated
// VPAs the observer issues no queries — the cost is an idle ticker.
func startPrometheusOOMObserver(ctx context.Context, config *recommender_config.RecommenderConfig, clusterState model.ClusterState, oomChan chan<- oom.OomInfo, promAPI prometheusv1.API) error {
	if promAPI == nil {
		return nil
	}
	queryTimeout, err := time.ParseDuration(config.QueryTimeout)
	if err != nil {
		return fmt.Errorf("parse query-timeout for OOM observer: %w", err)
	}
	observer := oom.NewPrometheusObserver(oom.PrometheusObserverConfig{
		API:            promAPI,
		ClusterState:   clusterState,
		OomChan:        oomChan,
		PollInterval:   config.PrometheusOOMObserverInterval,
		QueryTimeout:   queryTimeout,
		PodLabel:       config.CtrPodNameLabel,
		ContainerLabel: config.CtrNameLabel,
	})
	go observer.Run(ctx)
	klog.V(1).InfoS("Started Prometheus OOM observer", "interval", config.PrometheusOOMObserverInterval, "address", config.PrometheusAddress)
	return nil
}

// newPerVPABackfiller builds the optional per-VPA history backfiller. Returns
// nil (and no error) when promAPI is nil — VPAs without history annotations
// see no behavior change.
func newPerVPABackfiller(config *recommender_config.RecommenderConfig, promAPI prometheusv1.API) (*history.PerVPAProvider, error) {
	if promAPI == nil {
		return nil, nil
	}
	queryTimeout, err := time.ParseDuration(config.QueryTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse query-timeout for backfiller: %w", err)
	}
	historyDuration, err := prommodel.ParseDuration(config.HistoryLength)
	if err != nil {
		return nil, fmt.Errorf("parse history-length for backfiller: %w", err)
	}
	historyResolution, err := prommodel.ParseDuration(config.HistoryResolution)
	if err != nil {
		return nil, fmt.Errorf("parse history-resolution for backfiller: %w", err)
	}
	return history.NewPerVPAProvider(promAPI, history.PerVPAProviderOpts{
		QueryTimeout:      queryTimeout,
		HistoryDuration:   time.Duration(historyDuration),
		HistoryResolution: time.Duration(historyResolution),
		PodLabel:          config.CtrPodNameLabel,
		ContainerLabel:    config.CtrNameLabel,
	}), nil
}

func initGlobalMaxAllowed(config *recommender_config.RecommenderConfig) corev1.ResourceList {
	result := make(corev1.ResourceList)
	if !config.MaxAllowedCPU.IsZero() {
		result[corev1.ResourceCPU] = config.MaxAllowedCPU.Quantity
	}
	if !config.MaxAllowedMemory.IsZero() {
		result[corev1.ResourceMemory] = config.MaxAllowedMemory.Quantity
	}
	return result
}

func initHistoryProvider(ctx context.Context, rec Recommender, config *recommender_config.RecommenderConfig) error {
	useCheckpoints := config.Storage != "prometheus"
	if useCheckpoints {
		rec.GetClusterStateFeeder().InitFromCheckpoints(ctx)
	} else {
		promQueryTimeout, err := time.ParseDuration(config.QueryTimeout)
		if err != nil {
			return err
		}
		histConfig := history.PrometheusHistoryProviderConfig{
			Address:                config.PrometheusAddress,
			Insecure:               config.PrometheusInsecure,
			QueryTimeout:           promQueryTimeout,
			HistoryLength:          config.HistoryLength,
			HistoryResolution:      config.HistoryResolution,
			PodLabelPrefix:         config.PodLabelPrefix,
			PodLabelsMetricName:    config.PodLabelsMetricName,
			PodNamespaceLabel:      config.PodNamespaceLabel,
			PodNameLabel:           config.PodNameLabel,
			CtrNamespaceLabel:      config.CtrNamespaceLabel,
			CtrPodNameLabel:        config.CtrPodNameLabel,
			CtrNameLabel:           config.CtrNameLabel,
			CadvisorMetricsJobName: config.PrometheusJobName,
			Namespace:              config.CommonFlags.VpaObjectNamespace,
			Authentication: history.PrometheusCredentials{
				BearerToken: config.PrometheusBearerToken,
				Username:    config.Username,
				Password:    config.Password,
			},
		}
		provider, err := history.NewPrometheusHistoryProvider(histConfig)
		if err != nil {
			return err
		}
		rec.GetClusterStateFeeder().InitFromHistoryProvider(provider)
	}
	return nil
}
