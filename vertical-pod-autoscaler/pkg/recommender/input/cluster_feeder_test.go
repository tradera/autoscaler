/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	listersv1 "k8s.io/client-go/listers/core/v1"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	fakeautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned/typed/autoscaling.k8s.io/v1/fake"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/history"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/metrics"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/oom"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/input/spec"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/recommender/model"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	target_mock "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/mock"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
)

type fakeControllerFetcher struct {
	key *controllerfetcher.ControllerKeyWithAPIVersion
	err error
}

func (f *fakeControllerFetcher) FindTopMostWellKnownOrScalable(_ context.Context, _ *controllerfetcher.ControllerKeyWithAPIVersion) (*controllerfetcher.ControllerKeyWithAPIVersion, error) {
	return f.key, f.err
}

func parseLabelSelector(selector string) labels.Selector {
	labelSelector, _ := metav1.ParseToLabelSelector(selector)
	parsedSelector, _ := metav1.LabelSelectorAsSelector(labelSelector)
	return parsedSelector
}

var (
	recommenderName                     = "name"
	empty                               = ""
	unsupportedConditionTextFromFetcher = "Cannot read targetRef. Reason: targetRef not defined"
	unsupportedConditionNoExtraText     = "Cannot read targetRef"
	unsupportedConditionNoTargetRef     = "Cannot read targetRef"
	unsupportedConditionMudaMudaMuda    = "Error checking if target taxonomy.doestar/doseph-doestar is a topmost well-known or scalable controller: muda muda muda"
	unsupportedTargetRefHasParent       = "The target stardust.dodokind/dotaro has a parent controller but it should point to a topmost well-known or scalable controller"
)

const (
	kind         = "dodokind"
	name1        = "dotaro"
	name2        = "doseph"
	namespace    = "testNamespace"
	apiVersion   = "stardust"
	testGcPeriod = time.Minute
)

// NewClusterState returns a new clusterState with no pods.
func NewFakeClusterState(vpas map[model.VpaID]*model.Vpa, pods map[model.PodID]*model.PodState) *fakeClusterState {
	return &fakeClusterState{
		stubbedVPAs:  vpas,
		stubbedPods:  pods,
		addedSamples: make(map[model.ContainerID][]*model.ContainerUsageSampleWithKey),
	}
}

type fakeClusterState struct {
	model.ClusterState
	addedPods    []model.PodID
	addedSamples map[model.ContainerID][]*model.ContainerUsageSampleWithKey
	stubbedVPAs  map[model.VpaID]*model.Vpa
	stubbedPods  map[model.PodID]*model.PodState
}

func (cs *fakeClusterState) AddSample(sample *model.ContainerUsageSampleWithKey) error {
	_, podExists := cs.stubbedPods[sample.Container.PodID]
	if !podExists {
		return model.NewKeyError(sample.Container.PodID)
	}
	samplesForContainer := cs.addedSamples[sample.Container]
	cs.addedSamples[sample.Container] = append(samplesForContainer, sample)
	return nil
}

func (cs *fakeClusterState) AddOrUpdatePod(podID model.PodID, _ labels.Set, _ corev1.PodPhase) {
	cs.addedPods = append(cs.addedPods, podID)
	if cs.stubbedPods == nil {
		cs.stubbedPods = make(map[model.PodID]*model.PodState)
	}
	if cs.stubbedPods[podID] == nil {
		cs.stubbedPods[podID] = &model.PodState{
			ID:         podID,
			Containers: make(map[string]*model.ContainerState),
		}
	}
}

func (cs *fakeClusterState) SetInitContainers(podID model.PodID, initContainers []string) error {
	pod, podExists := cs.stubbedPods[podID]
	if !podExists || pod == nil {
		return model.NewKeyError(podID)
	}
	pod.InitContainers = append([]string(nil), initContainers...)
	return nil
}

func (cs *fakeClusterState) Pods() map[model.PodID]*model.PodState {
	return cs.stubbedPods
}

func (cs *fakeClusterState) VPAs() map[model.VpaID]*model.Vpa {
	return cs.stubbedVPAs
}

func (*fakeClusterState) StateMapSize() int {
	return 0
}

func TestLoadVPAs(t *testing.T) {
	type testCase struct {
		name                                string
		selector                            labels.Selector
		fetchSelectorError                  error
		targetRef                           *autoscalingv1.CrossVersionObjectReference
		topMostWellKnownOrScalableKey       *controllerfetcher.ControllerKeyWithAPIVersion
		findTopMostWellKnownOrScalableError error
		expectedSelector                    labels.Selector
		expectedConfigUnsupported           *string
		expectedConfigDeprecated            *string
		expectedVpaFetch                    bool
		recommenderName                     *string
		recommender                         string
	}

	testCases := []testCase{
		{
			name:                      "no selector",
			selector:                  nil,
			fetchSelectorError:        errors.New("targetRef not defined"),
			expectedSelector:          labels.Nothing(),
			expectedConfigUnsupported: &unsupportedConditionTextFromFetcher,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          true,
		},
		{
			name:                      "also no selector but no error",
			selector:                  nil,
			fetchSelectorError:        nil,
			expectedSelector:          labels.Nothing(),
			expectedConfigUnsupported: &unsupportedConditionNoExtraText,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          true,
		},
		{
			name:               "targetRef selector",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			topMostWellKnownOrScalableKey: &controllerfetcher.ControllerKeyWithAPIVersion{
				ControllerKey: controllerfetcher.ControllerKey{
					Kind:      kind,
					Name:      name1,
					Namespace: namespace,
				},
				ApiVersion: apiVersion,
			},
			expectedSelector:          parseLabelSelector("app = test"),
			expectedConfigUnsupported: nil,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          true,
		},
		{
			name:                      "no targetRef",
			selector:                  parseLabelSelector("app = test"),
			fetchSelectorError:        nil,
			expectedSelector:          labels.Nothing(),
			expectedConfigUnsupported: nil,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          true,
		},
		{
			name:               "can't decide if top-level-ref",
			selector:           nil,
			fetchSelectorError: nil,
			expectedSelector:   labels.Nothing(),
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			expectedConfigUnsupported: &unsupportedConditionNoTargetRef,
			expectedVpaFetch:          true,
		},
		{
			name:               "non-top-level targetRef",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			expectedSelector:   labels.Nothing(),
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			topMostWellKnownOrScalableKey: &controllerfetcher.ControllerKeyWithAPIVersion{
				ControllerKey: controllerfetcher.ControllerKey{
					Kind:      kind,
					Name:      name2,
					Namespace: namespace,
				},
				ApiVersion: apiVersion,
			},
			expectedConfigUnsupported: &unsupportedTargetRefHasParent,
			expectedVpaFetch:          true,
		},
		{
			name:               "error checking if top-level-ref",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			expectedSelector:   labels.Nothing(),
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       "doestar",
				Name:       "doseph-doestar",
				APIVersion: "taxonomy",
			},
			expectedConfigUnsupported:           &unsupportedConditionMudaMudaMuda,
			expectedVpaFetch:                    true,
			findTopMostWellKnownOrScalableError: errors.New("muda muda muda"),
		},
		{
			name:               "top-level target ref",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			expectedSelector:   parseLabelSelector("app = test"),
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			topMostWellKnownOrScalableKey: &controllerfetcher.ControllerKeyWithAPIVersion{
				ControllerKey: controllerfetcher.ControllerKey{
					Kind:      kind,
					Name:      name1,
					Namespace: namespace,
				},
				ApiVersion: apiVersion,
			},
			expectedConfigUnsupported: nil,
			expectedVpaFetch:          true,
		},
		{
			name:               "no recommenderName",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			topMostWellKnownOrScalableKey: &controllerfetcher.ControllerKeyWithAPIVersion{
				ControllerKey: controllerfetcher.ControllerKey{
					Kind:      kind,
					Name:      name1,
					Namespace: namespace,
				},
				ApiVersion: apiVersion,
			},
			expectedSelector:          parseLabelSelector("app = test"),
			expectedConfigUnsupported: nil,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          false,
			recommenderName:           &empty,
		},
		{
			name:               "recommenderName doesn't match recommender",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			topMostWellKnownOrScalableKey: &controllerfetcher.ControllerKeyWithAPIVersion{
				ControllerKey: controllerfetcher.ControllerKey{
					Kind:      kind,
					Name:      name1,
					Namespace: namespace,
				},
				ApiVersion: apiVersion,
			},
			expectedSelector:          parseLabelSelector("app = test"),
			expectedConfigUnsupported: nil,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          false,
			recommenderName:           &recommenderName,
			recommender:               "other",
		},
		{
			name:               "recommenderName matches recommender",
			selector:           parseLabelSelector("app = test"),
			fetchSelectorError: nil,
			targetRef: &autoscalingv1.CrossVersionObjectReference{
				Kind:       kind,
				Name:       name1,
				APIVersion: apiVersion,
			},
			topMostWellKnownOrScalableKey: &controllerfetcher.ControllerKeyWithAPIVersion{
				ControllerKey: controllerfetcher.ControllerKey{
					Kind:      kind,
					Name:      name1,
					Namespace: namespace,
				},
				ApiVersion: apiVersion,
			},
			expectedSelector:          parseLabelSelector("app = test"),
			expectedConfigUnsupported: nil,
			expectedConfigDeprecated:  nil,
			expectedVpaFetch:          true,
			recommenderName:           &recommenderName,
			recommender:               recommenderName,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			vpaBuilder := test.VerticalPodAutoscaler().WithName("testVpa").WithContainer("container").WithNamespace("testNamespace").WithTargetRef(tc.targetRef)
			if tc.recommender != "" {
				vpaBuilder = vpaBuilder.WithRecommender(tc.recommender)
			}
			vpa := vpaBuilder.Get()
			vpaLister := &test.VerticalPodAutoscalerListerMock{}
			vpaLister.On("List").Return([]*vpa_types.VerticalPodAutoscaler{vpa}, nil)

			targetSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
			clusterState := model.NewClusterState(testGcPeriod)

			clusterStateFeeder := clusterStateFeeder{
				vpaLister:       vpaLister,
				clusterState:    clusterState,
				selectorFetcher: targetSelectorFetcher,
				controllerFetcher: &fakeControllerFetcher{
					key: tc.topMostWellKnownOrScalableKey,
					err: tc.findTopMostWellKnownOrScalableError,
				},
			}
			if tc.recommenderName == nil {
				clusterStateFeeder.recommenderName = DefaultRecommenderName
			} else {
				clusterStateFeeder.recommenderName = *tc.recommenderName
			}

			if tc.expectedVpaFetch {
				targetSelectorFetcher.EXPECT().Fetch(vpa).Return(tc.selector, tc.fetchSelectorError)
			}
			clusterStateFeeder.LoadVPAs(context.Background())

			vpaID := model.VpaID{
				Namespace: vpa.Namespace,
				VpaName:   vpa.Name,
			}

			if !tc.expectedVpaFetch {
				assert.NotContains(t, clusterState.VPAs(), vpaID)
				return
			}
			assert.Contains(t, clusterState.VPAs(), vpaID)
			storedVpa := clusterState.VPAs()[vpaID]
			if tc.expectedSelector != nil {
				assert.NotNil(t, storedVpa.PodSelector)
				assert.Equal(t, tc.expectedSelector.String(), storedVpa.PodSelector.String())
			} else {
				assert.Nil(t, storedVpa.PodSelector)
			}

			conditions := storedVpa.GetConditionsMap()
			if tc.expectedConfigDeprecated != nil {
				assert.Contains(t, conditions, vpa_types.ConfigDeprecated)
				assert.Equal(t, *tc.expectedConfigDeprecated, conditions[vpa_types.ConfigDeprecated].Message)
			} else {
				assert.NotContains(t, conditions, vpa_types.ConfigDeprecated)
			}

			if tc.expectedConfigUnsupported != nil {
				assert.Contains(t, conditions, vpa_types.ConfigUnsupported)
				assert.Equal(t, *tc.expectedConfigUnsupported, conditions[vpa_types.ConfigUnsupported].Message)
			} else {
				assert.NotContains(t, conditions, vpa_types.ConfigUnsupported)
			}
		})
	}
}

type testSpecClient struct {
	pods []*spec.BasicPodSpec
}

func (c *testSpecClient) GetPodSpecs() ([]*spec.BasicPodSpec, error) {
	return c.pods, nil
}

func makeTestSpecClient(podLabels []map[string]string) spec.SpecClient {
	pods := make([]*spec.BasicPodSpec, len(podLabels))
	for i, l := range podLabels {
		pods[i] = &spec.BasicPodSpec{
			ID:        model.PodID{Namespace: "default", PodName: fmt.Sprintf("pod-%d", i)},
			PodLabels: l,
		}
	}
	return &testSpecClient{
		pods: pods,
	}
}

func newTestContainerSpec(podID model.PodID, containerName string, millicores int, memory int64) spec.BasicContainerSpec {
	containerID := model.ContainerID{
		PodID:         podID,
		ContainerName: containerName,
	}
	requestedResources := model.Resources{
		model.ResourceCPU:    model.ResourceAmount(millicores),
		model.ResourceMemory: model.ResourceAmount(memory),
	}
	return spec.BasicContainerSpec{
		ID:      containerID,
		Image:   containerName + "Image",
		Request: requestedResources,
	}
}

func newTestPodSpec(podId model.PodID, containerSpecs []spec.BasicContainerSpec, initContainerSpecs []spec.BasicContainerSpec) *spec.BasicPodSpec {
	return &spec.BasicPodSpec{
		ID:             podId,
		PodLabels:      map[string]string{podId.PodName + "LabelKey": podId.PodName + "LabelValue"},
		Containers:     containerSpecs,
		InitContainers: initContainerSpecs,
	}
}

func TestClusterStateFeeder_LoadPods_ContainerTracking(t *testing.T) {
	podWithoutInitContainersID := model.PodID{Namespace: "default", PodName: "PodWithoutInitContainers"}
	containerSpecs := []spec.BasicContainerSpec{
		newTestContainerSpec(podWithoutInitContainersID, "container1", 500, 512*1024*1024),
		newTestContainerSpec(podWithoutInitContainersID, "container2", 1000, 1024*1024*1024),
	}
	podWithoutInitContainers := newTestPodSpec(podWithoutInitContainersID, containerSpecs, nil)

	podWithInitContainersID := model.PodID{Namespace: "default", PodName: "PodWithInitContainers"}
	containerSpecs2 := []spec.BasicContainerSpec{
		newTestContainerSpec(podWithInitContainersID, "container1", 2000, 2048*1024*1024),
	}
	initContainerSpecs2 := []spec.BasicContainerSpec{
		newTestContainerSpec(podWithInitContainersID, "init1", 40, 128*1024*1024),
		newTestContainerSpec(podWithInitContainersID, "init2", 100, 256*1024*1024),
	}
	podWithInitContainers := newTestPodSpec(podWithInitContainersID, containerSpecs2, initContainerSpecs2)

	client := &testSpecClient{pods: []*spec.BasicPodSpec{podWithoutInitContainers, podWithInitContainers}}

	clusterState := model.NewClusterState(testGcPeriod)

	feeder := clusterStateFeeder{
		specClient:     client,
		memorySaveMode: false,
		clusterState:   clusterState,
	}

	feeder.LoadPods()

	assert.Equal(t, len(feeder.clusterState.Pods()), 2)
	assert.Equal(t, len(feeder.clusterState.Pods()[podWithInitContainersID].Containers), 1)
	assert.Equal(t, len(feeder.clusterState.Pods()[podWithInitContainersID].InitContainers), 2)
	assert.Equal(t, len(feeder.clusterState.Pods()[podWithoutInitContainersID].Containers), 2)
	assert.Equal(t, len(feeder.clusterState.Pods()[podWithoutInitContainersID].InitContainers), 0)

	// Re-loading the same pods must not cause the init container list to grow.
	// This guards against a regression where LoadPods appended to the existing
	// slice on every invocation, leading to unbounded growth over time.
	feeder.LoadPods()
	feeder.LoadPods()

	assert.Equal(t, len(feeder.clusterState.Pods()), 2)
	assert.Equal(t, len(feeder.clusterState.Pods()[podWithInitContainersID].InitContainers), 2)
	assert.ElementsMatch(t,
		[]string{"init1", "init2"},
		feeder.clusterState.Pods()[podWithInitContainersID].InitContainers,
	)
	assert.Equal(t, len(feeder.clusterState.Pods()[podWithoutInitContainersID].InitContainers), 0)
}

func TestClusterStateFeeder_LoadPods_PodDeletion(t *testing.T) {
	pod1ID := model.PodID{Namespace: "default", PodName: "Pod1"}
	pod1 := newTestPodSpec(pod1ID, nil, nil)
	pod2ID := model.PodID{Namespace: "default", PodName: "Pod2"}
	pod2 := newTestPodSpec(pod2ID, nil, nil)

	client := &testSpecClient{pods: []*spec.BasicPodSpec{pod1, pod2}}
	clusterState := model.NewClusterState(testGcPeriod)

	feeder := clusterStateFeeder{
		specClient:     client,
		memorySaveMode: false,
		clusterState:   clusterState,
	}

	feeder.LoadPods()
	assert.Len(t, feeder.clusterState.Pods(), 2)

	client.pods = []*spec.BasicPodSpec{pod1}
	feeder.LoadPods()

	// Since deletion is shifted, pod2 should still exist in clusterState.
	assert.Len(t, feeder.clusterState.Pods(), 2)
	assert.Contains(t, feeder.clusterState.Pods(), pod2ID)

	feeder.DeleteRemovedPods()
	// Now pod2 should be deleted.
	assert.Len(t, feeder.clusterState.Pods(), 1)
	assert.NotContains(t, feeder.clusterState.Pods(), pod2ID)
}

func TestClusterStateFeeder_ReconcileOOMsFromPodState(t *testing.T) {
	podID := model.PodID{Namespace: "default", PodName: "denormalizer"}
	const containerName = "container1"
	containerID := model.ContainerID{PodID: podID, ContainerName: containerName}

	// A running pod whose previous instance was OOMKilled: the kill survives on
	// LastTerminationState even though the live observer (OnAdd no-op) never saw
	// the transition.
	makePod := func(finishedAt time.Time, requestMi int64) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: podID.Namespace, Name: podID.PodName},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: containerName,
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceMemory: *resource.NewQuantity(requestMi*1024*1024, resource.BinarySI),
				}},
			}}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				Name: containerName,
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					Reason: "OOMKilled", FinishedAt: metav1.NewTime(finishedAt),
				}},
			}}},
		}
	}

	specClient := &testSpecClient{pods: []*spec.BasicPodSpec{
		newTestPodSpec(podID, []spec.BasicContainerSpec{
			newTestContainerSpec(podID, containerName, 100, 256*1024*1024),
		}, nil),
	}}

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	assert.NoError(t, indexer.Add(makePod(time.Now().Add(-2*time.Minute), 256)))
	podLister := listersv1.NewPodLister(indexer)

	clusterState := model.NewClusterState(testGcPeriod)
	feeder := clusterStateFeeder{
		specClient:   specClient,
		clusterState: clusterState,
		podLister:    podLister,
	}

	// First load tracks the pod and runs the one-shot reconcile, recovering the
	// OOM the live observer never saw.
	feeder.LoadPods()
	assert.True(t, feeder.oomBootReconcileDone)
	container := clusterState.GetContainer(containerID)
	if !assert.NotNil(t, container) {
		return
	}
	recovered := container.GetMaxMemoryPeak()
	assert.NotZero(t, recovered, "boot reconcile should recover the OOM from pod state")

	// Run-once guard: replace the pod with a newer, larger OOM and load again.
	// The reconcile already ran, so it must be skipped — re-reading the sticky
	// LastTerminationState every loop would re-count OOMs and inflate the
	// recommendation (the bump loop). The recovered signal must not change.
	assert.NoError(t, indexer.Update(makePod(time.Now(), 1024)))
	feeder.LoadPods()
	assert.Equal(t, recovered, clusterState.GetContainer(containerID).GetMaxMemoryPeak(),
		"reconcile must run only once; subsequent loads must not re-record OOMs")
}

func TestClusterStateFeeder_LoadPods_MemorySaverMode(t *testing.T) {
	for _, tc := range []struct {
		Name              string
		VPALabelSelectors []string
		PodLabels         []map[string]string
		TrackedPods       int
	}{
		{
			Name:              "simple",
			VPALabelSelectors: []string{"name=vpa-pod"},
			PodLabels: []map[string]string{
				{"name": "vpa-pod"},
				{"type": "stateful"},
			},
			TrackedPods: 1,
		},
		{
			Name:              "multiple",
			VPALabelSelectors: []string{"name=vpa-pod,type=stateful"},
			PodLabels: []map[string]string{
				{"name": "vpa-pod", "type": "stateful"},
				{"type": "stateful"},
				{"name": "vpa-pod"},
			},
			TrackedPods: 1,
		},
		{
			Name:              "no matches",
			VPALabelSelectors: []string{"name=vpa-pod"},
			PodLabels: []map[string]string{
				{"name": "non-vpa-pod", "type": "stateful"},
			},
			TrackedPods: 0,
		},
		{
			Name:              "set based",
			VPALabelSelectors: []string{"environment in (staging, qa),name=vpa-pod"},
			PodLabels: []map[string]string{
				{"name": "vpa-pod", "environment": "staging"},
				{"name": "vpa-pod", "environment": "production"},
				{"name": "non-vpa-pod", "environment": "staging"},
				{"name": "non-vpa-pod", "environment": "production"},
			},
			TrackedPods: 1,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			vpas := make(map[model.VpaID]*model.Vpa)
			for i, selector := range tc.VPALabelSelectors {
				vpaLabel, err := labels.Parse(selector)
				assert.NoError(t, err)
				key := model.VpaID{VpaName: fmt.Sprintf("test-vpa-%d", i), Namespace: "default"}
				vpas[key] = &model.Vpa{PodSelector: vpaLabel}
			}
			clusterState := NewFakeClusterState(vpas, nil)

			feeder := clusterStateFeeder{
				specClient:     makeTestSpecClient(tc.PodLabels),
				memorySaveMode: true,
				clusterState:   clusterState,
			}

			feeder.LoadPods()
			assert.Len(t, clusterState.addedPods, tc.TrackedPods, "number of pods is not %d", tc.TrackedPods)

			clusterState = NewFakeClusterState(vpas, nil)

			feeder = clusterStateFeeder{
				specClient:     makeTestSpecClient(tc.PodLabels),
				memorySaveMode: false,
				clusterState:   clusterState,
			}

			feeder.LoadPods()
			assert.Len(t, clusterState.addedPods, len(tc.PodLabels), "number of pods is not %d", len(tc.PodLabels))
		})
	}
}

func newContainerMetricsSnapshot(id model.ContainerID, cpuUsage int64, memUsage int64) (*metrics.ContainerMetricsSnapshot, []*model.ContainerUsageSampleWithKey) {
	snapshotTimestamp := time.Now()
	snapshotWindow := time.Duration(1234)
	snapshot := &metrics.ContainerMetricsSnapshot{
		ID:             id,
		SnapshotTime:   snapshotTimestamp,
		SnapshotWindow: snapshotWindow,
		Usage: model.Resources{
			model.ResourceCPU:    model.ResourceAmount(cpuUsage),
			model.ResourceMemory: model.ResourceAmount(memUsage),
		},
	}
	samples := []*model.ContainerUsageSampleWithKey{
		{
			Container: id,
			ContainerUsageSample: model.ContainerUsageSample{
				MeasureStart: snapshotTimestamp,
				Resource:     model.ResourceCPU,
				Usage:        model.ResourceAmount(cpuUsage),
			},
		},
		{
			Container: id,
			ContainerUsageSample: model.ContainerUsageSample{
				MeasureStart: snapshotTimestamp,
				Resource:     model.ResourceMemory,
				Usage:        model.ResourceAmount(memUsage),
			},
		},
	}
	return snapshot, samples
}

type fakeMetricsClient struct {
	snapshots []*metrics.ContainerMetricsSnapshot
}

func (m fakeMetricsClient) GetContainersMetrics(_ context.Context) ([]*metrics.ContainerMetricsSnapshot, error) {
	return m.snapshots, nil
}

func TestClusterStateFeeder_LoadRealTimeMetrics(t *testing.T) {
	_, tctx := ktesting.NewTestContext(t)
	namespaceName := "test-namespace"
	podID := model.PodID{Namespace: namespaceName, PodName: "Pod"}
	regularContainer1 := model.ContainerID{PodID: podID, ContainerName: "Container1"}
	regularContainer2 := model.ContainerID{PodID: podID, ContainerName: "Container2"}
	initContainer := model.ContainerID{PodID: podID, ContainerName: "InitContainer"}

	pods := map[model.PodID]*model.PodState{
		podID: {ID: podID,
			Containers: map[string]*model.ContainerState{
				"Container1": {},
				"Container2": {},
			},
			InitContainers: []string{
				"InitContainer",
			}},
	}

	var containerMetricsSnapshots []*metrics.ContainerMetricsSnapshot

	regularContainer1MetricsSnapshot, regularContainer1UsageSamples := newContainerMetricsSnapshot(regularContainer1, 100, 1024)
	containerMetricsSnapshots = append(containerMetricsSnapshots, regularContainer1MetricsSnapshot)
	regularContainer2MetricsSnapshot, regularContainer2UsageSamples := newContainerMetricsSnapshot(regularContainer2, 200, 2048)
	containerMetricsSnapshots = append(containerMetricsSnapshots, regularContainer2MetricsSnapshot)
	initContainer1MetricsSnapshots, _ := newContainerMetricsSnapshot(initContainer, 300, 3072)
	containerMetricsSnapshots = append(containerMetricsSnapshots, initContainer1MetricsSnapshots)

	clusterState := NewFakeClusterState(nil, pods)

	feeder := clusterStateFeeder{
		memorySaveMode: false,
		clusterState:   clusterState,
		metricsClient:  fakeMetricsClient{snapshots: containerMetricsSnapshots},
	}

	feeder.LoadRealTimeMetrics(tctx)

	assert.Equal(t, 2, len(clusterState.addedSamples))

	samplesForContainer1 := clusterState.addedSamples[regularContainer1]
	assert.Contains(t, samplesForContainer1, regularContainer1UsageSamples[0])
	assert.Contains(t, samplesForContainer1, regularContainer1UsageSamples[1])

	samplesForContainer2 := clusterState.addedSamples[regularContainer2]
	assert.Contains(t, samplesForContainer2, regularContainer2UsageSamples[0])
	assert.Contains(t, samplesForContainer2, regularContainer2UsageSamples[1])

	// Add extra container metrics for which there are no added pods to the state to simulate memory-saver=true
	extraPodID := model.PodID{Namespace: namespaceName, PodName: "ExtraPod"}
	extraContainer := model.ContainerID{PodID: extraPodID, ContainerName: "ExtraContainer"}
	extraContainerMetricsSnapshot, _ := newContainerMetricsSnapshot(extraContainer, 200, 2048)
	containerMetricsSnapshots = append(containerMetricsSnapshots, extraContainerMetricsSnapshot)

	clusterState = NewFakeClusterState(nil, pods)

	feeder = clusterStateFeeder{
		memorySaveMode: true,
		clusterState:   clusterState,
		metricsClient:  fakeMetricsClient{snapshots: containerMetricsSnapshots},
	}

	feeder.LoadRealTimeMetrics(tctx)

	assert.Equal(t, 2, len(clusterState.addedSamples))

	_, samplesForExtraContainerExist := clusterState.addedSamples[extraContainer]
	assert.False(t, samplesForExtraContainerExist)
}

func TestClusterStateFeeder_LoadRealTimeMetrics_OOMEventsWithDeletedPods(t *testing.T) {
	_, tctx := ktesting.NewTestContext(t)
	pod1ID := model.PodID{Namespace: "default", PodName: "Pod1"}
	pod2ID := model.PodID{Namespace: "default", PodName: "Pod2"}

	containerSpecs1 := []spec.BasicContainerSpec{
		newTestContainerSpec(pod1ID, "containerA", 500, 512*1024*1024),
	}
	pod1 := newTestPodSpec(pod1ID, containerSpecs1, nil)

	containerSpecs2 := []spec.BasicContainerSpec{
		newTestContainerSpec(pod2ID, "containerB", 500, 512*1024*1024),
	}
	pod2 := newTestPodSpec(pod2ID, containerSpecs2, nil)

	client := &testSpecClient{pods: []*spec.BasicPodSpec{pod1, pod2}}
	clusterState := model.NewClusterState(testGcPeriod)
	oomChan := make(chan oom.OomInfo, 10)

	feeder := clusterStateFeeder{
		specClient:     client,
		memorySaveMode: false,
		clusterState:   clusterState,
		oomChan:        oomChan,
		metricsClient:  fakeMetricsClient{snapshots: nil},
	}

	// Add two pods to the clusterState.
	feeder.LoadPods()
	assert.Len(t, feeder.clusterState.Pods(), 2)

	// Add OOM events for both pods to the oomChan.
	timestamp1 := time.Now()
	oomChan <- oom.OomInfo{
		Timestamp:   timestamp1,
		Memory:      model.ResourceAmount(1024 * 1024 * 1024),
		ContainerID: model.ContainerID{PodID: pod1ID, ContainerName: "containerA"},
	}
	oomChan <- oom.OomInfo{
		Timestamp:   timestamp1,
		Memory:      model.ResourceAmount(1024 * 1024 * 1024),
		ContainerID: model.ContainerID{PodID: pod2ID, ContainerName: "containerB"},
	}

	feeder.LoadRealTimeMetrics(tctx)

	// Asserting both OomInfo has been processed.
	c1State := feeder.clusterState.GetContainer(model.ContainerID{PodID: pod1ID, ContainerName: "containerA"})
	c2State := feeder.clusterState.GetContainer(model.ContainerID{PodID: pod2ID, ContainerName: "containerB"})
	assert.NotNil(t, c1State)
	assert.NotNil(t, c2State)
	assert.Greater(t, int64(c1State.GetMaxMemoryPeak()), int64(0))
	assert.Greater(t, int64(c2State.GetMaxMemoryPeak()), int64(0))

	// Add new higher memory OOM events to oomChan for both pods
	timestamp2 := timestamp1.Add(time.Minute)
	oomChan <- oom.OomInfo{
		Timestamp:   timestamp2,
		Memory:      model.ResourceAmount(2048 * 1024 * 1024),
		ContainerID: model.ContainerID{PodID: pod1ID, ContainerName: "containerA"},
	}
	oomChan <- oom.OomInfo{
		Timestamp:   timestamp2,
		Memory:      model.ResourceAmount(2048 * 1024 * 1024),
		ContainerID: model.ContainerID{PodID: pod2ID, ContainerName: "containerB"},
	}

	// Remove pod2 from spec client, simulating its eviction.
	client.pods = []*spec.BasicPodSpec{pod1}
	feeder.LoadPods()

	// Pod2 should still exist in clusterState.
	assert.Len(t, feeder.clusterState.Pods(), 2)
	assert.Contains(t, feeder.clusterState.Pods(), pod2ID)

	peak1Before := c1State.GetMaxMemoryPeak()
	peak2Before := c2State.GetMaxMemoryPeak()

	// Load Real-time metrics. OomInfo of Pod2 must be processed.
	feeder.LoadRealTimeMetrics(tctx)
	assert.Greater(t, int64(c1State.GetMaxMemoryPeak()), int64(peak1Before))
	assert.Greater(t, int64(c2State.GetMaxMemoryPeak()), int64(peak2Before))

	// Call DeleteRemovedPods to delete pod2.
	feeder.DeleteRemovedPods()
	assert.Len(t, feeder.clusterState.Pods(), 1)
	assert.NotContains(t, feeder.clusterState.Pods(), pod2ID)
}

type fakeHistoryProvider struct {
	history map[model.PodID]*history.PodHistory
	err     error
}

func (fhp *fakeHistoryProvider) GetClusterHistory() (map[model.PodID]*history.PodHistory, error) {
	return fhp.history, fhp.err
}

func TestClusterStateFeeder_InitFromHistoryProvider(t *testing.T) {
	pod1 := model.PodID{
		Namespace: "ns",
		PodName:   "a-pod",
	}
	memAmount := model.ResourceAmount(128 * 1024 * 1024)
	t0 := time.Date(2021, time.August, 30, 10, 21, 0, 0, time.UTC)
	containerCpu := "containerCpu"
	containerMem := "containerMem"
	pod1History := history.PodHistory{
		LastLabels: map[string]string{},
		LastSeen:   t0,
		Samples: map[string][]model.ContainerUsageSample{
			containerCpu: {
				{
					MeasureStart: t0,
					Usage:        10,
					Resource:     model.ResourceCPU,
				},
			},
			containerMem: {
				{
					MeasureStart: t0,
					Usage:        memAmount,
					Resource:     model.ResourceMemory,
				},
			},
		},
	}
	provider := fakeHistoryProvider{
		history: map[model.PodID]*history.PodHistory{
			pod1: &pod1History,
		},
	}

	clusterState := model.NewClusterState(testGcPeriod)
	feeder := clusterStateFeeder{
		clusterState: clusterState,
	}
	feeder.InitFromHistoryProvider(&provider)
	if !assert.Contains(t, feeder.clusterState.Pods(), pod1) {
		return
	}
	pod1State := feeder.clusterState.Pods()[pod1]
	if !assert.Contains(t, pod1State.Containers, containerCpu) {
		return
	}
	containerState := pod1State.Containers[containerCpu]
	if !assert.NotNil(t, containerState) {
		return
	}
	assert.Equal(t, t0, containerState.LastCPUSampleStart)
	if !assert.Contains(t, pod1State.Containers, containerMem) {
		return
	}
	containerState = pod1State.Containers[containerMem]
	if !assert.NotNil(t, containerState) {
		return
	}
	assert.Equal(t, memAmount, containerState.GetMaxMemoryPeak())
}

func TestFilterVPAs(t *testing.T) {
	recommenderName := "test-recommender"
	defaultRecommenderName := "default-recommender"

	vpa1 := &vpa_types.VerticalPodAutoscaler{
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: defaultRecommenderName},
			},
		},
	}
	vpa2 := &vpa_types.VerticalPodAutoscaler{
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: recommenderName},
			},
		},
	}
	vpa3 := &vpa_types.VerticalPodAutoscaler{
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: "another-recommender"},
			},
		},
	}

	allVpaCRDs := []*vpa_types.VerticalPodAutoscaler{vpa1, vpa2, vpa3}

	feeder := &clusterStateFeeder{
		recommenderName: recommenderName,
	}

	// Set expected results
	expectedResult := []*vpa_types.VerticalPodAutoscaler{vpa2}

	// Run the filterVPAs function
	result := filterVPAs(feeder, allVpaCRDs)

	if len(result) != len(expectedResult) {
		t.Fatalf("expected %d VPAs, got %d", len(expectedResult), len(result))
	}

	assert.ElementsMatch(t, expectedResult, result)
}

func TestFilterVPAsIgnoreNamespaces(t *testing.T) {
	vpa1 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "namespace1",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: DefaultRecommenderName},
			},
		},
	}
	vpa2 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "namespace2",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: DefaultRecommenderName},
			},
		},
	}
	vpa3 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ignore1",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: DefaultRecommenderName},
			},
		},
	}
	vpa4 := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ignore2",
		},
		Spec: vpa_types.VerticalPodAutoscalerSpec{
			Recommenders: []*vpa_types.VerticalPodAutoscalerRecommenderSelector{
				{Name: DefaultRecommenderName},
			},
		},
	}

	allVpaCRDs := []*vpa_types.VerticalPodAutoscaler{vpa1, vpa2, vpa3, vpa4}

	feeder := &clusterStateFeeder{
		recommenderName:   DefaultRecommenderName,
		ignoredNamespaces: []string{"ignore1", "ignore2"},
	}

	// Set expected results
	expectedResult := []*vpa_types.VerticalPodAutoscaler{vpa1, vpa2}

	// Run the filterVPAs function
	result := filterVPAs(feeder, allVpaCRDs)

	if len(result) != len(expectedResult) {
		t.Fatalf("expected %d VPAs, got %d", len(expectedResult), len(result))
	}

	assert.ElementsMatch(t, expectedResult, result)
}

func TestCanCleanupCheckpoints(t *testing.T) {
	_, tctx := ktesting.NewTestContext(t)
	namespace := "testNamespace"

	vpaBuilder := test.VerticalPodAutoscaler().WithContainer("container").WithNamespace(namespace).WithTargetRef(&autoscalingv1.CrossVersionObjectReference{
		Kind:       kind,
		Name:       name1,
		APIVersion: apiVersion,
	})

	balanced := vpaBuilder.WithRecommender("balanced").WithName("balanced").Get()
	performance := vpaBuilder.WithRecommender("performance").WithName("performance").Get()
	savings := vpaBuilder.WithRecommender("savings").WithName("savings").Get()
	defaultVpa := vpaBuilder.WithRecommender("default").WithName("default").Get()

	vpas := []*vpa_types.VerticalPodAutoscaler{balanced, performance, savings, defaultVpa}
	vpaLister := &test.VerticalPodAutoscalerListerMock{}
	vpaLister.On("List").Return(vpas, nil)

	vpaCheckPoint := &vpa_types.VerticalPodAutoscalerCheckpoint{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "nonExistentVPA",
		},
		Spec: vpa_types.VerticalPodAutoscalerCheckpointSpec{
			VPAObjectName: "nonExistentVPA",
		},
	}
	vpacheckpoints := []*vpa_types.VerticalPodAutoscalerCheckpoint{vpaCheckPoint}

	for _, vpa := range vpas {
		vpacheckpoints = append(vpacheckpoints, &vpa_types.VerticalPodAutoscalerCheckpoint{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: vpa.Namespace,
				Name:      vpa.Name,
			},
			Spec: vpa_types.VerticalPodAutoscalerCheckpointSpec{
				VPAObjectName: vpa.Name,
			},
		})
	}

	// Create a mock checkpoint client to track deletions
	checkpointClient := &fakeautoscalingv1.FakeAutoscalingV1{Fake: &core.Fake{}}
	// Track deleted checkpoints
	deletedCheckpoints := []string{}
	checkpointClient.AddReactor("delete", "verticalpodautoscalercheckpoints", func(action core.Action) (bool, runtime.Object, error) {
		deleteAction := action.(core.DeleteAction)
		deletedCheckpoints = append(deletedCheckpoints, deleteAction.GetName())
		return true, nil, nil
	})

	// Create checkpoint lister mock that will return the checkpoint list
	checkpointLister := &test.VerticalPodAutoscalerCheckPointListerMock{}
	checkpointLister.On("List").Return(vpacheckpoints, nil)

	feeder := clusterStateFeeder{
		vpaLister:           vpaLister,
		vpaCheckpointClient: checkpointClient,
		vpaCheckpointLister: checkpointLister,
		clusterState:        model.NewClusterState(testGcPeriod),
		recommenderName:     "default",
	}

	err := feeder.GarbageCollectCheckpoints(tctx)
	assert.NoError(t, err)

	assert.Contains(t, deletedCheckpoints, "nonExistentVPA")

	for _, vpa := range vpas {
		assert.NotContains(t, deletedCheckpoints, vpa.Name)
	}
}

// makeBackfillFeeder builds a feeder wired for per-VPA history backfill, with
// one VPA already known to the model.
func makeBackfillFeeder(t *testing.T, vpaAnnotations map[string]string) (*clusterStateFeeder, model.ClusterState, model.VpaID) {
	t.Helper()
	clusterState := model.NewClusterState(testGcPeriod)
	vpaID := model.VpaID{Namespace: "ns", VpaName: "vpa"}
	vpaCRD := &vpa_types.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   vpaID.Namespace,
			Name:        vpaID.VpaName,
			Annotations: vpaAnnotations,
		},
	}
	require.NoError(t, clusterState.AddOrUpdateVpa(vpaCRD, labels.Everything()))

	feeder := &clusterStateFeeder{
		clusterState:    clusterState,
		backfilledVPAs:  make(map[model.VpaID]struct{}),
		backfillSem:     make(chan struct{}, maxConcurrentBackfills),
		backfillResults: make(chan vpaBackfillResult, maxConcurrentBackfills),
	}
	return feeder, clusterState, vpaID
}

func newBackfillHistory(podName string) map[model.PodID]*history.PodHistory {
	podID := model.PodID{Namespace: "ns", PodName: podName}
	return map[model.PodID]*history.PodHistory{
		podID: {
			LastLabels: map[string]string{},
			Samples: map[string][]model.ContainerUsageSample{
				"ctr": {
					{
						MeasureStart: time.Unix(1, 0),
						Usage:        model.ResourceAmount(100),
						Resource:     model.ResourceCPU,
					},
				},
			},
		},
	}
}

// A completed backfill must not reach clusterState until the main goroutine
// drains it. clusterState is single-writer by design; the previous version
// wrote to it straight from the fetching goroutine, which crashed the
// recommender with "concurrent map read and map write".
func TestApplyPendingBackfillsOnlyWritesWhenDrained(t *testing.T) {
	feeder, clusterState, vpaID := makeBackfillFeeder(t, nil)

	feeder.backfillResults <- vpaBackfillResult{vpaID: vpaID, history: newBackfillHistory("pod-1")}
	assert.Empty(t, clusterState.Pods(), "backfill result must not be applied before it is drained")

	feeder.applyPendingBackfills()

	podID := model.PodID{Namespace: "ns", PodName: "pod-1"}
	require.Contains(t, clusterState.Pods(), podID)
	assert.NotNil(t, clusterState.GetContainer(model.ContainerID{PodID: podID, ContainerName: "ctr"}))
}

// Draining is non-blocking: it returns immediately when nothing is pending, so
// LoadVPAs never stalls waiting on a Prometheus query.
func TestApplyPendingBackfillsIsNonBlockingWhenEmpty(t *testing.T) {
	feeder, clusterState, _ := makeBackfillFeeder(t, nil)

	feeder.applyPendingBackfills()

	assert.Empty(t, clusterState.Pods())
}

// A backfill for a VPA that disappeared while its history was in flight is
// dropped rather than resurrecting pods for a workload we no longer track.
func TestApplyPendingBackfillsDropsResultForDeletedVPA(t *testing.T) {
	feeder, clusterState, _ := makeBackfillFeeder(t, nil)
	goneVpaID := model.VpaID{Namespace: "ns", VpaName: "deleted-vpa"}

	feeder.backfillResults <- vpaBackfillResult{vpaID: goneVpaID, history: newBackfillHistory("pod-2")}
	feeder.applyPendingBackfills()

	assert.Empty(t, clusterState.Pods())
}

// A feeder with backfill disabled (nil channel) must not block or panic.
func TestApplyPendingBackfillsNoopWhenDisabled(t *testing.T) {
	feeder := &clusterStateFeeder{clusterState: model.NewClusterState(testGcPeriod)}

	feeder.applyPendingBackfills()
}

// stubHistoryProvider stands in for history.PerVPAProvider so the fetch
// goroutine can be exercised without a live Prometheus. release, when set,
// holds the fetch open until the test closes it.
type stubHistoryProvider struct {
	returned chan struct{}
	release  <-chan struct{}
	hist     map[model.PodID]*history.PodHistory
	err      error
}

func (s *stubHistoryProvider) GetVPAHistory(_ context.Context, _ model.VpaID, _ map[string]string) (map[model.PodID]*history.PodHistory, error) {
	if s.release != nil {
		<-s.release
	}
	if s.returned != nil {
		close(s.returned)
	}
	return s.hist, s.err
}

// The regression test for the production crash: the fetch goroutine must not
// write clusterState. It is deliberately shaped so that the worker's post-fetch
// work overlaps a stand-in "main loop" hammering clusterState — under `-race`
// (which CI runs, hack/go-unit-tests-vpa.sh) a reintroduced write inside the
// worker trips the detector, and even without `-race` the assertion below
// catches it, because the backfilled pod must not appear until it is drained.
func TestHistoryBackfillNeverWritesClusterStateFromTheWorker(t *testing.T) {
	feeder, clusterState, vpaID := makeBackfillFeeder(t, map[string]string{
		annotations.HistoryQueryCPUAnnotation: "some_metric{}",
	})
	backfilledPod := model.PodID{Namespace: "ns", PodName: "pod-1"}
	release := make(chan struct{})
	feeder.perVPABackfiller = &stubHistoryProvider{release: release, hist: newBackfillHistory(backfilledPod.PodName)}

	feeder.maybeStartHistoryBackfill(vpaID)

	// Stand-in for the recommender's main loop churning clusterState while a
	// backfill is in flight — this is the access the worker used to race.
	stop, writerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			clusterState.AddOrUpdatePod(
				model.PodID{Namespace: "ns", PodName: fmt.Sprintf("main-loop-%d", i)},
				labels.Set{}, corev1.PodRunning)
		}
	}()
	close(release)

	var res vpaBackfillResult
	select {
	case res = <-feeder.backfillResults:
	case <-time.After(10 * time.Second):
		close(stop)
		<-writerDone
		t.Fatal("backfill result never arrived on backfillResults")
	}
	close(stop)
	<-writerDone

	assert.Equal(t, vpaID, res.vpaID)
	assert.NotContains(t, clusterState.Pods(), backfilledPod,
		"the fetch goroutine must hand results back, never write clusterState itself")

	// The result was taken off the channel above, so apply it directly: the
	// point is that applying is what makes it visible, and that only the main
	// goroutine does it.
	feeder.applyHistoryBackfill(res)
	assert.Contains(t, clusterState.Pods(), backfilledPod)
}

// A failed fetch sends nothing, so the drain must stay a no-op rather than
// block or apply a zero-valued result.
func TestHistoryBackfillFetchErrorSendsNothing(t *testing.T) {
	feeder, clusterState, vpaID := makeBackfillFeeder(t, map[string]string{
		annotations.HistoryQueryCPUAnnotation: "some_metric{}",
	})
	// A single slot lets the test synchronize with the worker's release.
	feeder.backfillSem = make(chan struct{}, 1)
	returned := make(chan struct{})
	feeder.perVPABackfiller = &stubHistoryProvider{err: errors.New("prometheus down"), returned: returned}

	feeder.maybeStartHistoryBackfill(vpaID)

	// First prove that the worker acquired its semaphore slot and called the
	// provider. An initially empty semaphore does not prove it has finished.
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("backfill provider was never called")
	}
	// Taking the only slot synchronizes with the worker's release after error
	// logging. Observing len(channel) alone has no happens-before relationship.
	select {
	case feeder.backfillSem <- struct{}{}:
		<-feeder.backfillSem
	case <-time.After(10 * time.Second):
		t.Fatal("backfill worker never released its semaphore")
	}
	assert.Empty(t, feeder.backfillResults)

	feeder.applyPendingBackfills()
	assert.Empty(t, clusterState.Pods())
}

// A VPA without history annotations must not dispatch a fetch at all.
func TestHistoryBackfillSkipsVPAWithoutAnnotations(t *testing.T) {
	feeder, _, vpaID := makeBackfillFeeder(t, nil)
	feeder.perVPABackfiller = &stubHistoryProvider{hist: newBackfillHistory("pod-1")}

	feeder.maybeStartHistoryBackfill(vpaID)

	assert.NotContains(t, feeder.backfilledVPAs, vpaID)
	assert.Empty(t, feeder.backfillResults)
}

func TestInitFromCheckpoints(t *testing.T) {
	const containerName = "container"
	firstSampleStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	newVpa := func(vpaNamespace, vpaName string, recommender string) *vpa_types.VerticalPodAutoscaler {
		builder := test.VerticalPodAutoscaler().WithName(vpaName).WithNamespace(vpaNamespace).WithContainer(containerName).WithTargetRef(&autoscalingv1.CrossVersionObjectReference{
			Kind:       kind,
			Name:       name1,
			APIVersion: apiVersion,
		})
		if recommender != "" {
			builder = builder.WithRecommender(recommender)
		}
		return builder.Get()
	}
	newCheckpoint := func(vpaNamespace, vpaName string, totalSamplesCount int, version string) *vpa_types.VerticalPodAutoscalerCheckpoint {
		return &vpa_types.VerticalPodAutoscalerCheckpoint{
			ObjectMeta: metav1.ObjectMeta{Namespace: vpaNamespace, Name: vpaName + "-" + containerName},
			Spec:       vpa_types.VerticalPodAutoscalerCheckpointSpec{VPAObjectName: vpaName, ContainerName: containerName},
			Status: vpa_types.VerticalPodAutoscalerCheckpointStatus{
				Version:           version,
				TotalSamplesCount: totalSamplesCount,
				FirstSampleStart:  metav1.NewTime(firstSampleStart),
				LastSampleStart:   metav1.NewTime(firstSampleStart.Add(time.Hour)),
			},
		}
	}

	testCases := []struct {
		name              string
		recommenderName   string
		ignoredNamespaces []string
		vpas              []*vpa_types.VerticalPodAutoscaler
		checkpoints       []*vpa_types.VerticalPodAutoscalerCheckpoint
		expectedLoaded    map[model.VpaID]int
		expectedErrors    int
	}{
		{
			name:            "loads checkpoint of tracked VPA",
			recommenderName: DefaultRecommenderName,
			vpas:            []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", "")},
			checkpoints:     []*vpa_types.VerticalPodAutoscalerCheckpoint{newCheckpoint("ns1", "vpa1", 42, model.SupportedCheckpointVersion)},
			expectedLoaded:  map[model.VpaID]int{{Namespace: "ns1", VpaName: "vpa1"}: 42},
		},
		{
			name:            "loads each checkpoint into its own VPA across namespaces",
			recommenderName: DefaultRecommenderName,
			vpas:            []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", ""), newVpa("ns2", "vpa1", "")},
			checkpoints: []*vpa_types.VerticalPodAutoscalerCheckpoint{
				newCheckpoint("ns1", "vpa1", 1, model.SupportedCheckpointVersion),
				newCheckpoint("ns2", "vpa1", 2, model.SupportedCheckpointVersion),
			},
			expectedLoaded: map[model.VpaID]int{{Namespace: "ns1", VpaName: "vpa1"}: 1, {Namespace: "ns2", VpaName: "vpa1"}: 2},
		},
		{
			name:            "skips checkpoint of VPA handled by another recommender",
			recommenderName: DefaultRecommenderName,
			vpas:            []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", ""), newVpa("ns1", "vpa2", "other-recommender")},
			checkpoints: []*vpa_types.VerticalPodAutoscalerCheckpoint{
				newCheckpoint("ns1", "vpa1", 42, model.SupportedCheckpointVersion),
				newCheckpoint("ns1", "vpa2", 7, model.SupportedCheckpointVersion),
			},
			expectedLoaded: map[model.VpaID]int{{Namespace: "ns1", VpaName: "vpa1"}: 42},
		},
		{
			name:            "non-default recommender skips checkpoint of default recommender's VPA",
			recommenderName: "other-recommender",
			vpas:            []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", ""), newVpa("ns1", "vpa2", "other-recommender")},
			checkpoints: []*vpa_types.VerticalPodAutoscalerCheckpoint{
				newCheckpoint("ns1", "vpa1", 42, model.SupportedCheckpointVersion),
				newCheckpoint("ns1", "vpa2", 7, model.SupportedCheckpointVersion),
			},
			expectedLoaded: map[model.VpaID]int{{Namespace: "ns1", VpaName: "vpa2"}: 7},
		},
		{
			name:            "skips orphaned checkpoint",
			recommenderName: DefaultRecommenderName,
			vpas:            []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", "")},
			checkpoints: []*vpa_types.VerticalPodAutoscalerCheckpoint{
				newCheckpoint("ns1", "vpa1", 42, model.SupportedCheckpointVersion),
				newCheckpoint("ns1", "vpa-orphaned", 7, model.SupportedCheckpointVersion),
			},
			expectedLoaded: map[model.VpaID]int{{Namespace: "ns1", VpaName: "vpa1"}: 42},
		},
		{
			name:              "skips checkpoint in ignored namespace",
			recommenderName:   DefaultRecommenderName,
			ignoredNamespaces: []string{"ns-ignored"},
			vpas:              []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", ""), newVpa("ns-ignored", "vpa1", "")},
			checkpoints: []*vpa_types.VerticalPodAutoscalerCheckpoint{
				newCheckpoint("ns1", "vpa1", 42, model.SupportedCheckpointVersion),
				newCheckpoint("ns-ignored", "vpa1", 7, model.SupportedCheckpointVersion),
			},
			expectedLoaded: map[model.VpaID]int{{Namespace: "ns1", VpaName: "vpa1"}: 42},
		},
		{
			name:            "logs error for checkpoint of tracked VPA that cannot be loaded",
			recommenderName: DefaultRecommenderName,
			vpas:            []*vpa_types.VerticalPodAutoscaler{newVpa("ns1", "vpa1", "")},
			checkpoints:     []*vpa_types.VerticalPodAutoscalerCheckpoint{newCheckpoint("ns1", "vpa1", 42, "invalidVersion")},
			expectedLoaded:  map[model.VpaID]int{},
			expectedErrors:  1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true)))
			klog.SetLogger(logger)
			defer klog.ClearLogger()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			vpaLister := &test.VerticalPodAutoscalerListerMock{}
			vpaLister.On("List").Return(tc.vpas, nil)
			checkpointLister := &test.VerticalPodAutoscalerCheckPointListerMock{}
			checkpointLister.On("List").Return(tc.checkpoints, nil)
			targetSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
			targetSelectorFetcher.EXPECT().Fetch(gomock.Any()).Return(parseLabelSelector("app = test"), nil).AnyTimes()

			clusterState := model.NewClusterState(testGcPeriod)
			feeder := clusterStateFeeder{
				vpaLister:           vpaLister,
				vpaCheckpointLister: checkpointLister,
				clusterState:        clusterState,
				selectorFetcher:     targetSelectorFetcher,
				controllerFetcher:   &fakeControllerFetcher{},
				recommenderName:     tc.recommenderName,
				ignoredNamespaces:   tc.ignoredNamespaces,
			}

			feeder.InitFromCheckpoints(t.Context())

			for id, expectedTotalSamplesCount := range tc.expectedLoaded {
				vpa, found := clusterState.VPAs()[id]
				if !assert.True(t, found, "VPA %v not in cluster state", id) {
					continue
				}
				state, found := vpa.ContainersInitialAggregateState[containerName]
				if !assert.True(t, found, "checkpoint not loaded for VPA %v", id) {
					continue
				}
				assert.Equal(t, expectedTotalSamplesCount, state.TotalSamplesCount)
				assert.True(t, firstSampleStart.Equal(state.FirstSampleStart), "unexpected FirstSampleStart %v for VPA %v", state.FirstSampleStart, id)
			}
			for id, vpa := range clusterState.VPAs() {
				if _, expected := tc.expectedLoaded[id]; !expected {
					assert.Empty(t, vpa.ContainersInitialAggregateState, "unexpected checkpoint loaded for VPA %v", id)
				}
			}

			var errorLogs []string
			for _, entry := range logger.GetSink().(ktesting.Underlier).GetBuffer().Data() {
				if entry.Type == ktesting.LogError {
					errorLogs = append(errorLogs, fmt.Sprintf("%s: %v", entry.Message, entry.Err))
				}
			}
			assert.Len(t, errorLogs, tc.expectedErrors, "unexpected error log entries:\n%s", strings.Join(errorLogs, "\n"))
		})
	}
}
