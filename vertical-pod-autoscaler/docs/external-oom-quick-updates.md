# Expedited updates from application OOM counters

The fork can use an application OOM counter to accelerate an in-place memory increase, even when the application catches the error and Kubernetes does not report `OOMKilled`.

## Contents

<!-- toc -->
- [Requirements](#requirements)
  - [Signal publication permission](#signal-publication-permission)
- [Behavior](#behavior)
- [Observability and rollout](#observability-and-rollout)
<!-- /toc -->

## Requirements

- Keep the installed upstream VPA CRDs unchanged. Run the fork's recommender and updater with annotation-based signal support; older components ignore the signal annotation.
- Use `InPlace` or `InPlaceOrRecreate` with the corresponding Kubernetes support and VPA feature gates enabled.
- Configure the existing `external.vpa.k8s.io/oom-counter-metric` VPA annotation with a Prometheus counter selector. Its pod/container labels must match the observer configuration, and its series must be scoped to the VPA's target pods.
- Allow the observer to establish a baseline. Its first observation does not synthesize historical OOM events.

### Signal publication permission

The shortcut needs `patch` permission on VPA metadata, separate from ordinary `verticalpodautoscalers/status` writes. Grant it to the recommender independently of the chart. For example, this namespaced binding permits publication only for VPAs in `workloads`; adjust the VPA namespace and existing recommender ServiceAccount name/namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: vpa-external-oom-annotation-writer
  namespace: workloads
rules:
  - apiGroups: ["autoscaling.k8s.io"]
    resources: ["verticalpodautoscalers"]
    verbs: ["patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: vpa-external-oom-annotation-writer
  namespace: workloads
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: vpa-external-oom-annotation-writer
subjects:
  - kind: ServiceAccount
    name: vpa-recommender
    namespace: kube-system
```

Without this permission, ordinary OOM sizing and recommendation status updates continue. The recommender logs `Cannot publish external OOM annotation`; the updater cannot expedite an unpublished observation.

## Behavior

An accepted counter increase follows the existing OOM histogram path. After successful ingestion, the recommender also publishes a versioned `external.vpa.k8s.io/oom-signals` annotation containing the pod name and UID, container name, counter selector, VPA generation, observation time and memory request at observation. Signals are restored from that annotation after recommender restarts and expire after ten minutes. Retention is limited to 128 pod/container entries; the annotation carries the newest entries that fit within 32 KiB and the remaining Kubernetes annotation budget. Metadata writes protect the VPA UID and resource version and leave other annotations unchanged. Malformed, oversized or unknown-version payloads are ignored. Counter selectors longer than 1024 bytes are not eligible for this signal path.

The updater can bypass the normal pod-lifetime and minimum-change gates when the exact pod/container has a fresh signal and the policy-capped memory recommendation exceeds its current request. It retains the normal admission, disruption-budget, rate-limit, resource-policy and infeasible-update safeguards.

The expedited action uses a per-pod recommendation snapshot that changes only memory for the signaled containers. CPU and other containers are not changed. Pods that still carry CPU-startup-boost annotations remain on the existing boost-worker path until those annotations are removed. This snapshot is carried through admission and resize execution. Atomic resize preconditions check the pod UID and original memory values, so a replacement pod or concurrent memory increase cannot be overwritten. Failed, deferred or infeasible expedited resizes do not fall back to eviction. `Recreate`, `Auto`, `Initial` and `Off` retain their ordinary update rules.

A signal cannot accelerate another increase after the pod's request exceeds the request recorded at the OOM. Duplicate observations do not refresh their timestamp. Pod UID changes and counter-selector changes establish a new counter baseline. Stale observations, changed policy generations, invalid ownership and events rejected by the histogram do not publish an expedited-update signal.

This is **not a forced recommendation increase or memory floor**. The histogram must still produce a higher recommendation, and policy limits still apply. It also does not alter Kubernetes pod termination status or the native quick-OOM path.

## Observability and rollout

The recommender logs `Accepted external OOM observation` after successful ingestion. Inspect `metadata.annotations["external.vpa.k8s.io/oom-signals"]` alongside `status.recommendation` to distinguish an accepted signal from a recommendation that remains unchanged or capped.

Keep the existing CRDs and chart unchanged. Grant the metadata-write permission separately, then upgrade the recommender and updater. Removing the OOM-counter annotation disables this shortcut and prunes retained signals on the next recommender refresh. Rolling either component back leaves the ordinary recommendation and update paths available; the optional annotation is not required for sizing. Removing the counter configuration disables expedited use immediately, even if metadata cleanup is denied.
