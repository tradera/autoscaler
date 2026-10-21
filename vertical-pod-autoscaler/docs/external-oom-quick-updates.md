# Expedited updates from application OOM counters

The fork can use an application OOM counter to accelerate an in-place memory increase, even when the application catches the error and Kubernetes does not report `OOMKilled`.

## Requirements

- Install the updated VPA CRDs, recommender and updater from the same fork revision. Older CRDs prune the optional signal field; older updaters ignore it.
- Use `InPlace` or `InPlaceOrRecreate` with the corresponding Kubernetes support and VPA feature gates enabled.
- Configure the existing `external.vpa.k8s.io/oom-counter-metric` VPA annotation with a Prometheus counter selector. Its pod/container labels must match the observer configuration, and its series must be scoped to the VPA's target pods.
- Allow the observer to establish a baseline. Its first observation does not synthesize historical OOM events.

No additional Kubernetes write permission is needed: the recommender uses its existing VPA status writer.

## Behavior

An accepted counter increase follows the existing OOM histogram path. After successful ingestion, the recommender also publishes an `externalOOMSignals` status entry containing the pod name and UID, container name, counter selector, VPA generation, observation time and memory request at observation. Signals are restored from status after recommender restarts, expire after ten minutes, and are limited to the 128 newest pod/container entries per VPA. Counter selectors longer than 1024 bytes are not eligible for this signal path.

The updater can bypass the normal pod-lifetime and minimum-change gates when the exact pod/container has a fresh signal and the policy-capped memory recommendation exceeds its current request. It retains the normal admission, disruption-budget, rate-limit, resource-policy and infeasible-update safeguards.

The expedited action uses a per-pod recommendation snapshot that changes only memory for the signaled containers. CPU and other containers are not changed. Pods that still carry CPU-startup-boost annotations remain on the existing boost-worker path until those annotations are removed. This snapshot is carried through admission and resize execution. Atomic resize preconditions check the pod UID and original memory values, so a replacement pod or concurrent memory increase cannot be overwritten. Failed, deferred or infeasible expedited resizes do not fall back to eviction. `Recreate`, `Auto`, `Initial` and `Off` retain their ordinary update rules.

A signal cannot accelerate another increase after the pod's request exceeds the request recorded at the OOM. Duplicate observations do not refresh their timestamp. Pod UID changes and counter-selector changes establish a new counter baseline. Stale observations, changed policy generations, invalid ownership and events rejected by the histogram do not publish an expedited-update signal.

This is **not a forced recommendation increase or memory floor**. The histogram must still produce a higher recommendation, and policy limits still apply. It also does not alter Kubernetes pod termination status or the native quick-OOM path.

## Observability and rollout

The recommender logs `Accepted external OOM observation` after successful ingestion. Inspect `status.externalOOMSignals` alongside `status.recommendation` to distinguish an accepted signal from a recommendation that remains unchanged or capped.

Upgrade the CRDs before relying on signal propagation, then upgrade both components. Removing the OOM-counter annotation disables this shortcut and prunes retained signals on the next recommender refresh. Rolling either component back leaves the ordinary recommendation and update paths available; the optional status field is not required for sizing.
