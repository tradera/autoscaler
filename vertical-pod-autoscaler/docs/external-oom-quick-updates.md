# Expedited updates from application OOM counters

The fork can use an application OOM counter to accelerate an in-place memory increase, even when the application catches the error and Kubernetes does not report `OOMKilled`. Accepted observations are published as advisory Kubernetes Events, not VPA runtime annotations or custom status fields.

## Contents

<!-- toc -->
- [Requirements](#requirements)
- [Event contract](#event-contract)
- [Updater behavior](#updater-behavior)
- [Observability and rollout](#observability-and-rollout)
<!-- /toc -->

## Requirements

- Keep the installed upstream VPA APIs and CRDs unchanged. Run the fork's recommender and updater with external-OOM Event support.
- Use `InPlace` or `InPlaceOrRecreate` with the corresponding Kubernetes support and VPA feature gates enabled.
- Configure the existing `external.vpa.k8s.io/oom-counter-metric` VPA annotation with a Prometheus counter selector. Its pod/container labels must match the observer configuration, and its series must be scoped to the VPA's target pods.
- Allow the observer to establish a baseline. Series in its first successful poll of a VPA are baselines and do not synthesize historical OOM events. A series that appears after that poll counts from zero, so a runtime that creates the series at its first OOM, such as .NET, has that OOM counted. A pod that replaces a tracked pod of the same name starts from a new baseline.
- Permit the recommender to create/patch Events and the updater to list Events in its configured namespace scope. The checked-in component roles already include these permissions. No VPA metadata-write permission is needed.

## Event contract

After the histogram accepts an OOM observation, the recommender emits a core/v1 Event with reason `ExternalOOMObserved`, type `Warning`, and source component `vpa-external-oom-observer`. Its involved Pod reference supplies namespace, name and UID. A versioned JSON message carries the container name, VPA name/UID/generation, selector SHA-256 fingerprint, original observation time and memory request at observation. The message is limited to 1 KiB; the selector itself is not copied into Events.

This is a private machine-readable Event contract, not parsing arbitrary human messages. Native `OOMKilled` status is never fabricated. The recorder preserves distinct structured payloads instead of combining their messages into summaries. Exact duplicate Events may be counted together; Event counts and retry timestamps do not create new observations.

Events are best-effort. Recorder queues, retries and spam filtering can lose signals under overload or API failure. Missing Events lose only expedited eligibility: ordinary histogram sizing and recommendation updates continue. Event reporting fields identify the protocol, not an authenticated sender; restrict Event-writing permissions to trusted identities in namespaces where the shortcut is enabled.

## Updater behavior

On its normal reconciliation pass, the updater lists matching Events, including those retained across an updater restart. It checks original observation age, source protocol, Pod UID/container, VPA UID/generation and configured selector fingerprint. Unknown, malformed, future and expired observations are ignored. The signal lifetime is ten minutes, independently of the cluster's Event retention setting.

The optional read is paginated and bounded to two seconds and 10,000 matching Events per pass. At most 128 newest pod/container entries are retained per VPA for that pass. Duplicates do not refresh observation times. Excessive volume or Event-read failures can reduce fast-path coverage, but do not stop ordinary updater processing. No persistent Event cache, extra watcher or startup synchronization barrier is added.

A fresh accepted observation lets the updater bypass the normal pod-lifetime and minimum-change gates only when the policy-capped memory recommendation exceeds the current request. The expedited action changes memory for the signaled containers only. CPU and other containers are not changed. Pods with CPU-startup-boost annotations remain on the existing boost-worker path until those annotations are removed.

The shortcut retains admission, disruption-budget, rate-limit, resource-policy and infeasible-update safeguards. A desired or applied memory request above the observation baseline makes the signal ineligible, including while kubelet status is catching up with a submitted resize. Atomic resize preconditions check Pod UID, resource version and original memory values. Failed, deferred or infeasible expedited resizes do not fall back to eviction. Other update modes retain their ordinary rules.

This is **not a forced recommendation increase or memory floor**. The histogram must still produce a higher recommendation. It does not change native quick-OOM handling or promise a seconds-level response: observer polling, recommender processing and updater reconciliation still determine latency.

## Observability and rollout

The recommender logs `Accepted external OOM observation` after successful ingestion. Use `kubectl describe pod` or inspect Events with reason `ExternalOOMObserved`, alongside the VPA's ordinary `status.recommendation`. An accepted observation is not proof that the recommendation increased, an Event was delivered, or a resize succeeded.

Keep the existing CRDs and chart, verify Event permissions, and upgrade both components. Removing the counter annotation immediately disables expedited use of retained Events. Rolling either component back leaves ordinary recommendation and update paths available; older components ignore the custom Events. No runtime VPA signal annotation, metadata patch role or draft status-field migration is required.
