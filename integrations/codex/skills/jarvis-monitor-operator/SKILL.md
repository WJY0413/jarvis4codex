---
name: jarvis-monitor-operator
description: Control a Jarvis product monitor through the versioned jarvisctl/API surface. Use when defining, inspecting, enabling, disabling, or reviewing monitored sources, state transitions, thresholds, deduplication, and monitor receipts.
---

# Jarvis Monitor Operator

Use only the product CLI/API. A monitor observes facts and emits events; it must not send messages, wake a harness, or execute business work directly.

1. Read source health and the current monitor specification.
2. Preview subject, sampling cadence, threshold, deduplication, and resulting event policy.
3. Apply the required confirmation gate for changes.
4. Execute through `jarvisctl`.
5. Read receipts and separately report observation, policy, execution, and delivery status.
