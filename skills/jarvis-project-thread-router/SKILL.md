---
name: jarvis-project-thread-router
description: Optional exact-thread routing policy; requires a separately configured local route registry.
---

# Jarvis Project Thread Router (0.3.0 candidate, local integration)

This policy is not a Jarvis MCP routing tool. Use it only when the deployment has an authorized route registry, resolver and outbox. A reply resolves to the original delivered `source_thread_id`; explicit configured ID outranks unique specific alias; ambiguous/missing routes go to a configured inbox, never an inferred COO thread. Preserve the exact task and its stored cwd; a busy target remains queued, not rerouted. Read native identity before sending and require a real delivery `message_id` for outbound success.

No private `thread_routes.json`, local Chief-of-Staff path, inbox ID, Bridge config or Bot credentials are in this release package. Without a configured resolver and authorization, report `unsupported`; do not install, alter routes, wake a task or claim cloud portability from this text alone.
