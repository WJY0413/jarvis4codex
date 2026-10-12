---
name: jarvis-subproject-dispatcher-v2
description: Optional visible-child governance when a local dispatcher and parent callback route are configured.
---

# Jarvis Subproject Dispatcher V2 (0.3.0 candidate, local integration)

This governance profile is distinct from native `jarvis_loop` auto-continuation. Use only with an authorized visible-thread dispatcher, exact parent callback route, configured heartbeat and outbox; none is supplied by installing this Skill. Do not layer its next-package dispatch over a native auto-continuing Loop. Read each child, parent, output and QA boundary before creating tasks.

For a supported local deployment, keep the sequence: authorized visible child creation → verified monitor/heartbeat registration → exact child terminal plus callback → parent acceptance → one configured Cooper outbox notification with delivery readback. Inactive-nonterminal means parent action, not automatic resume/replacement. `jarvis_notify(message,request_id,source_ref?)` does not itself perform the old COO acceptance routing; if dispatcher/route is absent, report `unsupported`. No Bot credentials, private registry or business queue are bundled.
