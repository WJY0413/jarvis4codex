---
name: jarvis-notify
description: Deliver one authorized message through the configured 0.3.0 Jarvis notification adapter.
---

# Jarvis Notify (0.3.0 candidate)

Use `jarvis_notify(message, request_id, source_ref?)` only when the loaded notification adapter supports the intended recipient/channel and the message is authorized. This is not the legacy internal `jarvis.notify(monitor_id, observed_thread_id, receipt_target_thread_id)` operation. No direct Feishu/email API is implied by this Skill.

Retain the request receipt, then use `jarvis_read_delivery(request_id=...)` or the exact returned `message_id` for delivery readback. Queued/accepted is not delivered; only a verified channel message ID proves delivery. Keep business result and delivery state separate. Never retry an ambiguous send with a new ID.
