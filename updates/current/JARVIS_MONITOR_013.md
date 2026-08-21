# Jarvis Monitor 0.1.3

`Monitor` is independent from `Heartbeat`: it reads one existing Codex thread at
3--60 second intervals for at most 24 hours.  It never starts a turn in the
observed thread.

## Start request

Required: `observed_thread_id`.  Optional: `monitor_name`, `monitor_key`,
`source_thread_id`, `source_event_key`, `interval_seconds`,
`expires_in_minutes`, `model`, `reasoning_effort`, and `outputs`.

Each output is one of:

- `resume_thread` with `target_thread_id`; optional `user_message_text`.
- `resume_source_thread`; requires `source_thread_id`; optional
  `user_message_text`.
- `notify_jarvis_bot`; optional `notification_text`.

The default output text is `JARVIS_MONITOR_COMPLETED_V1` plus the monitor and
observed-thread identity.  Each resume gets an internally persisted stable
`client_user_message_id`; `source_event_key` remains a separate trace field.

## Runtime commands

`jarvis_monitor_service.py --config <heartbeat-config> --dispatcher-root <root>`
supports `monitor-start --request`, `monitor-status --monitor-id`,
`monitor-run-once`, and `monitor-serve`.

`monitor-run-once` is a testing/diagnostic action, not a Heartbeat.
