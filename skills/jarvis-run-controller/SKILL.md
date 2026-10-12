---
name: jarvis-run-controller
description: Worker-side execution and receipt discipline for 0.3.0 Loop file or final-answer result modes.
---

# Jarvis Worker Run Controller (0.3.0 candidate)

Use only inside a Worker selected by a Loop; do not launch or supervise the Loop from here. Read the current injected binding, business Skill and `result_verification.mode`. Process only this turn's `candidate_ids`; never reuse a prior turn's IDs or write beyond `output_boundary`.

For `file_receipt`, save each result and exact receipt at the bound `receipt_paths` with the current request/turn identity. For `final_answer_json`, return the agreed JSON in the final answer; do not add six-line control prose, create a file receipt or claim business QA. The Holder records structural mismatch as review where the mode specifies it. `output_schema`/`schema_ref` are format constraints, not proof that facts are correct.

Only a verified terminal result permits the controller to advance. Identity, path or duplicate-execution uncertainty is a safety block. Ordinary missing fields, counts or business-quality gaps are review/failed outcomes under the selected mode, not a reason to fabricate lifecycle success.
