---
name: jarvis-loop-prompt-builder
description: Build non-duplicative startup and continuation prompts from the actual 0.3.0 Loop binding.
---

# Jarvis Loop Prompt Builder (0.3.0 candidate)

Before launch, read the business Skill and selected `jarvis_loop` preflight contract. Return explicit `prompt` and shorter `continue_prompt`; preserve user model, limits, evidence and stop rules. The Loop supplies slot, candidate IDs, lane, output boundary and receipt identity. Do not paste current company IDs or accumulated history into reusable prompts. A context-recovered Worker must have one resolvable worker-rule entrypoint.

Choose output wording from `result_verification.mode`: `file_receipt` writes the bound result/receipt pair; `final_answer_json` ends with only JSON and lets Holder save evidence. If using a registered schema, refer to its bound format but do not ask the Worker to choose a version or modify the hash. The format check does not replace business acceptance. Verify the effective prompt after real authorized dispatch; this Skill alone sends nothing.
