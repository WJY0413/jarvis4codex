# Bounded secondary-research contact workflow

```sh
python -m jarvis_control.contact_campaign prepare --root <run> --allocation <allocation.json> --config <launcher.json>
python -m jarvis_control.contact_campaign status --root <run>
python -m jarvis_control.contact_task run --packet <company-packet.json>
python -m jarvis_control.contact_task status --packet <same-company-packet.json>
```

This specialized adapter uses one fixed thread per seat and one assigned company per turn, bounded to ten seats and ten companies per seat. All packets must share one run root because capacity and seat locks are scoped there. `prepare` writes a real independent validation registry, new workflow tags/revisions, append-only creation events and original V2.5 work items. It does not acquire or fabricate a shared production lease. Source company IDs remain traceable through the explicit namespace mapping; original databases and history are unchanged.

The allocation declares secondary research, no production write, unique company/domain/seat-round assignments, immutable source and authorization references, and preserved exclusions. Historical research may be allowed by the caller without sending old contact answers to the worker. The observed run supplied empty prior-person and prior-route fields intentionally; output counts do not mean newly discovered production leads.

## Original form and receiver

The deployment supplies and hash-pins its approved original V2.5 `ContactResearchForm`, receiver/compiler and original V2 contract. Those business-skill sources and customer data are not bundled with this core source candidate. Configure `contact_skill_dir`, `contact_v2_contract` and `contact_source_hashes` before preparation. Do not point these fields at model-supplied code.

Because the model has no file environment, the prepared `task.json` is delivered inline with its hash. The original stable `submit_contact_research(workspace, research)` form is exposed through official dynamicTools. The host checks the workspace string against the exact current thread/turn/company binding and invokes only its own fixed path. The model has no arbitrary path, command or production-write authority.

Server requests are separated from ordinary RPC responses. The native turn identity is bound before any queued form is handled. A durable call intent precedes the sole actual receiver call. An exact duplicate can replay the saved real response; changed, late, cross-company or uncertain calls cannot re-execute the receiver. Original business/form rejections remain failed company outcomes. Other receiver exceptions remain uncertain and are not fabricated as receipts.

## Continue and review independently

After known native completion and an actual handled submission outcome, the controller records the exact handoff and advances the same thread to the next different assigned company. Successful and explicitly rejected submissions both advance under the caller's policy; the failed company is not retried or relabeled. Independent original `accept` is run outside the model. Source review is separate from structural acceptance and production admission.

The next packet binds the previous receipt and handoff hashes. Unknown submission/write outcomes, authentication, permission or quota blocks pause for reconciliation. The narrow verified no-submission disposition is described in [JARVIS_DOT.md](JARVIS_DOT.md); it never invents a receiver receipt.

At the finite allocation limit the seat stops. Neither model nor adapter chooses an eleventh company. The operator retains each original outcome, including rejected forms and source limitations.

## Known preparation limitation

The current campaign `status` reports registry issuance, not a crash-safe per-company preparation audit. If `prepare` is interrupted, independently verify every expected task/packet/prepare receipt before dispatch; do not treat a count in the registry as proof that all files were prepared. The observed 100-company preparation returned successfully before model work began.
