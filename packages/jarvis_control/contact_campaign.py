"""Issue an isolated secondary-research campaign through Jarvis, without DB writes."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import sys

from jarvis_control.file_task import FileTaskError, json_bytes, sha, safe_path, strict_json
from jarvis_control.contact_task import save
from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_schema import NAME, VERSION


def _prepare(root: Path, allocation_path: Path, config_path: Path):
    root = root.resolve()
    allocation = strict_json(allocation_path.read_bytes())
    cfg = strict_json(config_path.read_bytes())
    if allocation.get("schema") != "jarvis-dot-secondary-allocation/v1" or allocation.get("secondary_research") is not True or allocation.get("production_write") is not False:
        raise FileTaskError("only independent secondary-research campaigns are supported")
    rows = allocation["assignments"]
    if len(rows) != 100 or len({r["company_id"] for r in rows}) != 100 or len({r["official_domain"] for r in rows}) != 100:
        raise FileTaskError("100 unique companies/domains required")
    expected = {(f"seat-{s:02}", r) for s in range(1, 11) for r in range(1, 11)}
    if {(r["seat_id"], r["round"]) for r in rows} != expected:
        raise FileTaskError("exact ten-seat ten-company allocation required")
    for path, digest in cfg["contact_source_hashes"].items():
        if sha(Path(path).read_bytes()) != digest: raise FileTaskError("original skill changed")
    os.environ["COOPER_CONTACT_V2_CONTRACT"] = cfg["contact_v2_contract"]
    sys.path.insert(0, str(Path(cfg["contact_skill_dir"]) / "scripts"))
    v24 = importlib.import_module("v24")
    run_id = allocation["run_id"]
    ledger_path = root / "validation-campaign.json"
    if ledger_path.exists(): raise FileTaskError("campaign already issued; inspect existing state instead of reissuing")
    # This file is the new authoritative validation workflow state. These tags
    # are not represented as production tags or claims in either source DB.
    ledger = {"schema": "jarvis-secondary-campaign/v1", "module": NAME, "schema_version": VERSION, "run_id": run_id,
              "source_library_id": allocation["source_library_id"], "source_snapshot_sha256": allocation["source_snapshot_sha256"],
              "authorization_message_ids": allocation["authorization_message_ids"], "secondary_research": True,
              "allocation_sha256": sha(allocation_path.read_bytes()), "production_write": False,
              "issued_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "workflows": []}
    work = []
    for row in rows:
        if row["eligible"] is not True or row["no_go"] is not False or row["secondary_research"] is not True:
            raise FileTaskError("ineligible row in validation allocation")
        company_id = row["company_id"]
        workspace = safe_path(root, f"companies/{company_id}")
        if workspace.exists(): raise FileTaskError("company workspace already occupied")
        tag = "wft_secondary_" + run_id + "_" + company_id
        item = v24.V2.seal_work_item({"schema_version": "contact-mining-work-item/v1", "work_item_id": "cmwi_" + run_id + "_" + company_id,
            "entity": {"entity_type": "company", "entity_id": company_id, "company_name": row["company_name"],
                       "official_website": row["website"], "normalized_domain": row["official_domain"]},
            "workflow_binding": {"stage": "contact_mining", "status": "pending", "revision": 1, "current_tag_id": tag},
            "rating_context": {"rating": row["rating"], "rating_result_id": "snapshot:" + allocation["source_snapshot_sha256"] + ":" + company_id,
                               "authorized_for_contact_mining": True},
            "requested_depth": "deep", "research_context": {"country_code": row["country_code"], "search_language_mode": row["search_language_mode"],
                "bizseek_enabled": row["bizseek_enabled"], "source_people": [], "company_clues": [], "baseline_routes": []},
            "issued_at": ledger["issued_at"], "assignment_ref": "secondary-research:" + run_id})
        errors = v24.V2.validate_work_item(item)
        if errors: raise FileTaskError("original work-item contract rejected: " + "; ".join(errors))
        ledger["workflows"].append({"company_id": company_id, "source_company_id": row["source_company_id"], "official_domain": row["official_domain"], "current_tag_id": tag, "revision": 1, "status": "pending",
            "seat_id": row["seat_id"], "round": row["round"], "work_item_sha256": item["input_sha256"], "history_preserved": True})
        work.append((row, workspace, item))
    save(ledger_path, ledger)
    with (root / "validation-events.jsonl").open("a") as events:
        for row in ledger["workflows"]:
            events.write(json.dumps({"event": "validation_workflow_issued", "run_id": run_id, "issued_at": ledger["issued_at"], **row}, ensure_ascii=False) + "\n")
        events.flush(); os.fsync(events.fileno())
    if strict_json(ledger_path.read_bytes()) != ledger: raise FileTaskError("validation registry readback mismatch")
    for row, workspace, item in work:
        workspace.mkdir(parents=True, mode=0o700)
        assignment = workspace / "assignment.json"
        save(assignment, item)
        receipt = v24.prepare(assignment, workspace, run_id, str(workspace / "candidate-export.json"))
        save(workspace / "prepare-receipt.json", receipt)
        packet = {"schema": "jarvis-dot-contact/v1", "run_id": run_id, "seat_id": row["seat_id"], "round": row["round"],
            "company_workspace": str(workspace.relative_to(root)), "task_sha256": sha((workspace / "task.json").read_bytes()),
            "binding_sha256": sha((workspace / "binding.json").read_bytes()), "assignment_sha256": sha(assignment.read_bytes()),
            "allocation_file": str(allocation_path.relative_to(root)), "allocation_sha256": sha(allocation_path.read_bytes()),
            "launcher_config": str(config_path.relative_to(root)), "official_domain": row["official_domain"], "previous_receipt": None}
        save(root / f"{row['seat_id']}-r{row['round']:02}.packet.json", packet)
    return {"status": "prepared", "run_id": run_id, "company_count": len(work), "workflow_state": str(ledger_path),
            "model_turns_started": 0, "production_written": False, "history_overwritten": False}


def prepare(root, allocation_path, config_path):
    with ProcessLock(root / ".validation-campaign.lock", timeout_seconds=1, stale_seconds=86400):
        return _prepare(root, allocation_path, config_path)


def status(root):
    ledger = strict_json((root / "validation-campaign.json").read_bytes())
    return {"status": "prepared", "module": NAME, "schema_version": VERSION, "run_id": ledger["run_id"],
            "company_count": len(ledger["workflows"]), "secondary_research": True, "production_written": False,
            "allocation_sha256": ledger["allocation_sha256"], "workflows": ledger["workflows"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "status"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--allocation", type=Path)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    print(json.dumps(status(args.root) if args.action == "status" else prepare(args.root, args.allocation, args.config), ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
