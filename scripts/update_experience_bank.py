"""Apply a prepared AdPulse-only bank update with backups and optimistic concurrency checks.

Writes two files with atomic replacement per file, not a two-file filesystem transaction.
The journal and pre-write checks make interruption/concurrent-edit boundaries explicit.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ID = "project-local-adpulse"
NAMES = ("experience_bank.json", "project_evidence_bank.json")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def check_unchanged(paths, expected):
    changed = [str(p) for p in paths if sha(p.read_bytes()) != expected[p.name]]
    if changed:
        raise RuntimeError("Concurrent bank modification detected: " + ", ".join(changed))


def durable_write(path, data, mode=0o600):
    with path.open("xb") as handle:
        os.fchmod(handle.fileno(), mode)
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def encode(data):
    return (json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()


def merge(experience, evidence, addition):
    result, proofs = copy.deepcopy(experience), copy.deepcopy(evidence)
    if addition["experience"]["id"] != PROJECT_ID:
        raise ValueError("This updater is scoped to AdPulse")
    own = [entry for entry in experience["experiences"] if entry["id"] == PROJECT_ID]
    prior_proofs = [record for record in evidence["evidence"] if record["project_id"] == PROJECT_ID]
    replacing = PROJECT_ID in evidence["projects"] or bool(own)
    if replacing:
        if len(own) != 1 or PROJECT_ID not in evidence["projects"]:
            raise ValueError("Incomplete existing AdPulse identity")
        current = {"experience": own[0], "project": evidence["projects"][PROJECT_ID], "evidence": prior_proofs}
        if addition.get("base_project_sha256") != sha(encode(current)):
            raise ValueError("Concurrent AdPulse modification or missing base hash; rebuild the candidate")
        for key in ("metrics", "bullet_candidates"):
            if not {x["id"] for x in own[0].get(key, [])} <= {x["id"] for x in addition["experience"].get(key, [])}:
                raise ValueError("Stable metric/bullet IDs must be preserved")
        if not {r["id"] for r in prior_proofs} <= {r["id"] for r in addition["evidence"]}:
            raise ValueError("Stable evidence IDs must be preserved")
    elif any("adpulse" in (e["id"] + e.get("title", "")).lower() for e in experience["experiences"]):
        raise ValueError("AdPulse exists under another ID; reconcile identity first")
    existing = {record["id"] for record in proofs["evidence"] if record["project_id"] != PROJECT_ID}
    for record in addition["evidence"]:
        if record["project_id"] != PROJECT_ID or record["id"] in existing:
            raise ValueError("Evidence ownership or ID conflict")
        existing.add(record["id"])
    if replacing:
        result["experiences"] = [addition["experience"] if entry["id"] == PROJECT_ID else entry
                                 for entry in result["experiences"]]
    else:
        result["experiences"].append(addition["experience"])
    proofs["projects"][PROJECT_ID] = addition["project"]
    updated_by_id = {record["id"]: record for record in addition["evidence"]}
    prior_ids = {record["id"] for record in proofs["evidence"]}
    proofs["evidence"] = [updated_by_id[record["id"]] if record["project_id"] == PROJECT_ID else record
                          for record in proofs["evidence"]]
    proofs["evidence"].extend(record for record in addition["evidence"] if record["id"] not in prior_ids)
    proofs["updated_at"] = datetime.now(timezone.utc).isoformat()
    return result, proofs


def verify_sources(addition, project_root):
    for record in addition["evidence"]:
        path = (project_root / record["local_path"]).resolve()
        if not path.is_relative_to(project_root) or sha(path.read_bytes()) != record["sha256"]:
            raise ValueError("Evidence source changed or escaped project: " + record["id"])
    for metric in addition["experience"]["metrics"]:
        for point in (metric, metric.get("baseline", {})):
            source = point.get("source") or {}
            if source.get("report_path"):
                path = (project_root / source["report_path"]).resolve()
                if not path.is_relative_to(project_root):
                    raise ValueError("Metric report escaped project")
                value = json.loads(path.read_text())
                for component in source["json_pointer"].strip("/").split("/"):
                    value = value[int(component)] if isinstance(value, list) else value[component]
                if value != point["value"]:
                    raise ValueError("Metric differs from report: " + metric["id"])


def apply(bank_root, project_root, candidate, initial_snapshot, report_path):
    spec = importlib.util.spec_from_file_location("bank_validator", bank_root / "scripts/validate_banks.py")
    validator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validator)
    addition = validator.load_json(candidate)
    verify_sources(addition, project_root)
    paths = [bank_root / "data" / name for name in NAMES]
    with (bank_root / ".bank-update.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        before_bytes = {p.name: p.read_bytes() for p in paths}
        before_hash = {name: sha(data) for name, data in before_bytes.items()}
        original = [validator.load_json(p) for p in paths]
        errors = validator.validate(*original)
        if errors:
            raise ValueError(errors)
        updated = merge(*original, addition)
        errors = validator.validate(*updated, root=project_root, project_id=PROJECT_ID)
        if errors:
            raise ValueError(errors)
        check_unchanged(paths, before_hash)
        backup = bank_root / ".bank-backups" / ("adpulse-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
        backup.mkdir(parents=True)
        for name in NAMES:
            durable_write(backup / name, before_bytes[name])
        initial = json.loads(initial_snapshot.read_text())
        report = dict(project_id=PROJECT_ID, schema_version=updated[1]["schema_version"], backup_directory=str(backup),
                      before_sha256=before_hash, initial_read_changed={name: initial[name]["sha256"] != before_hash[name] for name in NAMES},
                      concurrent_strategy="cooperative flock plus optimistic hash rechecks; rebase on latest non-AdPulse entries",
                      transaction_boundary="atomic per file; two-file interruption recoverable using journal/backups",
                      phase="prepared", evidence_sources_verified=len(addition["evidence"]))
        durable_write(backup / "journal.json", encode(report))
        temporary, written = [], []
        try:
            data = {p.name: encode(value) for p, value in zip(paths, updated)}
            for p in paths:
                temp = p.with_name("." + p.name + "." + uuid.uuid4().hex + ".tmp")
                durable_write(temp, data[p.name], stat.S_IMODE(p.stat().st_mode))
                temporary.append(temp)
            check_unchanged(paths, before_hash)
            for p, temp in zip(paths, temporary):
                # Check both files again between replacements, preserving another writer's edits.
                expected = {name: sha(data[name]) if name in written else before_hash[name] for name in NAMES}
                check_unchanged(paths, expected)
                os.replace(temp, p)
                written.append(p.name)
            directory = os.open(paths[0].parent, os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            reread = [validator.load_json(p) for p in paths]
            errors = validator.validate(*reread, root=project_root, project_id=PROJECT_ID)
            if errors or reread != list(updated):
                raise RuntimeError("Post-write validation/concurrency check failed: " + str(errors))
            other_experiences = [e for e in original[0]["experiences"] if e["id"] != PROJECT_ID]
            other_evidence = [e for e in original[1]["evidence"] if e["project_id"] != PROJECT_ID]
            assert [e for e in reread[0]["experiences"] if e["id"] != PROJECT_ID] == other_experiences
            assert ({k: v for k, v in reread[1]["projects"].items() if k != PROJECT_ID}
                    == {k: v for k, v in original[1]["projects"].items() if k != PROJECT_ID})
            assert [e for e in reread[1]["evidence"] if e["project_id"] != PROJECT_ID] == other_evidence
            verify_sources(addition, project_root)
            report.update(phase="committed", passed=True, after_sha256={name: sha(data[name]) for name in NAMES},
                          other_experiences_preserved=len(other_experiences),
                          other_evidence_preserved=len(other_evidence),
                          experiences_after=len(reread[0]["experiences"]), evidence_after=len(reread[1]["evidence"]),
                          metric_references_verified=len(addition["experience"]["metrics"]), validation_errors=[])
            durable_write(backup / "committed.json", encode(report))
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_bytes(encode(report))
            return report
        except Exception:
            # Do not blindly roll back across another writer. Retain the pair and journal for recovery.
            report.update(phase="interrupted", replaced_files=written, passed=False)
            durable_write(backup / "interrupted.json", encode(report))
            raise
        finally:
            for temp in temporary:
                temp.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bank-root", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--initial-snapshot", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=Path("docs/evidence/refresh/bank-update.json"))
    args = parser.parse_args()
    print(json.dumps(apply(args.bank_root.resolve(), Path(__file__).resolve().parents[1], args.candidate,
                           args.initial_snapshot, args.report), ensure_ascii=False))
