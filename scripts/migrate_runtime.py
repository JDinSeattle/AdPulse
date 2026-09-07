"""Cold-clone ClickHouse and strictly restore BOTH Flink jobs; retain rollback assets.

Run from the repository root after building the target JAR/image. This interrupts
the local stack. Failures deliberately leave ingress stopped; inspect the journal
before recovery. Never downgrade an already-upgraded database volume in place.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

import requests

from adpulse.common import write_json
from adpulse.storage import ClickHouse
from integration import jobs, reconcile_live, run, wait_until

COMPOSE = ["docker", "compose", "-f", "deployment/compose.yaml"]
TABLES = ("results", "deliveries", "quality", "receipts", "visibility")


def command(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=600).stdout


def fingerprints(db):
    """SHA-256 of sorted logical rows; ignore nondeterministic insertion time.

    ReplacingMergeTree is read with FINAL; visibility is an append-only multiset.
    Keep every business field and every occurrence, including retry duplicates.
    This acceptance helper is deliberately exhaustive, not the serving API.
    """
    result = {}
    for table in TABLES:
        fields = "* EXCEPT(inserted_at)" if table in {"results", "quality"} else "*"
        final = "" if table == "visibility" else " FINAL"
        response = db.session.post(db.url, auth=db.auth,
                                   params={"output_format_json_quote_64bit_integers": 0},
                                   data=f"SELECT {fields} FROM adpulse.{table}{final} FORMAT JSONEachRow",
                                   timeout=120)
        response.raise_for_status()
        rows = sorted(response.content.splitlines())
        result[table] = {"logical_rows": len(rows), "sha256": hashlib.sha256(b"\n".join(rows)).hexdigest()}
    return result


def save_and_cancel(job, root):
    base = "http://localhost:18081"
    detail = requests.get(f"{base}/jobs/{job['jid']}", timeout=10).json()
    parallelism = max(v["parallelism"] for v in detail["vertices"])
    trigger = requests.post(f"{base}/jobs/{job['jid']}/savepoints",
                            json={"target-directory": "file:///opt/flink/state/savepoints", "cancel-job": True}, timeout=30)
    trigger.raise_for_status()
    request_id = trigger.json()["request-id"]

    def saved():
        response = requests.get(f"{base}/jobs/{job['jid']}/savepoints/{request_id}", timeout=10)
        response.raise_for_status()
        value = response.json()
        if value["status"]["id"] != "COMPLETED":
            return False
        if "failure-cause" in value.get("operation", {}):
            raise RuntimeError(value["operation"])
        return value["operation"]["location"]

    location = wait_until(saved, timeout=300, label="canonical savepoint and cancellation")
    proof = dict(job_id=job["jid"], name=job["name"], parallelism=parallelism, savepoint=location)
    write_json(root / f"savepoint-{job['jid']}.json", proof)
    return proof


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-volume", default="adpulse_clickhouse-data")
    parser.add_argument("--target-volume", default="adpulse_clickhouse-data-v26")
    parser.add_argument("--output", default="artifacts/operations/migration")
    args = parser.parse_args()
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=False)
    if args.source_volume == args.target_volume:
        raise ValueError("A distinct cold-clone volume is mandatory")
    # Verify source exists and target is new before interrupting anything.
    command("docker", "volume", "inspect", args.source_volume)
    if subprocess.run(["docker", "volume", "inspect", args.target_volume], capture_output=True).returncode == 0:
        raise ValueError("Target volume already exists; refusing to overwrite it")
    config = json.loads(command(*COMPOSE, "config", "--format", "json"))
    if config["volumes"]["clickhouse-data"]["name"] != args.target_volume:
        raise ValueError("Compose volume does not match the requested target")
    container_id = command(*COMPOSE, "ps", "-q", "clickhouse").strip()
    if not container_id:
        raise ValueError("Source ClickHouse must be running for a validated cold-copy boundary")
    mounted = json.loads(command("docker", "inspect", container_id))[0]["Mounts"]
    actual_source = next((m.get("Name") for m in mounted if m["Destination"] == "/var/lib/clickhouse"), None)
    if actual_source != args.source_volume:
        raise ValueError("Requested source is not the running ClickHouse data volume")
    current = jobs()
    if len(current) != 2 or any(j["state"] != "RUNNING" for j in current):
        raise ValueError("Expected exactly two healthy source jobs")
    report = {"passed": False, "environment": "local-docker-single-host", "started_at_epoch": time.time(),
              "source_volume_retained": args.source_volume, "target_volume": args.target_volume,
              "from_flink": requests.get("http://localhost:18081/overview", timeout=10).json()["flink-version"],
              "from_clickhouse": ClickHouse().query("SELECT version() AS version FORMAT JSONEachRow")[0]["version"],
              "savepoints": []}

    def journal(phase):
        report["phase"] = phase
        write_json(root / "report.json", report)
        print(phase, flush=True)

    journal("preflight")
    try:
        command(*COMPOSE, "stop", "collector")
        report["before_reconciliation"] = reconcile_live(timeout=300)
        journal("ingress-paused-and-reconciled")
        # Clean first, attribution second: downstream may consume committed clean output.
        for job in sorted(current, key=lambda j: "attribution" in j["name"].lower()):
            report["savepoints"].append(save_and_cancel(job, root))
            journal("saved-" + job["jid"])
        command(*COMPOSE, "stop", "sink", "api")
        report["before_tables"] = fingerprints(ClickHouse())
        command(*COMPOSE, "stop", "clickhouse", "taskmanager-1", "taskmanager-2", "jobmanager")
        command("docker", "volume", "create", args.target_volume)
        command("docker", "run", "--rm", "--user", "root", "--entrypoint", "sh",
                "-v", f"{args.source_volume}:/source:ro", "-v", f"{args.target_volume}:/target",
                "flink:1.20.5-scala_2.12-java17", "-c", "cp -a /source/. /target/")
        journal("cold-copy-complete-source-untouched")
        command(*COMPOSE, "up", "-d", "--no-deps", "--wait", "clickhouse", "jobmanager", "taskmanager-1", "taskmanager-2")
        report["after_tables"] = fingerprints(ClickHouse())
        report["table_fingerprints_equal"] = report["before_tables"] == report["after_tables"]
        if not report["table_fingerprints_equal"]:
            raise AssertionError("Historical table fingerprints changed")
        journal("target-database-verified")
        report["restores"] = []
        for saved in report["savepoints"]:
            entry = "io.adpulse.AttributionJob" if "attribution" in saved["name"].lower() else "io.adpulse.CleanJob"
            output = root / (entry.rsplit(".", 1)[-1] + "-restore.json")
            command(sys.executable, "scripts/restore_job.py", "--job-id", saved["job_id"], "--entry-class", entry,
                    "--state-path", saved["savepoint"], "--parallelism", str(saved["parallelism"]), "--output", str(output))
            proof = json.loads(output.read_text())
            if not proof["restored_checkpoint"] or proof["allow_non_restored_state"]:
                raise AssertionError("Strict restored-state evidence missing")
            report["restores"].append(proof)
            journal("restored-" + entry)
        command(*COMPOSE, "start", "sink", "api", "collector")
        wait_until(lambda: requests.get("http://localhost:8088/health", timeout=5).ok, label="collector restored")
        report["after_reconciliation"] = run(users=10, output=str(root / "integration"))
        report["to_flink"] = requests.get("http://localhost:18081/overview", timeout=10).json()["flink-version"]
        report["to_clickhouse"] = ClickHouse().query("SELECT version() AS version FORMAT JSONEachRow")[0]["version"]
        report.update(passed=True, completed_at_epoch=time.time())
        journal("validated")
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        journal("failed-inspect-before-resuming")
        raise


if __name__ == "__main__":
    main()
