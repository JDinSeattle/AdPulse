"""Actual collector→Kafka→Flink→ClickHouse/S3 correctness acceptance."""
from __future__ import annotations

import argparse
import os
import tempfile
import time
from contextlib import closing
from pathlib import Path

import requests

from adpulse import releases
from adpulse.archive import S3Objects, reconcile, verified_records
from adpulse.archive_index import ArchiveIndex
from adpulse.cli import archived_dimensions
from adpulse.common import canonical, digest, load_rules, now_ms, write_json
from adpulse.generator import generate, save_dataset, send
from adpulse.oracle import calculate, compare
from adpulse.storage import ClickHouse


def wait_until(check, timeout=180, label="condition"):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            last = check()
            if last:
                return last
        except (requests.RequestException, ConnectionError):
            pass
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {label}: {last}")


def jobs():
    response = requests.get("http://localhost:18081/jobs/overview", timeout=10)
    response.raise_for_status()
    return [j for j in response.json()["jobs"] if j["state"] not in {"CANCELED", "FINISHED", "FAILED"}]


def checkpoints():
    return {job["jid"]: requests.get(f"http://localhost:18081/jobs/{job['jid']}/checkpoints", timeout=10).json() for job in jobs()}


def _memory_reconcile_live(expected_receipts=(), timeout=180):
    db, objects = ClickHouse(), S3Objects()
    latest = {}

    def check():
        latest.clear()
        rows = verified_records(objects)
        accepted = db.query("SELECT arrayJoin(receipt_ids) AS receipt_id FROM adpulse.receipts FINAL FORMAT JSONEachRow")
        acknowledged = {r["receipt_id"] for r in accepted} | set(expected_receipts)
        lineage = db.query("SELECT receipt_id,disposition FROM adpulse.quality FINAL WHERE disposition!='signal' FORMAT JSONEachRow")
        coverage = reconcile(rows, lineage, acknowledged)
        if not coverage["archive_complete"] or not coverage["lineage_complete"]:
            latest.update(coverage)
            return False
        packets = [r["value"] for r in rows if r["topic"].endswith(".raw") and r["value"]["receipt_id"] in acknowledged]
        packets.sort(key=lambda p: (p["received_at"], p["batch_id"], p["index"]))
        expected = calculate(packets, release_id="live-v1", dimensions=archived_dimensions(rows))
        actual = db.snapshots("live-v1")
        comparison = compare(expected, actual)
        latest.update(comparison, completeness=coverage, acknowledged=len(acknowledged),
                      expected_metrics=len(expected["metrics"]), actual_metrics=len(actual["metrics"]),
                      expected_associations=len(expected["associations"]), actual_associations=len(actual["associations"]))
        return latest if comparison["passed"] else False

    try:
        return wait_until(check, timeout=timeout, label="full business reconciliation")
    except TimeoutError as exc:
        write_json("artifacts/integration/last-failure.json", latest)
        raise AssertionError(f"Live reconciliation failed; see artifacts/integration/last-failure.json: {canonical(latest)[:1800]}") from exc


def reconcile_live(expected_receipts=(), timeout=180):
    if os.getenv("ADPULSE_REFERENCE_BACKEND", "disk") == "memory":
        return _memory_reconcile_live(expected_receipts, timeout)
    from adpulse.reconciliation import run as disk_reconcile
    root = Path("artifacts/integration/disk-reference")
    root.mkdir(parents=True, exist_ok=True)
    latest = {}

    def check():
        with tempfile.TemporaryDirectory(prefix="attempt-", dir=root) as work:
            result = disk_reconcile(root / "archive.sqlite", Path(work) / "oracle.sqlite",
                                    expected_receipts=expected_receipts)
        latest.clear()
        latest.update(result)
        return result if result["passed"] else False

    try:
        return wait_until(check, timeout=timeout, label="disk-backed full business reconciliation")
    except TimeoutError as exc:
        write_json("artifacts/integration/last-failure.json", latest)
        raise AssertionError("Live reconciliation failed; see artifacts/integration/last-failure.json") from exc


def dimensions_ready():
    with closing(ArchiveIndex("artifacts/integration/disk-reference/archive.sqlite")) as index:
        index.refresh(S3Objects())
        return len(index.dimensions()) >= 4


def run(scenario="mixed", users=200, output="artifacts/integration"):
    root = Path(output)
    wait_until(lambda: len(jobs()) == 2 and all(j["state"] == "RUNNING" for j in jobs()), label="two running Flink jobs")
    wait_until(dimensions_ready, label="CDC initial snapshot archived")
    seed = now_ms() % 1_000_000_000
    dataset = generate(users=users, seed=seed, start_ms=now_ms() - users * 1000, scenario=scenario)
    save_dataset(dataset, root / "dataset")
    start = time.monotonic()
    acknowledgements = send(dataset["transport"], "http://localhost:8088", batch_prefix=f"integration-{seed}")
    write_json(root / "acknowledgements.json", acknowledgements)
    receipt_ids = [receipt for ack in acknowledgements for receipt in ack["receipt_ids"]]
    report = reconcile_live(receipt_ids)
    checkpoint_report = wait_until(lambda: (c if len(c := checkpoints()) == 2 and all(v.get("counts", {}).get("completed", 0) > 0 for v in c.values()) else False), label="committed checkpoints")
    report.update(mode="docker-kafka-flink-clickhouse-s3", scenario=scenario,
                  submitted_records=len(receipt_ids), convergence_seconds=round(time.monotonic() - start, 3),
                  checkpoint_counts={k: v["counts"] for k, v in checkpoint_report.items()},
                  archive_complete=True, sink_complete=True, rules_sha256=digest(load_rules()))
    with releases.connect() as db:
        row = db.execute("SELECT status FROM releases WHERE release_id='live-v1'").fetchone()
    if row["status"] == "building":
        releases.mark_validated("live-v1", report)
        releases.activate("live-v1", "initial real-stack oracle and archive acceptance")
    write_json(root / "report.json", report)
    print(canonical(report), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", default="mixed")
    parser.add_argument("--users", type=int, default=200)
    parser.add_argument("--output", default="artifacts/integration")
    args = parser.parse_args()
    run(args.scenario, args.users, args.output)
