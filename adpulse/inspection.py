"""Background inspection and atomic, explicitly dated read models for HTTP.

Metrics and archive coverage run in separate processes so cold archive indexing
cannot block metrics refresh. Each role has one writer, no HTTP-triggered work.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
import uuid
from contextlib import closing
from pathlib import Path

import requests
from prometheus_client import CollectorRegistry, Gauge, generate_latest

from . import releases
from .archive import S3Objects
from .archive_index import ArchiveIndex
from .common import canonical, now_ms
from .storage import ClickHouse

MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
OUTSTANDING_SQL = """SELECT min(received_at) AS oldest, count() AS n FROM
  (SELECT arrayJoin(receipt_ids) AS receipt_id,received_at FROM adpulse.receipts FINAL) a
  LEFT ANTI JOIN (SELECT receipt_id FROM adpulse.quality FINAL WHERE disposition!='signal') q
  ON a.receipt_id=q.receipt_id
  SETTINGS join_algorithm='grace_hash',grace_hash_join_initial_buckets=16,
           max_bytes_in_join=67108864,max_temporary_data_on_disk_size_for_query=4294967296
  FORMAT JSONEachRow"""


class SnapshotUnavailable(RuntimeError):
    pass


def directory():
    return Path(os.getenv("ADPULSE_INSPECTION_DIR", "artifacts/inspection"))


def read_snapshot(kind, *, max_age, root=None, clock=time.time):
    if kind not in {"coverage", "metrics"}:
        raise ValueError("Unknown inspection kind")
    try:
        with ((root or directory()) / (kind + ".json")).open("rb") as handle:
            data = handle.read(MAX_SNAPSHOT_BYTES + 1)
        if len(data) > MAX_SNAPSHOT_BYTES:
            raise ValueError("snapshot size")
        result = json.loads(data)
        age = clock() - result["generated_at"]
        if result["schema_version"] != 1 or result["kind"] != kind or result["status"] != "ok":
            raise ValueError("last inspection failed")
        if age < -5 or age > max_age:
            raise ValueError("snapshot stale")
        return result
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SnapshotUnavailable("Inspection unavailable or expired") from exc


def publish(root, kind, payload, *, started_at, error=None):
    root.mkdir(parents=True, exist_ok=True)
    result = dict(schema_version=1, kind=kind, status="error" if error else "ok",
                  generated_at=time.time(), started_at=started_at,
                  duration_seconds=time.time()-started_at, payload=payload if error is None else None,
                  error_type=type(error).__name__ if error else None)
    data = (canonical(result) + "\n").encode()
    if len(data) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Inspection snapshot exceeds byte budget")
    target = root / (kind + ".json")
    temp = root / ("." + kind + "." + uuid.uuid4().hex)
    try:
        with temp.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
    finally:
        temp.unlink(missing_ok=True)
    return result


def inspect_archive(root, *, db=None, objects=None):
    db, objects = db or ClickHouse(), objects or S3Objects()
    with closing(ArchiveIndex(root / "archive.sqlite")) as index:
        refreshed = index.refresh(objects)
        expected = (r["receipt_id"] for r in db.spooled_query("SELECT arrayJoin(receipt_ids) AS receipt_id FROM adpulse.receipts FINAL FORMAT JSONEachRow", directory=root))
        lineage = db.spooled_query("SELECT receipt_id,disposition,payload FROM adpulse.quality FINAL FORMAT JSONEachRow", directory=root)
        completeness = index.coverage(expected, lineage)
        quality_summary = list(db.iter_query("SELECT disposition,error_code,rule_version,count() AS records FROM adpulse.quality FINAL GROUP BY disposition,error_code,rule_version LIMIT 1001 FORMAT JSONEachRow"))
        if len(quality_summary) > 1000:
            raise ValueError("Quality summary exceeds category budget")
        samples = list(db.iter_query("SELECT payload FROM adpulse.quality FINAL WHERE disposition!='cleaned' ORDER BY inserted_at DESC LIMIT 50 FORMAT JSONEachRow"))
        return dict(**completeness, index=refreshed,
                    quality_summary=quality_summary, quality_samples=[json.loads(r["payload"]) for r in samples],
                    consistency="separately sampled sources; incremental index of previously checksum-verified immutable objects")


def operational_metrics():
    registry, db = CollectorRegistry(), ClickHouse()
    response = requests.get(os.getenv("FLINK_REST_URL", "http://jobmanager:8081") + "/jobs/overview", timeout=3)
    response.raise_for_status()
    running = sum(j["state"] == "RUNNING" and j["name"].startswith("AdPulse") for j in response.json()["jobs"])
    Gauge("adpulse_flink_running_jobs", "Expected two running AdPulse jobs", registry=registry).set(running)
    selected = releases.active()
    if selected:
        business = Gauge("adpulse_business_value", "Latest logical values at inspection time",
                         ["release", "cohort", "variant", "currency", "measure", "campaign", "region", "app_version"], registry=registry)
        totals = {}
        for result in db.iter_query("""SELECT argMax(payload,output_offset) AS payload FROM adpulse.results
            WHERE release_id={release:String} AND record_type='metric' GROUP BY output_key FORMAT JSONEachRow""", {"release": selected}):
            row = json.loads(result["payload"])
            for measure, value in row["values"].items():
                key = (selected, row["cohort_basis"], row["variant"], row["currency"], measure,
                       row["campaign_id"], row["region"], row["app_version"])
                if key not in totals and len(totals) >= 10000:
                    raise ValueError("Business metric cardinality exceeds 10000 series")
                totals[key] = totals.get(key, 0) + value
        for key, value in totals.items():
            business.labels(*key).set(value)
        Gauge("adpulse_active_release_info", "Active release at inspection time", ["release"], registry=registry).labels(selected).set(1)
        pending = next(db.iter_query("""SELECT countIf(status='pending') AS n FROM
            (SELECT argMax(JSONExtractString(payload,'status'),output_offset) AS status
             FROM adpulse.results WHERE release_id={release:String} AND record_type='association'
             GROUP BY output_key) FORMAT JSONEachRow""", {"release": selected}))
        Gauge("adpulse_pending_conversions", "Conversions awaiting click", registry=registry).set(pending["n"])
        freshness = next(db.iter_query("""SELECT count() AS records, quantileExact(0.95)(lag) AS p95 FROM
            (SELECT receipt_id, (toUnixTimestamp64Milli(min(visible_at))-min(received_at))/1000.0 AS lag
             FROM adpulse.visibility WHERE release_id={release:String} AND received_at >= {since:UInt64}
             GROUP BY receipt_id) FORMAT JSONEachRow""", {"release": selected, "since": max(0, now_ms()-300000)}))
        if freshness["records"]:
            Gauge("adpulse_freshness_p95_seconds", "Per-receipt acceptance to first visible metric P95", registry=registry).set(freshness["p95"])
    quality = Gauge("adpulse_quality_records", "Logical quality records", ["disposition", "error_code"], registry=registry)
    for number, row in enumerate(db.iter_query("SELECT disposition,error_code,count() AS n FROM adpulse.quality FINAL GROUP BY disposition,error_code FORMAT JSONEachRow")):
        if number >= 1000:
            raise ValueError("Quality metric cardinality exceeds 1000 series")
        quality.labels(row["disposition"], row["error_code"]).set(row["n"])
    accepted = next(db.iter_query("SELECT sum(length(receipt_ids)) AS n FROM adpulse.receipts FINAL FORMAT JSONEachRow"))
    Gauge("adpulse_acknowledged_records", "Durable receipt count", registry=registry).set(accepted["n"])
    outstanding = next(db.iter_query(OUTSTANDING_SQL))
    Gauge("adpulse_lineage_pending_records", "Acknowledged records without terminal classification", registry=registry).set(outstanding["n"])
    Gauge("adpulse_lineage_oldest_pending_seconds", "Age at inspection time of oldest unclassified receipt", registry=registry).set(
        max(0, (now_ms()-outstanding["oldest"])/1000) if outstanding["n"] else 0)
    return {"prometheus": generate_latest(registry).decode(), "active_release": selected}


def run_once(kind, root, *, wait_lock=False):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ("." + kind + ".lock")).open("a") as lock:
        # One writer per role, including manual --once invocations. A competing
        # process must not replace a healthy snapshot with an artificial failure.
        fcntl.flock(lock, fcntl.LOCK_EX | (0 if wait_lock else fcntl.LOCK_NB))
        return _run_locked(kind, root)


def _run_locked(kind, root):
    started = time.time()
    try:
        payload = inspect_archive(root) if kind == "coverage" else operational_metrics()
        return publish(root, kind, payload, started_at=started)
    except Exception as exc:
        publish(root, kind, None, started_at=started, error=exc)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=["coverage", "metrics"])
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--wait-lock", action="store_true", help="Wait for the role's existing writer, e.g. explicit CI refresh")
    parser.add_argument("--interval", type=float)
    args = parser.parse_args()
    interval = args.interval if args.interval is not None else 120 if args.kind == "coverage" else 30
    if interval < 1:
        parser.error("interval must be at least one second")
    root = directory()
    while True:
        try:
            result = run_once(args.kind, root, wait_lock=args.wait_lock)
            print(canonical({k: result[k] for k in ("kind", "status", "duration_seconds", "generated_at")}), flush=True)
        except Exception as exc:
            print(canonical(dict(kind=args.kind, status="error", error_type=type(exc).__name__,
                                 detail=str(exc)[:500])), flush=True)
            if args.once:
                raise
        if args.once:
            return
        time.sleep(interval)


if __name__ == "__main__":
    main()
