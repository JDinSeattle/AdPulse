from __future__ import annotations

import json
import os
import time
from typing import Annotated, Literal

import requests

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from . import releases
from .archive import S3Objects, verified_records, reconcile
from .common import now_ms
from .storage import ClickHouse, PageQueryError
from .pagination import CursorCodec, CursorPositionTooLarge, InvalidCursor, MAX_CURSOR_LENGTH, MAX_PAGE_SIZE, valid_release

app = FastAPI(title="AdPulse measurement and governance API", version="0.2.0")
CURSORS = CursorCodec()
QUERY_REGISTRY = CollectorRegistry()
QUERY_TIME = Histogram("adpulse_query_seconds", "Bounded result query latency", ["kind", "outcome"], registry=QUERY_REGISTRY)
QUERY_ROWS = Counter("adpulse_query_rows", "Rows served by bounded result queries", ["kind"], registry=QUERY_REGISTRY)
PageSize = Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)]
Cursor = Annotated[str | None, Query(max_length=MAX_CURSOR_LENGTH)]
Release = Annotated[str | None, Query(pattern=r"^[a-zA-Z0-9_-]{1,80}$")]
Filter = Annotated[str | None, Query(min_length=1, max_length=200)]


def release_id(value):
    chosen = value or releases.active()
    if not chosen:
        raise HTTPException(409, "No active release; validate and activate one first")
    return chosen


def result_page(kind, release, cursor, limit, filters):
    filters = {k: v for k, v in filters.items() if v is not None}
    after, expires_at = "", None
    if cursor is not None:
        try:
            decoded = CURSORS.decode(cursor, kind=kind, filters=filters, release=release)
        except InvalidCursor as exc:
            raise HTTPException(400, str(exc)) from exc
        release, after, expires_at = decoded["release"], decoded["after"], decoded["expires_at"]
    selected = release_id(release)
    if not valid_release(selected):
        raise HTTPException(422, "Invalid release identifier")
    metadata = releases.get_release(selected)
    if not metadata:
        raise HTTPException(404, "Unknown release")
    if metadata["status"] not in {"validated", "active", "retired"}:
        raise HTTPException(409, "Release is not validated for serving")
    started, outcome = time.monotonic(), "ok"
    try:
        rows, next_key = ClickHouse().page(selected, kind, limit=limit, after=after, filters=filters)
    except PageQueryError as exc:
        outcome = exc.reason
        raise HTTPException(504 if outcome == "timeout" else 503,
                            {"code": "QUERY_" + outcome.upper(),
                             "message": "Query did not complete; narrow filters or retry when dependencies recover"}) from exc
    finally:
        QUERY_TIME.labels(kind, outcome).observe(time.monotonic() - started)
    try:
        next_cursor = CURSORS.encode(release=selected, kind=kind, after=next_key, filters=filters,
                                    expires_at=expires_at) if next_key else None
    except CursorPositionTooLarge as exc:
        raise HTTPException(422, "Result key exceeds cursor capacity; narrow filters or use an archive export") from exc
    QUERY_ROWS.labels(kind).inc(len(rows))
    return {"release_id": selected, "next_cursor": next_cursor, "has_more": next_cursor is not None,
            "consistency": "live_keyset" if metadata["kind"] == "live" else "immutable_release",
            "metrics" if kind == "metric" else "associations": rows}


@app.get("/health")
def health():
    try:
        ClickHouse().query("SELECT 1 FORMAT JSONEachRow")
        with releases.connect() as db:
            db.execute("SELECT 1")
    except Exception as error:
        raise HTTPException(503, "Query dependencies unavailable") from error
    return {"status": "ready"}


@app.get("/v1/releases")
def release_list():
    return {"active_release": releases.active(), "releases": releases.list_releases()}


@app.post("/v1/releases/{release}/activate")
def activate(release: str, reason: str):
    try:
        return releases.activate(release, reason)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error


@app.get("/v1/metrics")
def metrics(release: Release = None, cohort: Literal["occurrence", "impression", "click"] | None = None,
            campaign: Filter = None, region: Filter = None, app_version: Filter = None,
            limit: PageSize = 100, cursor: Cursor = None):
    result = result_page("metric", release, cursor, limit,
                         dict(cohort=cohort, campaign=campaign, region=region, app_version=app_version))
    rows = result["metrics"]
    for row in rows:
        values = row["values"]
        row["ctr"] = values["clicks"] / values["impressions"] if row["cohort_basis"] == "impression" and values["impressions"] else None
        row["cvr"] = values["converted_clicks"] / values["clicks"] if row["cohort_basis"] == "click" and values["clicks"] else None
        row["quality_status"] = "missing_exposure" if row["experiment_id"] == "unknown" else "small_sample" if max(values["impressions"], values["clicks"]) < 100 else "available"
    return result


@app.get("/v1/associations")
def associations(release: Release = None, status: Literal["pending", "matched", "unmatched"] | None = None,
                 limit: PageSize = 100, cursor: Cursor = None):
    return result_page("association", release, cursor, limit, dict(status=status))


@app.get("/v1/quality")
def quality():
    db = ClickHouse()
    summary = db.query("SELECT disposition,error_code,rule_version,count() AS records FROM adpulse.quality FINAL GROUP BY disposition,error_code,rule_version FORMAT JSONEachRow")
    samples = db.query("SELECT payload FROM adpulse.quality FINAL WHERE disposition != 'cleaned' ORDER BY inserted_at DESC LIMIT 50 FORMAT JSONEachRow")
    return {"summary": summary, "samples": [json.loads(r["payload"]) for r in samples]}


@app.get("/v1/reconcile")
def completeness():
    db = ClickHouse()
    expected = db.query("SELECT arrayJoin(receipt_ids) AS receipt_id FROM adpulse.receipts FINAL FORMAT JSONEachRow")
    lineage = db.query("SELECT receipt_id,disposition FROM adpulse.quality FINAL WHERE disposition!='signal' FORMAT JSONEachRow")
    return reconcile(verified_records(S3Objects()), lineage, [r["receipt_id"] for r in expected])


@app.get("/v1/trace/{receipt_id}")
def trace(receipt_id: str):
    """Bounded-development trace lookup through checksummed archive and quality evidence."""
    rows = verified_records(S3Objects())
    raw = [r for r in rows if r["topic"].endswith(".raw") and r["value"].get("receipt_id") == receipt_id]
    if not raw:
        raise HTTPException(404, "Receipt is not yet present in a committed archive manifest")
    quality = ClickHouse().query("SELECT payload FROM adpulse.quality FINAL WHERE receipt_id={receipt:String} FORMAT JSONEachRow", {"receipt": receipt_id})
    return {"receipt_id": receipt_id, "raw": raw, "quality": [json.loads(q["payload"]) for q in quality]}


@app.get("/metrics")
def prometheus():
    registry = CollectorRegistry()
    try:
        response = requests.get(os.getenv("FLINK_REST_URL", "http://jobmanager:8081") + "/jobs/overview", timeout=3)
        response.raise_for_status()
        running = sum(j["state"] == "RUNNING" and j["name"].startswith("AdPulse") for j in response.json()["jobs"])
    except requests.RequestException:
        running = 0
    Gauge("adpulse_flink_running_jobs", "Expected two running AdPulse jobs", registry=registry).set(running)
    selected = releases.active()
    if selected:
        snapshot = ClickHouse().snapshots(selected, "metric")
        business = Gauge("adpulse_business_value", "Latest logical values, cohort/currency must be selected",
                         ["release", "cohort", "variant", "currency", "measure", "campaign", "region", "app_version"], registry=registry)
        totals = {}
        for row in snapshot["metrics"]:
            for measure, value in row["values"].items():
                key = (selected, row["cohort_basis"], row["variant"], row["currency"], measure,
                       row["campaign_id"], row["region"], row["app_version"])
                totals[key] = totals.get(key, 0) + value
        for key, value in totals.items():
            business.labels(*key).set(value)
        Gauge("adpulse_active_release_info", "Active release", ["release"], registry=registry).labels(selected).set(1)
        pending = ClickHouse().query("""SELECT countIf(JSONExtractString(payload,'status')='pending') AS n
            FROM adpulse.latest_results WHERE release_id={release:String} AND startsWith(output_key,'a:') FORMAT JSONEachRow""", {"release": selected})[0]["n"]
        Gauge("adpulse_pending_conversions", "Conversions awaiting click", registry=registry).set(pending)
    db = ClickHouse()
    if selected:
        freshness = db.query("""SELECT count() AS records, quantileExact(0.95)(lag) AS p95 FROM
            (SELECT receipt_id, (toUnixTimestamp64Milli(min(visible_at))-min(received_at))/1000.0 AS lag
             FROM adpulse.visibility WHERE release_id={release:String} AND received_at >= {since:UInt64}
             GROUP BY receipt_id) FORMAT JSONEachRow""", {"release": selected, "since": max(0, now_ms() - 300000)})[0]
        if freshness["records"]:
            Gauge("adpulse_freshness_p95_seconds", "Per-receipt acceptance to first visible metric P95", registry=registry).set(freshness["p95"])
    q = Gauge("adpulse_quality_records", "Logical quality records", ["disposition", "error_code"], registry=registry)
    for row in db.query("SELECT disposition,error_code,count() AS n FROM adpulse.quality FINAL GROUP BY disposition,error_code FORMAT JSONEachRow"):
        q.labels(row["disposition"], row["error_code"]).set(row["n"])
    accepted = db.query("SELECT sum(length(receipt_ids)) AS n FROM adpulse.receipts FINAL FORMAT JSONEachRow")[0]["n"]
    Gauge("adpulse_acknowledged_records", "Durable receipt count", registry=registry).set(accepted)
    # Receipt-to-lineage age catches a stopped cleaning job even when no bad records are emitted.
    outstanding = db.query("""SELECT min(received_at) AS oldest, count() AS n FROM
      (SELECT arrayJoin(receipt_ids) AS receipt_id, received_at FROM adpulse.receipts FINAL)
      WHERE receipt_id NOT IN (SELECT receipt_id FROM adpulse.quality FINAL WHERE disposition!='signal') FORMAT JSONEachRow""")
    Gauge("adpulse_lineage_pending_records", "Acknowledged records without terminal cleaning disposition", registry=registry).set(outstanding[0]["n"])
    Gauge("adpulse_lineage_oldest_pending_seconds", "Age of oldest unclassified receipt", registry=registry).set(
        max(0, (now_ms() - outstanding[0]["oldest"]) / 1000) if outstanding[0]["n"] else 0)
    return Response(generate_latest(registry) + generate_latest(QUERY_REGISTRY), media_type=CONTENT_TYPE_LATEST)
