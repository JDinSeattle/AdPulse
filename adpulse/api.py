from __future__ import annotations

import os
import time
import sqlite3
from contextlib import closing
from threading import BoundedSemaphore
from typing import Annotated, Literal


from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from . import releases
from .archive_index import ArchiveIndex
from .inspection import SnapshotUnavailable, directory, read_snapshot
from .storage import ClickHouse, PageQueryError
from .pagination import CursorCodec, CursorPositionTooLarge, InvalidCursor, MAX_CURSOR_LENGTH, MAX_PAGE_SIZE, valid_release

app = FastAPI(title="AdPulse measurement and governance API", version="0.3.0")
CURSORS = CursorCodec()
QUERY_SLOTS = BoundedSemaphore(int(os.getenv("ADPULSE_QUERY_CONCURRENCY", "4")))
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
    if not QUERY_SLOTS.acquire(blocking=False):
        QUERY_TIME.labels(kind, "overloaded").observe(0)
        raise HTTPException(503, {"code": "QUERY_OVERLOADED", "message": "Query capacity is busy; retry later"},
                            headers={"Retry-After": "1"})
    try:
        return _result_page(kind, release, cursor, limit, filters)
    finally:
        QUERY_SLOTS.release()


def _result_page(kind, release, cursor, limit, filters):
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


def inspection_snapshot(kind):
    try:
        return read_snapshot(kind, max_age=600 if kind == "coverage" else 90)
    except SnapshotUnavailable as exc:
        raise HTTPException(503, {"code": "INSPECTION_UNAVAILABLE", "message": "Background inspection is unavailable or expired"},
                            headers={"Retry-After": "30"}) from exc


@app.get("/v1/quality")
def quality():
    # Samples remain bounded in the database and in the response. Summary is
    # sampled in the background rather than scanning all lineage per request.
    snapshot = inspection_snapshot("coverage")
    return {"summary": snapshot["payload"].get("quality_summary", []),
            "samples": snapshot["payload"].get("quality_samples", []),
            "checked_at_epoch": snapshot["generated_at"], "consistency": "inspection_snapshot"}


@app.get("/v1/reconcile")
def completeness():
    snapshot = inspection_snapshot("coverage")
    return {**snapshot["payload"], "checked_at_epoch": snapshot["generated_at"],
            "check_started_at_epoch": snapshot["started_at"], "consistency": "inspection_snapshot"}


@app.get("/v1/trace/{receipt_id}")
def trace(receipt_id: Annotated[str, Path(min_length=1, max_length=256)]):
    snapshot = inspection_snapshot("coverage")
    try:
        with closing(ArchiveIndex(directory() / "archive.sqlite", readonly=True)) as index:
            index.db.execute("BEGIN")
            raw, quality = index.trace(receipt_id), index.quality(receipt_id)
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise HTTPException(503, "Trace index unavailable or result exceeds budget") from exc
    if not raw:
        raise HTTPException(404, {"message": "Receipt absent from the checked archive index", "checked_at_epoch": snapshot["generated_at"]})
    return {"receipt_id": receipt_id, "raw": raw, "quality": quality,
            "checked_at_epoch": snapshot["generated_at"], "consistency": "incremental_verified_archive_index"}


@app.get("/metrics")
def prometheus():
    registry = CollectorRegistry()
    ready = Gauge("adpulse_inspection_snapshot_ready", "Fresh successful background inspection available", ["kind"], registry=registry)
    checked = Gauge("adpulse_inspection_snapshot_timestamp_seconds", "Successful inspection completion time", ["kind"], registry=registry)
    text = ""
    for kind, max_age in (("metrics", 90), ("coverage", 600)):
        try:
            snapshot = read_snapshot(kind, max_age=max_age)
            ready.labels(kind).set(1)
            checked.labels(kind).set(snapshot["generated_at"])
            if kind == "metrics":
                text = snapshot["payload"]["prometheus"]
        except SnapshotUnavailable:
            ready.labels(kind).set(0)
    return Response(text.encode() + generate_latest(registry) + generate_latest(QUERY_REGISTRY), media_type=CONTENT_TYPE_LATEST)
