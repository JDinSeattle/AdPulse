"""Exhaustive local reference calculation with disk-backed joins and comparison."""
from __future__ import annotations

import json
import time
from contextlib import closing

from .archive import S3Objects
from .archive_index import ArchiveIndex
from .common import canonical, load_rules
from .disk import DiskWorkspace
from .oracle import calculate
from .storage import ClickHouse


def compare_streams(workspace, expected, actual, *, sample_limit=100):
    if not 0 <= sample_limit <= 1000:
        raise ValueError("Invalid difference sample budget")
    db = workspace.db
    db.execute("CREATE TABLE comparison(kind TEXT,key TEXT,expected TEXT,actual TEXT,PRIMARY KEY(kind,key))")
    counts = {}
    for side, outputs in (("expected", expected), ("actual", actual)):
        for kind in ("metrics", "associations"):
            count = 0
            for row in outputs[kind]:
                count += 1
                if kind == "metrics":
                    key, value = row["metric_key"], row["values"]
                    if not any(value.values()):
                        continue
                else:
                    key = row["association_key"]
                    value = [row[f] for f in ("status", "reason", "experiment_id", "variant")]
                prior = db.execute(f"SELECT {side} FROM comparison WHERE kind=? AND key=?", (kind, key)).fetchone()
                if prior and prior[0] is not None:
                    raise ValueError("Duplicate business result key")
                db.execute(f"INSERT INTO comparison(kind,key,{side}) VALUES(?,?,?) "
                           f"ON CONFLICT(kind,key) DO UPDATE SET {side}=excluded.{side}", (kind, key, canonical(value)))
                workspace.tick()
            counts[side + "_" + kind] = count
    db.commit()
    count = db.execute("SELECT count(*) FROM comparison WHERE expected IS NOT actual").fetchone()[0]
    samples = [dict(kind=k, key=key, expected=json.loads(e) if e is not None else None,
                    actual=json.loads(a) if a is not None else None)
               for k, key, e, a in db.execute("SELECT * FROM comparison WHERE expected IS NOT actual ORDER BY kind,key LIMIT ?", (sample_limit,))]
    return dict(passed=count == 0, differences=samples, difference_count=count, sample_limit=sample_limit, **counts)


def coverage(index, db, expected_receipts=()):
    def accepted():
        yield from expected_receipts
        for row in db.iter_query("SELECT arrayJoin(receipt_ids) AS receipt_id FROM adpulse.receipts FINAL FORMAT JSONEachRow"):
            yield row["receipt_id"]
    return index.coverage(accepted(), db.iter_query("SELECT receipt_id,disposition FROM adpulse.quality FINAL WHERE disposition!='signal' FORMAT JSONEachRow"))


def actual_results(db, kind, release, directory):
    # Reduce the same latest offset as the original full-payload query. Only
    # comparison fields enter aggregate state; spill before the query RAM cap.
    if kind == "metrics":
        fields = ("metric_key", "values")
    elif kind == "associations":
        fields = ("association_key", "status", "reason", "experiment_id", "variant")
    else:
        raise ValueError("Unknown result kind")
    projection = "tuple(" + ",".join("JSONExtractRaw(payload,'" + f + "')" for f in fields) + ")"
    query = f"""SELECT output_key,argMax({projection},output_offset) AS latest
        FROM adpulse.results WHERE release_id={{release:String}} AND record_type={{kind:String}}
        GROUP BY output_key
        SETTINGS max_bytes_before_external_group_by=67108864,
                 max_temporary_data_on_disk_size_for_query=4294967296
        FORMAT JSONEachRow"""
    for row in db.spooled_query(query, {"release": release, "kind": kind[:-1]}, directory=directory):
        yield dict(zip(fields, (json.loads(value) for value in row["latest"])))


def run(index_path, workspace_path, *, db=None, objects=None, release="live-v1", expected_receipts=(), audit=False):
    started = time.monotonic()
    db, objects = db or ClickHouse(), objects or S3Objects()
    with closing(ArchiveIndex(index_path)) as index:
        index_report = index.refresh(objects, audit=audit)
        completeness = coverage(index, db, expected_receipts)
        if not completeness["archive_complete"] or not completeness["lineage_complete"]:
            return dict(passed=False, completeness=completeness, phase="source-coverage", index=index_report,
                        elapsed_seconds=time.monotonic()-started)
        with closing(DiskWorkspace(workspace_path)) as workspace:
            expected = calculate(index.packets(), load_rules(), release, index.dimensions(), storage=workspace)

            comparison = compare_streams(workspace, expected, {
                kind: actual_results(db, kind, release, workspace.path.parent)
                for kind in ("metrics", "associations")})
        return dict(**comparison, completeness=completeness, acknowledged=completeness["acknowledged"],
                    phase="complete", index=index_report, elapsed_seconds=time.monotonic()-started,
                    checked_at_epoch=time.time(), mode="disk-backed-independent-oracle",
                    boundary="Local reference snapshot across separately sampled archive/DB sources; not a distributed transaction. Rerun while ingestion is quiescent for full equality.")
