"""Real local ClickHouse/PostgreSQL query invariants, isolated registry and owned fixture releases."""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import threading
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient
from psycopg import sql

from adpulse import api, releases
from adpulse.common import canonical, digest, load_rules, now_ms, write_json
from adpulse.storage import ClickHouse, PageQueryError, QueryBudget


def all_pages(client, kind, **params):
    name = "metrics" if kind == "metric" else "associations"
    rows, cursor, pinned, pages = [], None, None, 0
    while True:
        response = client.get("/v1/" + name, params={**params, **({"cursor": cursor} if cursor else {})})
        response.raise_for_status()
        page = response.json()
        if pinned is None:
            pinned = page["release_id"]
        assert page["release_id"] == pinned
        assert len(page[name]) <= params.get("limit", 100)
        rows.extend(page[name])
        pages += 1
        cursor = page["next_cursor"]
        if not cursor:
            assert not page["has_more"]
            return rows, pages


def run(reference_release, output):
    db, connect = ClickHouse(), releases.connect
    original_active = releases.active()
    token = uuid.uuid4().hex[:12]
    schema = "query_acceptance_" + token
    owned = ["query-test-" + token + "-a", "query-test-" + token + "-b"]
    report = dict(started_at_ms=now_ms(), environment="real local ClickHouse and PostgreSQL, synthetic query fixtures; ASGI TestClient",
                  reference_release=reference_release, fixture_releases=owned, passed=False)
    try:
        with connect() as registry:
            registry.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            for table in ("releases", "active_release", "release_audit"):
                registry.execute(sql.SQL("CREATE TABLE {}.{} (LIKE public.{} INCLUDING ALL)").format(
                    sql.Identifier(schema), sql.Identifier(table), sql.Identifier(table)))

        def isolated_connect():
            connection = connect()
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            return connection

        with patch.object(releases, "connect", isolated_connect), TestClient(api.app) as client:
            for release in owned:
                releases.register(release, load_rules())
                payloads = []
                for index in range(37):
                    key = f"a:key-{index:03d}"
                    for version, status in ((1, "pending"), (2, "matched"), (2, "matched")):
                        payload = dict(record_type="association", release_id=release, output_key=key,
                                       association_key=key[2:], status=status, reason=status.upper())
                        payloads.append(dict(release_id=release, output_key=key, record_type="association",
                                             output_topic="adpulse.results." + release, output_partition=0,
                                             output_offset=version, payload=canonical(payload), hash=digest(payload)))
                db.insert("results", payloads)
                releases.mark_validated(release, dict(passed=True, archive_complete=True, sink_complete=True,
                                                      scope="isolated synthetic query fixture, no ingestion claim"))
            releases.activate(owned[0], "isolated query acceptance start")
            assert not client.get("/v1/associations", params={"status": "pending"}).json()["associations"]
            first = client.get("/v1/associations", params={"status": "matched", "limit": 5}).json()
            expected = db.snapshots(owned[0], "association")["associations"]
            started = threading.Event()

            def switches():
                for index in range(20):
                    releases.activate(owned[1 - index % 2], "isolated concurrent pointer test")
                    started.set()
                releases.activate(owned[1], "isolated query acceptance finish")

            with concurrent.futures.ThreadPoolExecutor(1) as executor:
                future = executor.submit(switches)
                assert started.wait(10)
                found, cursor = first["associations"], first["next_cursor"]
                while cursor:
                    params = dict(status="matched", limit=5, cursor=cursor)
                    one, retry = client.get("/v1/associations", params=params), client.get("/v1/associations", params=params)
                    assert one.status_code == retry.status_code == 200
                    assert one.json() == retry.json(), "Same immutable cursor must be retry-stable"
                    page = one.json()
                    assert page["release_id"] == owned[0] and page["consistency"] == "immutable_release"
                    found.extend(page["associations"])
                    cursor = page["next_cursor"]
                future.result(30)
            assert found == expected and len(found) == len({r["association_key"] for r in found}) == 37
            assert client.get("/v1/associations").json()["release_id"] == owned[1]
            report.update(concurrent_pointer_switches=21, latest_filter_no_resurrection=True,
                          retry_stable=True, fixture_distinct_rows=len(found))
            assert not db.page(owned[0], "association", filters={"status": "matched' OR 1=1 --"})[0]
            rejected = []
            for name, budget in (("scan_rows", QueryBudget(read_rows=1)), ("response_bytes", QueryBudget(result_bytes=32))):
                try:
                    db.page(owned[0], "association", budget=budget)
                except PageQueryError as error:
                    assert error.reason == "budget"
                    rejected.append(name)
                else:
                    raise AssertionError("Budget did not fail closed: " + name)
            report["real_budget_rejections"] = rejected

        # Real replay traversal under the normal registry. Never alter the active production-like pointer.
        with TestClient(api.app) as client:
            checked = {}
            for kind, name in (("association", "associations"), ("metric", "metrics")):
                found, pages = all_pages(client, kind, release=reference_release, limit=500)
                expected = db.snapshots(reference_release, kind)[name]
                for row in expected:
                    row.pop("receipt_updates", None)
                if kind == "metric":
                    for row in found:
                        for extra in ("ctr", "cvr", "quality_status"):
                            row.pop(extra)
                assert found == expected
                checked[name] = dict(rows=len(found), pages=pages, equal_to_full_snapshot=True)
            report["full_traversal"] = checked
        report["passed"] = True
    finally:
        with connect() as registry:
            registry.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        db.query("ALTER TABLE adpulse.results DELETE WHERE release_id IN ({a:String},{b:String}) SETTINGS mutations_sync=2",
                 {"a": owned[0], "b": owned[1]})
        report["original_active_preserved"] = releases.active() == original_active
        report["finished_at_ms"] = now_ms()
        write_json(output, report)
    assert report["original_active_preserved"]
    print(json.dumps(report))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True)
    parser.add_argument("--output", default="artifacts/query-acceptance.json")
    args = parser.parse_args()
    run(args.release, args.output)
