"""Actual ClickHouse large-batch/retry acceptance using owned synthetic Kafka metadata.

This does not start Kafka or prove consumer offset commits; CI sink-replay covers
that boundary separately. Only this script's uniquely named fixture rows are removed.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
import time
import uuid

import requests

from adpulse.common import canonical, write_json
from adpulse.storage import ClickHouse, lookup_parameters


@dataclass
class Message:
    payload: dict
    source_topic: str
    source_offset: int
    source_partition: int = 0

    def value(self):
        return canonical(self.payload).encode()

    def topic(self):
        return self.source_topic

    def partition(self):
        return self.source_partition

    def offset(self):
        return self.source_offset


def run(output):
    db = ClickHouse()
    release = "sink-batch-test-" + uuid.uuid4().hex[:12]
    topic = os.getenv("TOPIC_PREFIX", "adpulse") + ".results." + release
    keys = [f"a:{i:05d}-" + "x" * 100 + "广告" * 8 for i in range(2000)]
    messages = [Message(dict(record_type="association", release_id=release, output_key=key,
                             association_key=key[2:], status="matched", reason="MATCHED"), topic, i)
                for i, key in enumerate(keys)]
    routes = [[release, key] for key in keys]
    chunks = list(lookup_parameters(routes))
    report = dict(passed=False, started_at_epoch=time.time(), release=release, batch_messages=len(messages),
                  environment="real ClickHouse; synthetic Kafka message metadata, no broker involved in this helper",
                  unbounded_parameter_bytes=len(canonical(routes).encode()), lookup_chunks=len(chunks),
                  max_lookup_parameter_bytes=max(len(c.encode()) for c in chunks))
    try:
        try:
            db.query("SELECT length({keys:String}) AS n FORMAT JSONEachRow", {"keys": canonical(routes)})
            report["unbounded_parameter_rejected"] = False
        except requests.HTTPError as exc:
            if "Field value too long" not in exc.response.text:
                raise
            report.update(unbounded_parameter_rejected=True, baseline_error="HTML Form Exception: Field value too long")
        db.write_batch(messages)
        db.write_batch(messages)  # Same persisted batch before/after hypothetical offset loss.
        rows = db.snapshots(release)["associations"]
        assert {r["output_key"] for r in rows} == set(keys) and len(rows) == 2000
        deliveries = db.query("SELECT count() AS n FROM adpulse.deliveries FINAL WHERE topic={topic:String} FORMAT JSONEachRow", {"topic": topic})[0]["n"]
        assert deliveries == 2000
        bad = Message({**messages[-1].payload, "status": "unmatched"}, topic, 1999)
        try:
            db.write_batch([bad])
            raise AssertionError("Changed payload at the same offset was accepted")
        except ValueError as exc:
            assert "conflicting content" in str(exc)
        try:
            db.write_batch([Message(messages[0].payload, topic, 3000, 1)])
            raise AssertionError("Existing output key changed partition")
        except ValueError as exc:
            assert "changed partition" in str(exc)
        assert db.snapshots(release)["associations"] == rows
        report.update(passed=True, exact_logical_results=len(rows), exact_deliveries=deliveries,
                      identical_retry_verified=True, conflicting_offset_rejected=True, partition_change_rejected=True)
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # Parameterized, uniquely owned fixtures only; never truncate shared tables.
        for table, field, value in (("results", "release_id", release), ("deliveries", "topic", topic)):
            db.query(f"ALTER TABLE adpulse.{table} DELETE WHERE {field}={{owned:String}} SETTINGS mutations_sync=1", {"owned": value})
        report.update(fixture_rows_removed=True, elapsed_seconds=round(time.time() - report["started_at_epoch"], 3))
        write_json(output, report)
    print(canonical(report))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="artifacts/sink-acceptance.json")
    run(parser.parse_args().output)
