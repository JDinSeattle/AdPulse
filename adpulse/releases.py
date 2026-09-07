from __future__ import annotations

import os
import re
import time

import psycopg
from psycopg.rows import dict_row

from .common import canonical, digest


def connect():
    return psycopg.connect(os.getenv("POSTGRES_DSN", "postgresql://adpulse:adpulse-local@localhost:15432/adpulse"), row_factory=dict_row)


def register(release, rules, kind="replay", manifest_hash=None, partitions=3):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", release):
        raise ValueError("Invalid release identifier")
    topic = f"{os.getenv('TOPIC_PREFIX', 'adpulse')}.results.{release}"
    with connect() as db:
        db.execute("""INSERT INTO releases(release_id,kind,status,topic,partition_count,rules_sha256,rules,manifest_sha256)
          VALUES(%s,%s,'building',%s,%s,%s,%s,%s)""", (release, kind, topic, partitions, digest(rules), canonical(rules), manifest_hash))
    return topic


def active():
    with connect() as db:
        row = db.execute("SELECT release_id FROM active_release WHERE singleton").fetchone()
    return row["release_id"] if row else None


def get_release(release):
    with connect() as db:
        return db.execute("SELECT release_id,kind,status FROM releases WHERE release_id=%s", (release,)).fetchone()


def list_releases():
    with connect() as db:
        return db.execute("""SELECT release_id,kind,status,topic,partition_count,rules_sha256,
            rules->>'rule_version' AS rule_version,manifest_sha256,created_at,validated_at,
            report - 'processing_pending' - 'differences' AS report FROM releases ORDER BY created_at DESC""").fetchall()


def mark_validated(release, report):
    if not report.get("passed") or not report.get("archive_complete") or not report.get("sink_complete"):
        raise ValueError("Release requires passing reconciliation, complete archive and visible sink")
    with connect() as db:
        updated = db.execute("""UPDATE releases SET status='validated',report=%s,validated_at=now()
            WHERE release_id=%s AND status='building' RETURNING release_id""", (canonical(report), release)).fetchone()
        if not updated:
            raise ValueError("Release is missing or no longer building")


def activate(release, reason):
    if not reason.strip():
        raise ValueError("An audit reason is required")
    with connect() as db:
        # Serializes first activation as well as later pointer swaps.
        db.execute("SELECT pg_advisory_xact_lock(710021)")
        target = db.execute("SELECT status,report FROM releases WHERE release_id=%s FOR UPDATE", (release,)).fetchone()
        if not target or target["status"] not in {"validated", "retired", "active"}:
            raise ValueError("Only validated releases can be activated or rolled back")
        previous = db.execute("SELECT release_id FROM active_release WHERE singleton FOR UPDATE").fetchone()
        old = previous["release_id"] if previous else None
        if old == release:
            return {"previous": old, "active": release, "changed": False}
        if old:
            db.execute("UPDATE releases SET status='retired' WHERE release_id=%s", (old,))
        db.execute("UPDATE releases SET status='active' WHERE release_id=%s", (release,))
        db.execute("INSERT INTO active_release VALUES(TRUE,%s) ON CONFLICT(singleton) DO UPDATE SET release_id=EXCLUDED.release_id", (release,))
        db.execute("INSERT INTO release_audit(previous_release,next_release,reason) VALUES(%s,%s,%s)", (old, release, reason))
    return {"previous": old, "active": release, "changed": True}


def publish_snapshot(snapshot, rules, manifest_hash, baseline=()):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    release = snapshot["release_id"]
    topic = register(release, rules, manifest_hash=manifest_hash)
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    admin = AdminClient({"bootstrap.servers": brokers})
    for future in admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1,
                                                config={"cleanup.policy": "delete", "retention.ms": "604800000"})]).values():
        future.result(30)
    producer = Producer({"bootstrap.servers": brokers, "transactional.id": f"adpulse-replay-{release}",
                         "enable.idempotence": True, "acks": "all", "transaction.timeout.ms": 900000})
    producer.init_transactions(60)
    current = {row["metric_key"] for row in snapshot["metrics"]}
    for old in baseline:
        if old["metric_key"] not in current:
            zero = dict(old, release_id=release, rule_version=rules["rule_version"], status="final", values=dict.fromkeys(old["values"], 0))
            zero.pop("receipt_updates", None)
            snapshot["metrics"].append(zero)
    try:
        producer.begin_transaction()
        for collection, kind, key in (("metrics", "metric", "metric_key"), ("associations", "association", "association_key")):
            for row in snapshot[collection]:
                row["record_type"] = kind
                row["output_key"] = ("m:" if kind == "metric" else "a:") + row[key]
                producer.produce(topic, key=row["output_key"], value=canonical(row))
                producer.poll(0)
        producer.commit_transaction(120)
    except Exception:
        producer.abort_transaction(30)
        with connect() as db:
            db.execute("UPDATE releases SET status='failed' WHERE release_id=%s", (release,))
        raise
    return topic


def wait_for_snapshot(snapshot, clickhouse, timeout=180):
    from .oracle import compare
    deadline = time.monotonic() + timeout
    report = None
    while time.monotonic() < deadline:
        actual = clickhouse.snapshots(snapshot["release_id"])
        report = compare(snapshot, actual)
        if report["passed"] and len(actual["metrics"]) == len(snapshot["metrics"]):
            return dict(report, sink_complete=True)
        time.sleep(2)
    raise TimeoutError(f"Sink did not reconcile: {report}")
