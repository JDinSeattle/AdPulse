from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .archive import S3Objects, archive_batch
from .common import now_ms
from .storage import ClickHouse

HEARTBEAT = Gauge("adpulse_worker_heartbeat_seconds", "Last completed poll/batch", ["role"])
RECORDS = Counter("adpulse_worker_records", "Successfully persisted records", ["role"])
ERRORS = Counter("adpulse_worker_errors", "Failed persistence attempts", ["role"])
VISIBLE = Histogram("adpulse_acceptance_to_visible_seconds", "Accepted to synchronous ClickHouse insert; live output only",
                    buckets=(0.1, 1, 5, 10, 20, 30, 60, 120, 300, 600))
EVENT_DELAY = Histogram("adpulse_event_to_visible_seconds", "Event time to visible, includes synthetic lateness",
                        buckets=(1, 10, 60, 300, 3600, 7200, 86400, 172800))
LAG = Gauge("adpulse_consumer_offset_lag", "Offset distance, NOT business record count", ["role", "topic", "partition"])


def main(argv=None):
    from confluent_kafka import Consumer, TopicPartition
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("sink", "archive"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    prefix = os.getenv("TOPIC_PREFIX", "adpulse")
    consumer = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
                         "group.id": f"adpulse-{args.role}-v1", "auto.offset.reset": "earliest",
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "topic.metadata.refresh.interval.ms": 10000,
                         "isolation.level": "read_committed", "max.poll.interval.ms": 900000})
    topics = [f"{prefix}.raw", f"{prefix}.receipts", f"{prefix}.quarantine", f"{prefix}.quality", "adpulse.cdc.public.campaign_versions"] if args.role == "archive" else [f"^{prefix}\\.results\\..*", f"{prefix}.quality", f"{prefix}.receipts"]
    consumer.subscribe(topics)
    target = S3Objects() if args.role == "archive" else ClickHouse()
    start_http_server(int(os.getenv("METRICS_PORT", "9101")))
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while running:
            batch = consumer.consume(num_messages=int(os.getenv("BATCH_SIZE", "1000" if args.role == "archive" else "200")), timeout=1)
            if not batch:
                HEARTBEAT.labels(args.role).set(time.time())
                continue
            for message in batch:
                if message.error():
                    raise RuntimeError(message.error())
            while running:
                try:
                    if args.role == "archive":
                        rows = [dict(topic=m.topic(), partition=m.partition(), offset=m.offset(),
                                     value=json.loads(m.value()) if m.value() else None) for m in batch]
                        archive_batch(target, rows)
                    else:
                        target.write_batch(batch)
                        result_records = [m for m in batch if json.loads(m.value()).get("record_type") in {"metric", "association"}]
                        if os.getenv("FAULT_CRASH_AFTER_WRITE") == "1" and result_records:
                            print(json.dumps({"fault": "after_result_write_before_commit", "result_records": len(result_records),
                                              "offsets": [[m.topic(), m.partition(), m.offset()] for m in result_records]}), flush=True)
                            os._exit(77)  # Intentional drill: persisted output, uncommitted consumption offsets.
                        for message in batch:
                            payload = json.loads(message.value())
                            if payload.get("record_type") in {"metric", "association"} and payload.get("release_id", "").startswith("live-"):
                                VISIBLE.observe(max(0, (now_ms() - payload.get("received_at", now_ms())) / 1000))
                                EVENT_DELAY.observe(max(0, (now_ms() - payload.get("event_time", now_ms())) / 1000))
                    offsets = {}
                    for message in batch:
                        offsets[(message.topic(), message.partition())] = message.offset() + 1
                    consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()], asynchronous=False)
                    for (topic, partition), offset in offsets.items():
                        # Metrics must not perform one blocking broker RPC per partition per batch.
                        _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), cached=True)
                        if high >= 0:
                            LAG.labels(args.role, topic, str(partition)).set(max(0, high - offset))
                    HEARTBEAT.labels(args.role).set(time.time())
                    RECORDS.labels(args.role).inc(len(batch))
                    break
                except Exception:
                    ERRORS.labels(args.role).inc()
                    logging.exception("Persistence failed; holding batch, consumption offsets not committed")
                    time.sleep(2)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
