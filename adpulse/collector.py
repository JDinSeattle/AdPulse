from __future__ import annotations

import json
import os
import threading
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.concurrency import run_in_threadpool

from .common import canonical, digest, now_ms

ACCEPTED = Counter("adpulse_collector_accepted_events", "Durably acknowledged records")
FAILED = Counter("adpulse_collector_failed_batches", "Unacknowledged batches")
ACK_TIME = Histogram("adpulse_collector_ack_seconds", "Kafka transaction commit latency")
MAX_BYTES = 2 * 1024 * 1024


class KafkaReceiptWriter:
    def __init__(self):
        from confluent_kafka import Producer
        self.prefix = os.getenv("TOPIC_PREFIX", "adpulse")
        self.producer = Producer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            "enable.idempotence": True, "acks": "all",
            "transactional.id": os.getenv("COLLECTOR_TRANSACTIONAL_ID", "adpulse-collector-1"),
            "transaction.timeout.ms": 900000, "message.timeout.ms": 30000,
        })
        self.producer.init_transactions(60)
        self.lock = threading.Lock()

    def accept(self, events, client_batch_id):
        batch_id = str(uuid.uuid4())
        received_at = now_ms()
        packets = [dict(batch_id=batch_id, client_batch_id=client_batch_id, receipt_id=f"{batch_id}:{i}",
                        index=i, received_at=received_at, event=e) for i, e in enumerate(events)]
        delivery, errors = [], []

        def callback(receipt_id, payload_hash):
            def on_delivery(error, message):
                if error:
                    errors.append(str(error))
                else:
                    delivery.append(dict(receipt_id=receipt_id, topic=message.topic(), partition=message.partition(),
                                         offset=message.offset(), sha256=payload_hash))
            return on_delivery

        with self.lock, ACK_TIME.time():
            try:
                self.producer.begin_transaction()
                for packet in packets:
                    event = packet["event"]
                    key = str(event.get("event_id", packet["receipt_id"])) if isinstance(event, dict) else packet["receipt_id"]
                    self.producer.produce(f"{self.prefix}.raw", key=key, value=canonical(packet),
                                          on_delivery=callback(packet["receipt_id"], digest(packet)))
                if self.producer.flush(30) or errors or len(delivery) != len(packets):
                    raise RuntimeError(f"Raw delivery failed: {errors}")
                manifest = dict(record_type="receipt", batch_id=batch_id, client_batch_id=client_batch_id,
                                received_at=received_at, count=len(packets),
                                records=sorted(delivery, key=lambda d: d["receipt_id"]),
                                receipt_ids=[p["receipt_id"] for p in packets])
                self.producer.produce(f"{self.prefix}.receipts", key=batch_id, value=canonical(manifest))
                self.producer.commit_transaction(60)
            except Exception:
                FAILED.inc()
                try:
                    self.producer.abort_transaction(30)
                except Exception:
                    pass
                raise
        ACCEPTED.inc(len(packets))
        return dict(batch_id=batch_id, client_batch_id=client_batch_id, accepted=len(packets),
                    received_at=received_at, manifest_sha256=digest(manifest), receipt_ids=manifest["receipt_ids"])


def create_app(writer=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.writer = writer or await run_in_threadpool(KafkaReceiptWriter)
        yield

    application = FastAPI(title="AdPulse collector", version="0.1.0", lifespan=lifespan)

    @application.get("/health")
    def health():
        return {"status": "ready" if getattr(application.state, "writer", None) else "starting", "boundary": "kafka-transaction-commit"}

    @application.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @application.post("/v1/events", status_code=202)
    async def accept(request: Request):
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > MAX_BYTES:
                raise HTTPException(413, "Batch exceeds 2 MiB")
        try:
            batch = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(400, "Invalid JSON") from None
        if not isinstance(batch, dict) or not isinstance(batch.get("events"), list) or not 1 <= len(batch["events"]) <= 1000:
            raise HTTPException(422, "events must contain 1–1000 records")
        client_id = batch.get("client_batch_id", "unspecified")
        if not isinstance(client_id, str) or len(client_id) > 200:
            raise HTTPException(422, "Invalid client_batch_id")
        try:
            return await run_in_threadpool(application.state.writer.accept, batch["events"], client_id)
        except Exception as exc:
            # No acknowledged response before both events and receipt manifest commit.
            raise HTTPException(503, "Kafka commit not confirmed; retry with the same business event IDs") from exc

    return application


app = create_app()
