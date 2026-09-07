from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

from adpulse.archive import LocalObjects, archive_batch, reconcile, verified_records
from adpulse.collector import create_app
from adpulse.common import digest
from adpulse.storage import LocalSnapshotStore


def metric(value=1, release="r1"):
    return {"release_id": release, "metric_key": "key", "values": {"impressions": value}}


def test_sink_retries_out_of_order_offsets_and_restart(tmp_path):
    store = LocalSnapshotStore(tmp_path / "sink.db")
    store.insert("topic", 0, 10, metric(10))
    store.insert("topic", 0, 10, metric(10))
    store.db.close()
    recovered = LocalSnapshotStore(tmp_path / "sink.db")
    recovered.insert("topic", 0, 5, metric(5))
    recovered.insert("topic", 0, 12, metric(12))
    assert recovered.read("r1")["metrics"][0]["values"]["impressions"] == 12


def test_conflicting_same_offset_and_partition_movement_fail():
    store = LocalSnapshotStore()
    store.insert("topic", 0, 10, metric())
    with pytest.raises(ValueError, match="conflicting"):
        store.insert("topic", 0, 10, metric(5))
    with pytest.raises(ValueError, match="moved"):
        store.insert("topic", 1, 11, metric())


def test_release_isolation_and_zero_replacement():
    store = LocalSnapshotStore()
    store.insert("t1", 0, 1000, metric(9))
    store.insert("t2", 2, 1, metric(3, "r2"))
    store.insert("t2", 2, 2, metric(0, "r2"))
    assert store.read("r1")["metrics"][0]["values"]["impressions"] == 9
    assert store.read("r2")["metrics"][0]["values"]["impressions"] == 0


def test_archive_retries_commit_manifests_and_checksums(tmp_path):
    objects = LocalObjects(tmp_path)
    rows = [{"topic": "adpulse.raw", "partition": 0, "offset": 7, "value": {"receipt_id": "r1"}},
            {"topic": "adpulse.raw", "partition": 0, "offset": 12, "value": {"receipt_id": "r2"}}]
    manifest = archive_batch(objects, rows)
    archive_batch(objects, rows)
    assert verified_records(objects) == rows
    assert manifest["record_count"] == 2  # offset gaps include transactional control records.
    (tmp_path / manifest["data_key"]).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        verified_records(objects)


def test_archive_data_without_commit_manifest_is_not_visible(tmp_path):
    objects = LocalObjects(tmp_path)
    objects.put("data/orphan.jsonl", b"uncommitted")
    assert verified_records(objects) == []


def test_receipt_completeness_and_missing_lineage():
    raw = {"topic": "adpulse.raw", "partition": 0, "offset": 10, "value": {"receipt_id": "r1"}}
    manifest = {"count": 1, "receipt_ids": ["r1"], "records": [{"receipt_id": "r1", "topic": "adpulse.raw", "partition": 0, "offset": 10, "sha256": digest(raw["value"])}]}
    rows = [raw, {"topic": "adpulse.receipts", "partition": 0, "offset": 0, "value": manifest}]
    report = reconcile(rows, expected_receipts=["r1", "r2"])
    assert report["archived"] == 1 and report["archive_missing"] == ["r2"]
    assert report["processing_pending"] == ["r1", "r2"]
    changed = deepcopy(rows)
    changed[0]["value"]["tampered"] = True
    with pytest.raises(ValueError, match="disagree"):
        reconcile(changed)


class ReceiptWriter:
    def __init__(self, fail=False):
        self.fail = fail
        self.called = False

    def accept(self, events, batch):
        self.called = True
        if self.fail:
            raise RuntimeError("Kafka did not commit")
        return {"accepted": len(events), "client_batch_id": batch}


def test_collector_returns_503_without_durable_ack():
    with TestClient(create_app(ReceiptWriter(fail=True))) as client:
        assert client.post("/v1/events", json={"events": [{}]}).status_code == 503


def test_collector_accepts_bad_schema_for_traceable_quarantine():
    writer = ReceiptWriter()
    with TestClient(create_app(writer)) as client:
        assert client.post("/v1/events", json={"events": [{"bad": "schema"}]}).status_code == 202
        assert writer.called
        assert client.post("/v1/events", json={"events": []}).status_code == 422
        assert client.post("/v1/events", content="{").status_code == 400
        assert client.post("/v1/events", content=b"x" * (2 * 1024 * 1024 + 1)).status_code == 413
