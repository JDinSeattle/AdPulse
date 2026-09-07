from copy import deepcopy
from contextlib import closing
import io
import json
from threading import BoundedSemaphore

import pytest
from fastapi.testclient import TestClient

from adpulse import api
from adpulse.archive import LocalObjects, archive_batch, reconcile, verified_records
from adpulse.archive_index import ArchiveIndex, chunks
from adpulse.common import canonical, digest, load_rules
from adpulse.disk import DiskWorkspace
from adpulse.generator import generate, receipts
from adpulse.inspection import SnapshotUnavailable, publish, read_snapshot
from adpulse.oracle import calculate
from adpulse.reconciliation import compare_streams
from adpulse.storage import ClickHouse


@pytest.mark.parametrize("scenario", ["normal", "mixed", "duplicates", "schema", "conversion-first", "out-of-order", "hotspot"])
def test_disk_oracle_preserves_all_outputs_and_order(tmp_path, scenario):
    packets = receipts(generate(80, seed=31, scenario=scenario)["transport"])
    rules = load_rules()
    dimensions = [{"campaign_id": "campaign-0", "effective_from": 0, "source_version": 1, "attributes": {"channel": "旧"}}]
    expected = calculate(packets, rules, "same", dimensions)
    with closing(DiskWorkspace(tmp_path / "reference.sqlite")) as workspace:
        actual = calculate(iter(packets), rules, "same", dimensions, storage=workspace)
        for key in ("metrics", "associations", "quality"):
            actual[key] = list(actual[key])
        assert actual == expected
    with pytest.raises(FileExistsError):
        DiskWorkspace(tmp_path / "reference.sqlite")


def test_disk_comparison_counts_all_differences_but_bounds_samples(tmp_path):
    expected = {"metrics": ({"metric_key": str(i), "values": {"clicks": 1}} for i in range(250)), "associations": []}
    with closing(DiskWorkspace(tmp_path / "reference.sqlite")) as workspace:
        report = compare_streams(workspace, expected, {"metrics": [], "associations": []}, sample_limit=3)
    assert not report["passed"] and report["difference_count"] == 250
    assert len(report["differences"]) == 3


def archive_fixture(tmp_path):
    objects = LocalObjects(tmp_path / "objects")
    packets = receipts(generate(12, scenario="mixed")["transport"])
    rows = [dict(topic="adpulse.raw", partition=0, offset=i*2, value=p) for i, p in enumerate(packets)]
    refs = [dict(receipt_id=r["value"]["receipt_id"], topic=r["topic"], partition=0, offset=r["offset"], sha256=digest(r["value"])) for r in rows]
    manifest = dict(count=len(refs), receipt_ids=[r["receipt_id"] for r in refs], records=refs)
    archive_batch(objects, [dict(topic="adpulse.receipts", partition=1, offset=7, value=manifest)])
    data_manifest = archive_batch(objects, rows)
    result = calculate(packets)
    return objects, rows, data_manifest, result


def test_incremental_index_reopen_exact_coverage_and_trace(tmp_path):
    objects, rows, _, result = archive_fixture(tmp_path)
    index_path = tmp_path / "index.sqlite"
    with closing(ArchiveIndex(index_path)) as index:
        assert index.refresh(objects)["verified_manifests"] == 2
        old = reconcile(verified_records(objects), result["quality"])
        quality = (dict(r, payload=canonical(r)) for r in result["quality"])
        new = index.coverage(lineage=quality)
        assert all(new[k] == value for k, value in old.items())
        assert index.trace(rows[0]["value"]["receipt_id"]) == [rows[0]]
        assert index.quality(rows[0]["value"]["receipt_id"])
        assert list(index.packets()) == sorted([r["value"] for r in rows], key=lambda p: (p["received_at"], p["batch_id"], p["index"]))
    with closing(ArchiveIndex(index_path)) as index:
        assert index.refresh(objects)["verified_manifests"] == 0
        assert index.refresh(objects, audit=True)["verified_manifests"] == 2


def test_invalid_object_rolls_back_all_rows_and_can_be_retried(tmp_path):
    objects, rows, manifest, _ = archive_fixture(tmp_path)
    body_path = objects.directory / manifest["data_key"]
    original = body_path.read_bytes()
    # Valid rows but deliberately wrong object digest: failure only at EOF.
    body_path.write_bytes(original.replace(b'"index":0', b'"index": 0', 1))
    with closing(ArchiveIndex(tmp_path / "index.sqlite")) as index:
        with pytest.raises(ValueError):
            index.refresh(objects)
        assert not index.trace(rows[0]["value"]["receipt_id"])
        body_path.write_bytes(original)
        index.refresh(objects)
        assert index.trace(rows[0]["value"]["receipt_id"])


def test_index_rejects_conflicting_offsets_and_changed_immutable_objects(tmp_path):
    objects, rows, manifest, _ = archive_fixture(tmp_path)
    with closing(ArchiveIndex(tmp_path / "index.sqlite")) as index:
        index.refresh(objects)
        bad = deepcopy(rows[0])
        bad["value"]["received_at"] += 1
        archive_batch(objects, [bad])
        with pytest.raises(ValueError, match="Conflicting archived offset"):
            index.refresh(objects)
        assert index.trace(rows[0]["value"]["receipt_id"])[0] == rows[0]
    # Fresh index for explicit audit versus incremental immutability semantics.
    other = LocalObjects(tmp_path / "other")
    manifest = archive_batch(other, rows)
    with closing(ArchiveIndex(tmp_path / "other.sqlite")) as index:
        index.refresh(other)
        (other.directory / manifest["data_key"]).write_bytes(b"broken\n")
        assert index.refresh(other)["verified_manifests"] == 0
        with pytest.raises((ValueError, KeyError)):
            index.refresh(other, audit=True)


def test_archive_byte_budgets_and_truncated_stream():
    assert list(chunks(io.BytesIO(b'abc\ndef'), max_line=3)) == [b'abc\n', b'def']
    with pytest.raises(ValueError, match="line"):
        list(chunks(io.BytesIO(b'12345'), max_line=4))
    with pytest.raises(ValueError, match="object"):
        list(chunks(io.BytesIO(b'12345'), max_bytes=4))


def test_index_detects_removed_manifest_and_bounds_missing_receipt_lists(tmp_path):
    objects, _, _, _ = archive_fixture(tmp_path)
    with closing(ArchiveIndex(tmp_path / "index.sqlite")) as index:
        index.refresh(objects)
        report = index.coverage((f'missing-{i}' for i in range(200)), sample_limit=2)
        assert report['archive_missing_count'] == 200
        assert len(report['archive_missing']) == 2
        (objects.directory / next(objects.iter_keys('manifests/'))).unlink()
        with pytest.raises(ValueError, match='manifest is missing'):
            index.refresh(objects)


def test_only_actual_cdc_counts_toward_dimension_history_budget(tmp_path):
    objects = LocalObjects(tmp_path/'objects')
    rows = [dict(topic='adpulse.quality',partition=0,offset=i,value={'receipt_id':str(i)}) for i in range(10001)]
    archive_batch(objects,rows)
    dimension = {'campaign_id':'c','effective_from':0,'source_version':1,'attributes':{}}
    archive_batch(objects,[dict(topic='adpulse.cdc.public.campaign_versions',partition=0,offset=0,value={'after':dimension,'op':'c'})])
    with closing(ArchiveIndex(tmp_path/'index.sqlite')) as index:
        index.refresh(objects)
        assert index.dimensions()==[dict(dimension,deleted=False)]


def test_disk_reference_matches_frozen_prechange_fixture(tmp_path):
    from pathlib import Path
    fixture = json.loads(Path('tests/fixtures/oracle-baseline.json').read_text())
    with closing(DiskWorkspace(tmp_path / 'work.sqlite')) as workspace:
        result = calculate(iter(fixture['packets']), fixture['rules'], fixture['expected']['release_id'], storage=workspace)
        for field in ('metrics', 'associations', 'quality'):
            result[field] = list(result[field])
        assert result == fixture['expected']


def test_snapshot_expiry_failure_and_future_clock_fail_closed(tmp_path):
    result = publish(tmp_path, "coverage", {"archive_complete": True}, started_at=100)
    now = result["generated_at"]
    assert read_snapshot("coverage", root=tmp_path, max_age=10, clock=lambda: now+10)["payload"]["archive_complete"]
    for tick in (now+11, now-6):
        with pytest.raises(SnapshotUnavailable):
            read_snapshot("coverage", root=tmp_path, max_age=10, clock=lambda: tick)
    publish(tmp_path, "coverage", None, started_at=now, error=ValueError("private exception text"))
    with pytest.raises(SnapshotUnavailable):
        read_snapshot("coverage", root=tmp_path, max_age=600)
    assert "private exception text" not in (tmp_path / "coverage.json").read_text()


def test_http_uses_index_and_snapshots_without_database_or_object_queries(tmp_path, monkeypatch):
    objects, rows, _, result = archive_fixture(tmp_path)
    with closing(ArchiveIndex(tmp_path / "archive.sqlite")) as index:
        index.refresh(objects)
        coverage = index.coverage(lineage=(dict(r, payload=canonical(r)) for r in result["quality"]))
    publish(tmp_path, "coverage", coverage, started_at=0)
    publish(tmp_path, "metrics", {"prometheus": "adpulse_flink_running_jobs 2\n"}, started_at=0)
    monkeypatch.setenv("ADPULSE_INSPECTION_DIR", str(tmp_path))

    def forbidden(*args, **kwargs):
        raise AssertionError("Read-model endpoint accessed an external dependency")
    monkeypatch.setattr(api, "ClickHouse", forbidden)
    monkeypatch.setattr(api.releases, "active", forbidden)
    with TestClient(api.app) as client:
        receipt = rows[0]["value"]["receipt_id"]
        assert client.get("/v1/trace/" + receipt).json()["raw"] == [rows[0]]
        assert client.get("/v1/reconcile").json()["archive_complete"]
        assert client.get("/v1/quality").status_code == 200
        assert "adpulse_flink_running_jobs 2" in client.get("/metrics").text
        assert client.get("/v1/trace/missing").status_code == 404
        data = json.loads((tmp_path / "metrics.json").read_text())
        data["generated_at"] = 0
        (tmp_path / "metrics.json").write_text(json.dumps(data))
        text = client.get("/metrics").text
        assert 'adpulse_inspection_snapshot_ready{kind="metrics"} 0.0' in text
        assert "adpulse_flink_running_jobs" not in text
        publish(tmp_path, "coverage", None, started_at=0, error=ValueError())
        assert client.get("/v1/reconcile").status_code == 503
        assert client.get("/v1/trace/" + receipt).status_code == 503


def test_query_overload_rejects_without_dependency_and_releases_slot(monkeypatch):
    slot = BoundedSemaphore(1)
    monkeypatch.setattr(api, "QUERY_SLOTS", slot)
    slot.acquire()
    with TestClient(api.app) as client:
        response = client.get("/v1/associations")
        assert response.status_code == 503 and response.headers["Retry-After"] == "1"
    slot.release()
    monkeypatch.setattr(api, "_result_page", lambda *args: (_ for _ in ()).throw(ValueError("dependency failed")))
    with pytest.raises(ValueError):
        api.result_page("metric", None, None, 1, {})
    assert slot.acquire(blocking=False)


@pytest.mark.parametrize('case', ['valid', 'truncated', 'oversized'])
def test_query_spool_closes_upstream_before_consumption_and_cleans_up(tmp_path, case):
    class Response:
        closed = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

        def raise_for_status(self):
            pass

        def iter_content(self, size):
            yield b'{"receipt_id":"'
            yield '广告'.encode()
            yield b'"}\n' if case != 'truncated' else b'"'
    response = Response()
    db = ClickHouse()
    db.session.post = lambda *a, **kw: response
    result = db.spooled_query('query', directory=tmp_path, max_bytes=4 if case=='oversized' else 100)
    if case=='valid':
        assert next(result) == {'receipt_id':'广告'}
        assert response.closed
        result.close()
    else:
        with pytest.raises(ValueError):
            list(result)
    assert response.closed and not list(tmp_path.iterdir())
