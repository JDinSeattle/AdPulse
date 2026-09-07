import base64
import json

import pytest
from fastapi.testclient import TestClient

from adpulse import api
from adpulse.pagination import CursorCodec, InvalidCursor
from adpulse.storage import ClickHouse, PageQueryError, QueryBudget


def test_cursor_survives_restart_with_shared_key_and_preserves_expiry():
    clock = [100]
    codec = CursorCodec(b"a" * 32, clock=lambda: clock[0])
    token = codec.encode(release="r1", kind="association", after='a:["广告"]', filters={})
    data = CursorCodec(b"a" * 32, clock=lambda: 101).decode(token, kind="association", filters={})
    assert data["after"] == 'a:["广告"]' and data["expires_at"] == 1900
    clock[0] = 1899
    next_token = codec.encode(release="r1", kind="association", after="a:next", filters={}, expires_at=data["expires_at"])
    clock[0] = 1900
    with pytest.raises(InvalidCursor):
        codec.decode(next_token, kind="association", filters={})


@pytest.mark.parametrize("change", ["signature", "key", "kind", "filter", "release", "malformed", "oversized"])
def test_cursor_rejects_tampering_and_changed_scope(change):
    codec = CursorCodec(b"a" * 32)
    token = codec.encode(release="r1", kind="association", after="a:key", filters={"status": "matched"})
    options = dict(kind="association", filters={"status": "matched"}, release="r1")
    if change == "signature":
        raw = bytearray(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
        raw[-1] ^= 1
        token = base64.urlsafe_b64encode(raw).decode()
    elif change == "key":
        codec = CursorCodec(b"b" * 32)
    elif change == "kind":
        options["kind"] = "metric"
    elif change == "filter":
        options["filters"] = {"status": "unmatched"}
    elif change == "release":
        options["release"] = "r2"
    elif change == "malformed":
        token = "!not-a-cursor"
    else:
        token = "a" * 4097
    with pytest.raises(InvalidCursor):
        codec.decode(token, **options)


@pytest.fixture
def serving(monkeypatch):
    active = ["r1"]
    calls = []
    monkeypatch.setattr(api.releases, "active", lambda: active[0])
    monkeypatch.setattr(api.releases, "get_release", lambda r: {"kind": "replay", "status": "validated"})

    class Database:
        def page(self, release, kind, **options):
            calls.append((release, kind, options))
            return [{"association_key": options["after"] or "first", "status": "matched"}], "a:next" if not options["after"] else None

    monkeypatch.setattr(api, "ClickHouse", Database)
    with TestClient(api.app) as client:
        yield client, active, calls


def test_pagination_pins_release_when_active_pointer_changes(serving):
    client, active, calls = serving
    first = client.get("/v1/associations?limit=1&status=matched").json()
    active[0] = "r2"
    second = client.get("/v1/associations", params={"limit": 1, "status": "matched", "cursor": first["next_cursor"]}).json()
    assert first["release_id"] == second["release_id"] == "r1"
    assert second["next_cursor"] is None and not second["has_more"]
    assert calls[-1][0] == "r1" and calls[-1][2]["after"] == "a:next"
    assert client.get("/v1/associations").json()["release_id"] == "r2"


@pytest.mark.parametrize("query", ["limit=0", "limit=501", "limit=1.2", "status=bogus", "release=a%27--"])
def test_invalid_requests_do_not_reach_database(serving, query):
    client, _, calls = serving
    assert client.get("/v1/associations?" + query).status_code == 422
    assert not calls


def test_unknown_or_unvalidated_release_is_not_served(serving, monkeypatch):
    client, _, calls = serving
    for metadata, expected in [(None, 404), ({"kind": "replay", "status": "building"}, 409),
                               ({"kind": "replay", "status": "failed"}, 409)]:
        monkeypatch.setattr(api.releases, "get_release", lambda r: metadata)
        assert client.get("/v1/associations?release=r1").status_code == expected
    assert not calls


def test_budget_failure_has_no_partial_success_or_internal_error_leak(serving, monkeypatch):
    client, _, _ = serving
    def fail(*args, **kwargs):
        raise PageQueryError("budget")
    monkeypatch.setattr(api.ClickHouse, "page", fail)
    result = client.get("/v1/associations")
    assert result.status_code == 503 and "associations" not in result.json()
    assert result.json()["detail"]["code"] == "QUERY_BUDGET"


def test_live_pagination_explicitly_does_not_claim_immutable_snapshot(serving, monkeypatch):
    client, _, _ = serving
    monkeypatch.setattr(api.releases, "get_release", lambda r: {"kind": "live", "status": "retired"})
    assert client.get("/v1/associations").json()["consistency"] == "live_keyset"


def test_response_budget_stops_stream_and_closes_connection():
    class Response:
        closed = False
        def raise_for_status(self):
            pass
        def iter_content(self, chunk_size):
            yield b"x" * 11
            raise AssertionError("must stop reading after byte budget")
        def close(self):
            self.closed = True
    response = Response()
    db = ClickHouse()
    db.session.post = lambda *args, **kwargs: response
    with pytest.raises(PageQueryError, match="budget"):
        db.page("r1", "association", budget=QueryBudget(result_bytes=10))
    assert response.closed


def test_large_business_key_does_not_produce_an_unusable_next_cursor(serving, monkeypatch):
    client, _, _ = serving
    monkeypatch.setattr(api.ClickHouse, "page", lambda *args, **kwargs: ([{}], "a:" + "x" * 2000))
    response = client.get("/v1/associations")
    assert response.status_code == 422 and "next_cursor" not in response.json()


def test_invalid_json_response_cannot_be_reported_as_complete_page():
    class Response:
        def raise_for_status(self):
            pass
        def iter_content(self, chunk_size):
            yield json.dumps({"output_key": "a:1", "public_payload": "{}"}).encode() + b"\nException: failed"
        def close(self):
            pass
    db = ClickHouse()
    db.session.post = lambda *args, **kwargs: Response()
    with pytest.raises(PageQueryError, match="invalid_response"):
        db.page("r1", "association")
