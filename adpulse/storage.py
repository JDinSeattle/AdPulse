"""Full-snapshot sinks with explicit partition ordering and retry conflict checks."""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass

import requests

from .common import canonical, digest
from .pagination import MAX_PAGE_SIZE, valid_release


@dataclass(frozen=True)
class QueryBudget:
    seconds: int = 3
    memory_bytes: int = 128 * 1024 * 1024
    read_rows: int = 2_000_000
    result_bytes: int = 8 * 1024 * 1024


class PageQueryError(RuntimeError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def lookup_parameters(keys, max_bytes=64 * 1024):
    """Bound decoded HTTP field bytes, including JSON escaping and separators.

    Deduplicate lookup identities only; every delivery is still validated/written.
    Larger synchronous write batches must not create unbounded form parameters.
    """
    encoded, size = [], 2
    for key in dict.fromkeys(tuple(key) for key in keys):
        value = canonical(list(key))
        length = len(value.encode("utf-8"))
        if length + 2 > max_bytes:
            raise ValueError("Single lookup identity exceeds parameter byte budget")
        if size + length + bool(encoded) > max_bytes:
            yield "[" + ",".join(encoded) + "]"
            encoded, size = [], 2
        size += length + bool(encoded)
        encoded.append(value)
    if encoded:
        yield "[" + ",".join(encoded) + "]"


class LocalSnapshotStore:
    """Durable local harness for replay/crash invariant tests; not a Flink substitute."""
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS deliveries(topic TEXT, partition_id INTEGER, offset_id INTEGER, hash TEXT,
          PRIMARY KEY(topic,partition_id,offset_id));
        CREATE TABLE IF NOT EXISTS snapshots(release_id TEXT,key TEXT,topic TEXT,partition_id INTEGER,offset_id INTEGER,payload TEXT,
          PRIMARY KEY(release_id,key));
        """)

    def insert(self, topic, partition, offset, payload):
        release = payload["release_id"]
        key = payload.get("output_key", ("m:" + payload["metric_key"]) if "metric_key" in payload else "a:" + payload["association_key"])
        with self.db:
            existing = self.db.execute("SELECT hash FROM deliveries WHERE topic=? AND partition_id=? AND offset_id=?", (topic, partition, offset)).fetchone()
            if existing and existing[0] != digest(payload):
                raise ValueError("Same Kafka offset has conflicting content")
            route = self.db.execute("SELECT topic,partition_id,offset_id FROM snapshots WHERE release_id=? AND key=?", (release, key)).fetchone()
            if route and route[:2] != (topic, partition):
                raise ValueError("Key moved across topics/partitions; use a new release")
            self.db.execute("INSERT OR IGNORE INTO deliveries VALUES(?,?,?,?)", (topic, partition, offset, digest(payload)))
            if not route or offset > route[2]:
                self.db.execute("INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?,?)", (release, key, topic, partition, offset, canonical(payload)))

    def read(self, release):
        rows = [json.loads(row[0]) for row in self.db.execute("SELECT payload FROM snapshots WHERE release_id=? ORDER BY key", (release,))]
        return {"release_id": release, "metrics": [r for r in rows if "metric_key" in r],
                "associations": [r for r in rows if "association_key" in r]}


class ClickHouse:
    def __init__(self, url=None):
        self.url = url or os.getenv("CLICKHOUSE_URL", "http://localhost:18123")
        self.auth = (os.getenv("CLICKHOUSE_USER", "adpulse"), os.getenv("CLICKHOUSE_PASSWORD", "adpulse-local"))
        self.session = requests.Session()

    def query(self, sql, parameters=None):
        params = {"output_format_json_quote_64bit_integers": 0, **{f"param_{key}": value for key, value in (parameters or {}).items()}}
        response = self.session.post(self.url, params=params, data=sql.encode(), auth=self.auth, timeout=30)
        response.raise_for_status()
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]

    def iter_query(self, sql, parameters=None, *, seconds=120, memory_bytes=512 * 1024 * 1024):
        """Stream JSONEachRow for offline verification; mid-stream DB errors fail closed."""
        from .archive_index import chunks
        params = {"output_format_json_quote_64bit_integers": 0, "max_execution_time": seconds,
                  "max_memory_usage": memory_bytes, "max_threads": 2,
                  "timeout_overflow_mode": "throw", "max_rows_to_read": 100_000_000,
                  "read_overflow_mode": "throw",
                  **{f"param_{key}": value for key, value in (parameters or {}).items()}}
        with self.session.post(self.url, params=params, data=sql.encode(), auth=self.auth,
                               timeout=(3, seconds + 5), stream=True) as response:
            response.raise_for_status()
            response.raw.decode_content = True
            for line in chunks(response.raw, max_bytes=1024 ** 4):
                if line.strip():
                    yield json.loads(line)

    def insert(self, table, rows):
        if not rows:
            return
        if table not in {"results", "quality", "receipts", "deliveries", "visibility"}:
            raise ValueError("Unknown table")
        sql = f"INSERT INTO adpulse.{table} FORMAT JSONEachRow\n" + "\n".join(canonical(r) for r in rows)
        response = self.session.post(self.url, data=sql.encode(), auth=self.auth, timeout=60)
        response.raise_for_status()

    def snapshots(self, release, kind=None):
        restriction = " AND record_type={kind:String}" if kind else ""
        rows = self.query("""SELECT output_key, argMax(payload, output_offset) AS payload
            FROM adpulse.results WHERE release_id={release:String}""" + restriction + " GROUP BY output_key FORMAT JSONEachRow", {"release": release, **({"kind": kind} if kind else {})})
        payloads = [json.loads(r["payload"]) for r in rows]
        return {"release_id": release, "metrics": sorted([r for r in payloads if r["record_type"] == "metric"], key=lambda r: r["metric_key"]),
                "associations": sorted([r for r in payloads if r["record_type"] == "association"], key=lambda r: r["association_key"])}

    def page(self, release, kind, *, limit=100, after="", filters=None, budget=None):
        """Bounded serving path. Filter mutable payload fields only AFTER argMax.

        Full snapshots remain available internally for exhaustive reconciliation.
        A row limit bounds transfer, not scan work; independent server budgets bound scans.
        """
        fields = {"metric": {"cohort": "cohort_basis", "campaign": "campaign_id", "region": "region",
                             "app_version": "app_version"}, "association": {"status": "status"}}
        filters, budget = filters or {}, budget or QueryBudget()
        if (not valid_release(release) or kind not in fields or type(limit) is not int
                or not 1 <= limit <= MAX_PAGE_SIZE or not isinstance(after, str) or len(after) > 1500
                or set(filters) - fields[kind].keys()
                or any(not isinstance(v, str) or len(v) > 200 for v in filters.values())):
            raise ValueError("Invalid page query")
        predicates = [f"JSONExtractString(payload,'{fields[kind][key]}')={{filter_{key}:String}}" for key in filters]
        where = " AND ".join(predicates) or "1"
        sql = """SELECT output_key, JSONMergePatch(payload,'{"receipt_updates":null}') AS public_payload
            FROM (SELECT output_key, argMax(payload, output_offset) AS payload
                  FROM adpulse.results WHERE release_id={release:String} AND record_type={kind:String}
                    AND output_key>{after:String} GROUP BY output_key)
            WHERE """ + where + " ORDER BY output_key LIMIT {fetch_limit:UInt32} FORMAT JSONEachRow"
        params = {"param_release": release, "param_kind": kind, "param_after": after,
                  "param_fetch_limit": limit + 1, **{f"param_filter_{k}": v for k, v in filters.items()},
                  "max_execution_time": budget.seconds, "timeout_overflow_mode": "throw",
                  "max_memory_usage": budget.memory_bytes, "max_rows_to_read": budget.read_rows,
                  "read_overflow_mode": "throw", "max_result_bytes": budget.result_bytes,
                  "result_overflow_mode": "throw", "max_threads": 2, "wait_end_of_query": 1,
                  "buffer_size": budget.result_bytes}
        response = None
        try:
            response = self.session.post(self.url, params=params, data=sql.encode(), auth=self.auth,
                                         timeout=(3, budget.seconds + 3), stream=True)
            response.raise_for_status()
            body = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                if len(body) + len(chunk) > budget.result_bytes:
                    raise PageQueryError("budget")
                body.extend(chunk)
            entries = [json.loads(line) for line in body.splitlines() if line.strip()]
            if len(entries) > limit + 1:
                raise ValueError("Server violated page bound")
            rows = [json.loads(r["public_payload"]) for r in entries[:limit]]
            for row in rows:
                row.pop("receipt_updates", None)
            return rows, entries[limit - 1]["output_key"] if len(entries) > limit else None
        except requests.Timeout as exc:
            raise PageQueryError("timeout") from exc
        except requests.HTTPError as exc:
            # No raw SQL, credentials or database exception text goes to clients/metric labels.
            body = exc.response.text if exc.response is not None else ""
            reason = "budget" if any(code in body for code in (
                "TOO_MANY_ROWS", "TOO_MANY_BYTES", "TIMEOUT_EXCEEDED", "MEMORY_LIMIT_EXCEEDED")) else "unavailable"
            raise PageQueryError(reason) from exc
        except requests.RequestException as exc:
            raise PageQueryError("unavailable") from exc
        except (ValueError, KeyError, TypeError) as exc:
            raise PageQueryError("invalid_response") from exc
        finally:
            if response is not None:
                response.close()

    def write_batch(self, messages):
        groups = {"results": [], "quality": [], "receipts": [], "visibility": []}
        deliveries, batch_offsets, routes = [], {}, {}
        parsed = [(m, json.loads(m.value())) for m in messages]
        identities = [[m.topic(), m.partition(), m.offset()] for m, _ in parsed]
        old_deliveries = []
        for parameter in lookup_parameters(identities):
            old_deliveries.extend(self.query("""SELECT topic,partition_id,offset_id,hash FROM adpulse.deliveries
            WHERE (topic,partition_id,offset_id) IN
              (SELECT JSONExtractString(x,1),JSONExtractUInt(x,2),JSONExtractUInt(x,3)
               FROM (SELECT arrayJoin(JSONExtractArrayRaw({keys:String})) AS x)) FORMAT JSONEachRow""",
                {"keys": parameter}))
        old_hashes = {(r["topic"], r["partition_id"], r["offset_id"]): r["hash"] for r in old_deliveries}
        requested_routes = [[p["release_id"], p["output_key"]] for _, p in parsed if p.get("record_type") in {"metric", "association"}]
        old_routes = []
        for parameter in lookup_parameters(requested_routes):
            old_routes.extend(self.query("""SELECT release_id,output_key,any(output_topic) AS topic,any(output_partition) AS partition
            FROM adpulse.results WHERE (release_id,output_key) IN
              (SELECT JSONExtractString(x,1),JSONExtractString(x,2)
               FROM (SELECT arrayJoin(JSONExtractArrayRaw({keys:String})) AS x))
            GROUP BY release_id,output_key FORMAT JSONEachRow""", {"keys": parameter}))
        previous_routes = {(r["release_id"], r["output_key"]): (r["topic"], r["partition"]) for r in old_routes}
        for message, payload in parsed:
            topic, partition, offset = message.topic(), message.partition(), message.offset()
            sha = digest(payload)
            identity = (topic, partition, offset)
            if identity in batch_offsets and batch_offsets[identity] != sha:
                raise ValueError("Conflicting content in batch")
            batch_offsets[identity] = sha
            if identity in old_hashes and old_hashes[identity] != sha:
                raise ValueError("Same Kafka offset has conflicting content")
            kind = payload.get("record_type")
            if kind in {"metric", "association"}:
                release, key = payload["release_id"], payload["output_key"]
                if topic != f"{os.getenv('TOPIC_PREFIX', 'adpulse')}.results.{release}":
                    raise ValueError("Result release/topic mismatch")
                route_id = (release, key)
                if route_id in routes and routes[route_id] != (topic, partition):
                    raise ValueError("Key changed partition within batch")
                routes[route_id] = (topic, partition)
                if route_id in previous_routes and previous_routes[route_id] != (topic, partition):
                    raise ValueError("Key changed partition; create new release")
                groups["results"].append(dict(release_id=release, output_key=key, record_type=kind,
                                             output_topic=topic, output_partition=partition, output_offset=offset,
                                             payload=canonical(payload), hash=sha, received_at=payload.get("received_at", 0)))
                for receipt in payload.get("receipt_updates", []):
                    groups["visibility"].append(dict(release_id=release, receipt_id=receipt["receipt_id"],
                                                     received_at=receipt["received_at"], event_time=receipt["event_time"],
                                                     output_topic=topic, output_partition=partition, output_offset=offset))
            elif kind == "quality":
                groups["quality"].append(dict(output_topic=topic, output_partition=partition, output_offset=offset,
                                             receipt_id=payload.get("receipt_id", ""), disposition=payload["disposition"],
                                             error_code=payload["error_code"], rule_version=payload["rule_version"], payload=canonical(payload)))
            elif kind == "receipt":
                groups["receipts"].append(dict(batch_id=payload["batch_id"], received_at=payload["received_at"],
                                              receipt_ids=payload["receipt_ids"], payload=canonical(payload)))
            else:
                raise ValueError(f"Unrecognized sink record: {kind}")
            deliveries.append(dict(topic=topic, partition_id=partition, offset_id=offset, hash=sha))
        # Persist immutable delivery hashes BEFORE any output. A crash between CH
        # requests cannot bypass conflict checking on the next attempt.
        self.insert("deliveries", deliveries)
        for table, rows in groups.items():
            self.insert(table, rows)
