"""Incremental verified archive index with per-manifest atomic publication.

This is a rebuildable local cache, not another archive authority. Incremental
refresh trusts previously checked immutable objects; audit=True rechecks them.
Memory scales with configured object/line/cache bounds, not total archive rows.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from contextlib import closing

from .common import canonical, digest
from .disk import connect

MAX_MANIFEST = 8 * 1024 * 1024
MAX_LINE = 2 * 1024 * 1024
MAX_OBJECT = 64 * 1024 * 1024


def chunks(stream, *, max_line=MAX_LINE, max_bytes=MAX_OBJECT):
    buffer = bytearray()
    total = 0
    while block := stream.read(64 * 1024):
        total += len(block)
        if total > max_bytes:
            raise ValueError("Archive object exceeds byte budget")
        buffer.extend(block)
        while (end := buffer.find(b"\n")) >= 0:
            if end > max_line:
                raise ValueError("Archive line exceeds byte budget")
            yield bytes(buffer[:end + 1])
            del buffer[:end + 1]
        if len(buffer) > max_line:
            raise ValueError("Archive line exceeds byte budget")
    if buffer:
        yield bytes(buffer)


class ArchiveIndex:
    def __init__(self, path, *, readonly=False):
        self.db = connect(path, readonly=readonly)
        if readonly:
            return
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS manifests(key TEXT PRIMARY KEY, hash TEXT NOT NULL, checked_at REAL);
          CREATE TABLE IF NOT EXISTS records(
            topic TEXT, part INTEGER, off INTEGER, row_json TEXT, hash TEXT,
            receipt TEXT, received_at INTEGER, batch TEXT, batch_index INTEGER,
            kind TEXT, manifest TEXT, ordinal INTEGER, PRIMARY KEY(topic,part,off));
          CREATE INDEX IF NOT EXISTS receipt_lookup ON records(receipt) WHERE kind='raw';
          CREATE INDEX IF NOT EXISTS receipt_identity ON records(receipt,kind);
          CREATE INDEX IF NOT EXISTS packet_order ON records(received_at,batch,batch_index) WHERE kind='raw';
          CREATE INDEX IF NOT EXISTS record_kind ON records(kind);
          CREATE TABLE IF NOT EXISTS ack_refs(receipt TEXT PRIMARY KEY, topic TEXT, part INTEGER, off INTEGER, hash TEXT);
          CREATE TABLE IF NOT EXISTS expected(receipt TEXT PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS lineage(receipt TEXT PRIMARY KEY, disposition TEXT);
          CREATE TABLE IF NOT EXISTS trace_quality(receipt TEXT, hash TEXT, payload TEXT, PRIMARY KEY(receipt,hash));
          CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT);
        """)

    def close(self):
        self.db.close()

    def refresh(self, objects, *, audit=False):
        scanned, verified, rows = 0, 0, 0
        self.db.execute("DROP TABLE IF EXISTS temp.scan_keys")
        self.db.execute("CREATE TEMP TABLE scan_keys(key TEXT PRIMARY KEY)")
        for key in objects.iter_keys("manifests/"):
            scanned += 1
            self.db.execute("INSERT INTO scan_keys VALUES(?)", (key,))
            old = self.db.execute("SELECT hash FROM manifests WHERE key=?", (key,)).fetchone()
            if old and not audit:
                continue
            with closing(objects.open(key)) as stream:
                raw_manifest = stream.read(MAX_MANIFEST + 1)
            if len(raw_manifest) > MAX_MANIFEST:
                raise ValueError("Archive manifest exceeds byte budget")
            if old and old[0] != hashlib.sha256(raw_manifest).hexdigest():
                raise ValueError("Immutable manifest changed")
            manifest = json.loads(raw_manifest)
            if not re.fullmatch(r"data/[0-9a-f]{64}\.jsonl", manifest["data_key"]):
                raise ValueError("Invalid content-addressed data key")
            if manifest.get("format_version") != 1 or len(manifest["records"]) != manifest["record_count"]:
                raise ValueError("Archive manifest count/version mismatch")
            hasher, count = hashlib.sha256(), 0
            # No rows from a partially checked object become visible to readers.
            with self.db:
                with closing(objects.open(manifest["data_key"])) as stream:
                    for ordinal, line in enumerate(chunks(stream)):
                        hasher.update(line)
                        if ordinal >= manifest["record_count"]:
                            raise ValueError("Archive count mismatch")
                        row, ref = json.loads(line), manifest["records"][ordinal]
                        identity = row["topic"], row["partition"], row["offset"]
                        value_hash = digest(row["value"])
                        if identity != (ref["topic"], ref["partition"], ref["offset"]) or value_hash != ref["sha256"]:
                            raise ValueError("Archive offset/content mismatch")
                        serialized = canonical(row)
                        existing = self.db.execute("SELECT row_json FROM records WHERE topic=? AND part=? AND off=?", identity).fetchone()
                        if existing and existing[0] != serialized:
                            raise ValueError("Conflicting archived offset")
                        value = row["value"]
                        kind = "raw" if row["topic"].endswith(".raw") else "receipt" if row["topic"].endswith(".receipts") else "dimension"
                        packet = value if kind == "raw" else {}
                        self.db.execute("INSERT OR IGNORE INTO records VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                        (*identity, serialized, value_hash, packet.get("receipt_id"), packet.get("received_at"),
                                         packet.get("batch_id", ""), packet.get("index", ordinal), kind, key, ordinal))
                        if kind == "receipt":
                            self._acknowledgements(value)
                        count += 1
                if count != manifest["record_count"] or hasher.hexdigest() != manifest["sha256"]:
                    raise ValueError("Archive checksum/count mismatch")
                self.db.execute("INSERT OR REPLACE INTO manifests VALUES(?,?,?)",
                                (key, hashlib.sha256(raw_manifest).hexdigest(), time.time()))
            verified += 1
            rows += count
        with self.db:
            if self.db.execute("SELECT key FROM manifests WHERE key NOT IN (SELECT key FROM scan_keys) LIMIT 1").fetchone():
                raise ValueError("Previously indexed commit manifest is missing")
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES('indexed_at',?)", (str(time.time()),))
        return dict(listed_manifests=scanned, verified_manifests=verified, verified_rows=rows, audit=audit)

    def _acknowledgements(self, manifest):
        refs = manifest["records"]
        if (manifest["count"] != len(refs) or len(set(manifest["receipt_ids"])) != len(refs)
                or set(manifest["receipt_ids"]) != {r["receipt_id"] for r in refs}):
            raise ValueError("Receipt manifest count/identity mismatch")
        for ref in refs:
            values = ref["topic"], ref["partition"], ref["offset"], ref["sha256"]
            old = self.db.execute("SELECT topic,part,off,hash FROM ack_refs WHERE receipt=?", (ref["receipt_id"],)).fetchone()
            if old and old != values:
                raise ValueError("Conflicting receipt acknowledgement")
            self.db.execute("INSERT OR IGNORE INTO ack_refs VALUES(?,?,?,?,?)", (ref["receipt_id"], *values))

    def coverage(self, expected=(), lineage=(), *, sample_limit=100):
        if not 0 <= sample_limit <= 1000:
            raise ValueError("Invalid difference sample budget")
        with self.db:
            self.db.execute("DELETE FROM expected")
            self.db.execute("DELETE FROM lineage")
            self.db.execute("DELETE FROM trace_quality")
            self.db.executemany("INSERT OR IGNORE INTO expected VALUES(?)", ((r,) for r in expected))
            self.db.execute("INSERT OR IGNORE INTO expected SELECT receipt FROM ack_refs")
            for row in lineage:
                if "payload" in row:
                    payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
                    if payload["receipt_id"] != row["receipt_id"] or payload["disposition"] != row["disposition"]:
                        raise ValueError("Quality payload identity mismatch")
                    self.db.execute("INSERT OR IGNORE INTO trace_quality VALUES(?,?,?)",
                                    (row["receipt_id"], digest(payload), canonical(payload)))
                if row["disposition"] == "signal":
                    continue
                old = self.db.execute("SELECT disposition FROM lineage WHERE receipt=?", (row["receipt_id"],)).fetchone()
                if old and old[0] != row["disposition"]:
                    raise ValueError("Receipt has conflicting lineage dispositions")
                self.db.execute("INSERT OR IGNORE INTO lineage VALUES(?,?)", (row["receipt_id"], row["disposition"]))
            conflicts = self.db.execute("""SELECT a.receipt FROM ack_refs a JOIN records r ON r.receipt=a.receipt AND r.kind='raw'
                WHERE a.topic!=r.topic OR a.part!=r.part OR a.off!=r.off OR a.hash!=r.hash LIMIT 1""").fetchone()
            if conflicts:
                raise ValueError("Acknowledgement and archive disagree")
            duplicate = self.db.execute("SELECT receipt FROM records INDEXED BY receipt_identity WHERE kind='raw' GROUP BY receipt HAVING count(*)>1 LIMIT 1").fetchone()
            if duplicate:
                raise ValueError("Receipt maps to multiple source identities")
            missing_sql = "SELECT e.receipt FROM expected e WHERE NOT EXISTS(SELECT 1 FROM records r INDEXED BY receipt_identity WHERE r.kind='raw' AND r.receipt=e.receipt)"
            pending_sql = "SELECT e.receipt FROM expected e WHERE NOT EXISTS(SELECT 1 FROM lineage l WHERE l.receipt=e.receipt)"
            count = self.db.execute("SELECT count(*) FROM expected").fetchone()[0]
            missing = self.db.execute("SELECT count(*) FROM (" + missing_sql + ")").fetchone()[0]
            pending = self.db.execute("SELECT count(*) FROM (" + pending_sql + ")").fetchone()[0]
            return dict(acknowledged=count, archived=count-missing, classified=count-pending,
                        archive_missing_count=missing, processing_pending_count=pending,
                        archive_missing=[r[0] for r in self.db.execute(missing_sql + " ORDER BY e.receipt LIMIT ?", (sample_limit,))],
                        processing_pending=[r[0] for r in self.db.execute(pending_sql + " ORDER BY e.receipt LIMIT ?", (sample_limit,))],
                        sample_limit=sample_limit, archive_complete=bool(count) and not missing,
                        lineage_complete=bool(count) and not pending)

    def packets(self):
        for (value,) in self.db.execute("""SELECT r.row_json FROM records r JOIN expected e ON e.receipt=r.receipt
            WHERE r.kind='raw' ORDER BY r.received_at,r.batch,r.batch_index,r.topic,r.part,r.off"""):
            yield json.loads(value)["value"]

    def dimensions(self):
        # CDC is explicitly a small broadcast table; cap rather than silently
        # allowing an unbounded Python dictionary in the reference implementation.
        from .cli import archived_dimensions
        def limited_rows():
            total, number = 0, 0
            # Quality/quarantine are also archived. Only actual CDC topics count
            # toward the explicitly small broadcast-dimension budget. Enumerate
            # topic names through the covering source-identity index first.
            for (topic,) in self.db.execute("SELECT topic FROM records GROUP BY topic ORDER BY topic"):
                if not topic.endswith("campaign_versions"):
                    continue
                for (row,) in self.db.execute("SELECT row_json FROM records WHERE topic=? ORDER BY part,off", (topic,)):
                    number += 1
                    total += len(row.encode())
                    if number > 10000 or total > 16 * 1024 * 1024:
                        raise ValueError("Dimension history exceeds reference-table limit")
                    yield json.loads(row)
        return archived_dimensions(limited_rows())

    def trace(self, receipt, *, limit=10):
        rows = self.db.execute("SELECT row_json FROM records INDEXED BY receipt_identity WHERE kind='raw' AND receipt=? LIMIT ?", (receipt, limit+1)).fetchall()
        if len(rows) > limit:
            raise ValueError("Trace exceeds source identity budget")
        return [json.loads(row[0]) for row in rows]

    def quality(self, receipt, *, limit=100):
        rows = self.db.execute("SELECT payload FROM trace_quality WHERE receipt=? ORDER BY hash LIMIT ?", (receipt, limit+1)).fetchall()
        if len(rows) > limit:
            raise ValueError("Trace exceeds quality record budget")
        return [json.loads(row[0]) for row in rows]
