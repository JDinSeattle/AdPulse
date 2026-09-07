"""Disk-backed containers for the independent batch oracle, not Flink state.

SQLite page cache is a budget, not a hard process limit. Experiments additionally
enforce cgroup limits. Iterators never fetch the complete result into Python.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .common import canonical


def connect(path, *, readonly=False, cache_mib=32):
    path = Path(path).resolve()
    if readonly:
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path, timeout=2)
    db.execute(f"PRAGMA cache_size=-{int(cache_mib) * 1024}")
    db.execute("PRAGMA mmap_size=0")
    db.execute("PRAGMA temp_store=FILE")
    return db


def tuple_key(value):
    return tuple(tuple_key(x) for x in value) if isinstance(value, list) else value


class DiskMap:
    def __init__(self, workspace, name, pairs=()):
        self.store, self.name = workspace, name
        workspace.db.execute(f'CREATE TABLE "{name}" (seq INTEGER PRIMARY KEY, key TEXT UNIQUE, value TEXT)')
        for key, value in pairs:
            self[key] = value

    def get(self, key, default=None):
        row = self.store.db.execute(f'SELECT value FROM "{self.name}" WHERE key=?', (canonical(key),)).fetchone()
        return json.loads(row[0]) if row else default

    def __getitem__(self, key):
        marker = object()
        value = self.get(key, marker)
        if value is marker:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        self.store.db.execute(f'INSERT INTO "{self.name}"(key,value) VALUES(?,?) '
                              'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (canonical(key), canonical(value)))
        self.store.tick()

    def __contains__(self, key):
        return self.store.db.execute(f'SELECT 1 FROM "{self.name}" WHERE key=?', (canonical(key),)).fetchone() is not None

    def add(self, key):
        self[key] = True

    def setdefault(self, key, default):
        marker = object()
        value = self.get(key, marker)
        if value is marker:
            self[key] = default
            return default
        return value

    def items(self):
        for key, value in self.store.db.execute(f'SELECT key,value FROM "{self.name}" ORDER BY seq'):
            yield tuple_key(json.loads(key)), json.loads(value)

    def values(self):
        for (value,) in self.store.db.execute(f'SELECT value FROM "{self.name}" ORDER BY seq'):
            yield json.loads(value)

    def ordered(self):
        # Canonical string keys escape characters, so sort the decoded key using
        # SQLite BINARY collation to match Python string order, not JSON escaping.
        for (value,) in self.store.db.execute(f'SELECT value FROM "{self.name}" ORDER BY json_extract(key,\'$\')'):
            yield json.loads(value)

    def __len__(self):
        return self.store.db.execute(f'SELECT count(*) FROM "{self.name}"').fetchone()[0]


class DiskSequence:
    def __init__(self, workspace, name):
        self.store, self.name = workspace, name
        workspace.db.execute(f'CREATE TABLE "{name}" (seq INTEGER PRIMARY KEY, value TEXT)')

    def append(self, value):
        self.store.db.execute(f'INSERT INTO "{self.name}"(value) VALUES(?)', (canonical(value),))
        self.store.tick()

    def __iter__(self):
        for (value,) in self.store.db.execute(f'SELECT value FROM "{self.name}" ORDER BY seq'):
            yield json.loads(value)

    def ordered(self, field):
        if field != "association_key":
            raise ValueError("Unknown sort field")
        for (value,) in self.store.db.execute(f'SELECT value FROM "{self.name}" ORDER BY json_extract(value,\'$.association_key\')'):
            yield json.loads(value)

    def __len__(self):
        return self.store.db.execute(f'SELECT count(*) FROM "{self.name}"').fetchone()[0]


class DiskWorkspace:
    def __init__(self, path, cache_mib=32):
        self.path = Path(path)
        # Never overwrite another run or reuse partial reference state.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=False)
        self.db = connect(self.path, cache_mib=cache_mib)
        self.writes, self.input_records = 0, 0

    def tick(self):
        self.writes += 1
        if self.writes % 10000 == 0:
            self.db.commit()

    def mapping(self, name, pairs=()):
        if not name.isidentifier():
            raise ValueError("Invalid internal table name")
        return DiskMap(self, name, pairs)

    def sequence(self, name):
        if not name.isidentifier():
            raise ValueError("Invalid internal table name")
        return DiskSequence(self, name)

    def close(self):
        self.db.commit()
        self.db.close()

