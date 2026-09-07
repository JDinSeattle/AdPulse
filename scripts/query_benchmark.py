"""Reproduce legacy full materialization versus a bounded first page on the SAME release."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import statistics
import time
import tracemalloc
from pathlib import Path

from adpulse.api import associations
from adpulse.common import write_json
from adpulse.storage import ClickHouse


def legacy(release, limit=None):
    # Preserved 0.1.0 serving algorithm: fetch and deserialize the whole release first.
    result = {"release_id": release, "associations": ClickHouse().snapshots(release, "association")["associations"]}
    if limit is not None:
        result["associations"] = result["associations"][:limit]
    return result


def run(release, output, repeats=5):
    variants = {"legacy_full": lambda: legacy(release), "legacy_first_100": lambda: legacy(release, 100),
                "paged_first_100": lambda: associations(release, limit=100)}
    measurements = {name: [] for name in variants}
    for fn in variants.values():
        fn()
    for iteration in range(repeats):
        # Alternate order to reduce systematic cache/order bias; tracing is on for all variants.
        for name in list(variants)[::1 if iteration % 2 == 0 else -1]:
            tracemalloc.start()
            start = time.perf_counter()
            result = variants[name]()
            elapsed = time.perf_counter() - start
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            measurements[name].append(dict(seconds=elapsed, peak_python_bytes=peak,
                                           response_bytes=len(json.dumps(result, separators=(",", ":")).encode()),
                                           rows=len(result["associations"])))
    expected, actual = legacy(release, 100)["associations"], associations(release, limit=100)["associations"]
    for row in expected:
        row.pop("receipt_updates", None)
    assert expected == actual, "First pages must contain identical latest business records"
    db = ClickHouse()
    summary = {name: {field: statistics.median(r[field] for r in values)
                      for field in ("seconds", "peak_python_bytes", "response_bytes", "rows")}
               for name, values in measurements.items()}
    report = dict(passed=True, release=release, measurements=measurements, medians=summary,
                  environment=dict(platform=platform.platform(), python=platform.python_version(),
                                   clickhouse=db.query("SELECT version() AS version FORMAT JSONEachRow")[0]["version"],
                                   packages={p: importlib.metadata.version(p) for p in ("fastapi", "starlette", "requests")}),
                  method="One warmup per variant, alternating order, five repeats by default; real local ClickHouse; tracemalloc enabled; in-process endpoint calls excluding ASGI wire overhead and response serialization from latency/peak measurements",
                  scope="Same immutable release; first-page efficiency and transfer bounds, not total-export throughput, streaming ingestion speed, production latency or steady-state capacity",
                  source_sha256={p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
                                 for p in ("scripts/query_benchmark.py", "adpulse/api.py", "adpulse/storage.py")})
    write_json(output, report)
    print(json.dumps({"passed": True, "medians": summary, "output": output}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, help="Use a validated, immutable replay release")
    parser.add_argument("--output", default="artifacts/query-benchmark.json")
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Use at least three measured repetitions")
    run(args.release, args.output, args.repeats)
