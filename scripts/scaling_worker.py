"""One resource-controlled, cold-index archive/oracle measurement.

Mounted against either the immutable baseline package or candidate package.
Both include input reads, verification, calculation and complete output hashing.
"""
import argparse
import hashlib
import json
import resource
import time
from contextlib import closing
from pathlib import Path

from adpulse.archive import LocalObjects, verified_records
from adpulse.common import canonical, write_json
from adpulse.oracle import calculate


def fingerprints(result):
    output = {}
    for kind in ("quality", "metrics", "associations"):
        count, sha = 0, hashlib.sha256()
        for row in result[kind]:
            sha.update((canonical(row)+"\n").encode())
            count += 1
        output[kind] = dict(count=count, sha256=sha.hexdigest())
    output["metadata"] = {k: v for k, v in result.items() if k not in output}
    return output


def measure(mode, fixture, output):
    if output.exists():
        raise FileExistsError(output)
    started = time.monotonic()
    objects = LocalObjects(fixture / "objects")
    # This tiny reference warmup initializes schema machinery for both paths;
    # it does not read the fixture or create an index.
    calculate([])
    warmup = time.monotonic()-started
    started = time.monotonic()
    if mode == "memory":
        rows = verified_records(objects)
        packets = [r["value"] for r in rows if r["topic"].endswith(".raw")]
        packets.sort(key=lambda p: (p["received_at"], p["batch_id"], p["index"]))
        result = calculate(packets, release_id="fixed-fixture")
        proof = fingerprints(result)
        disk_bytes = 0
    else:
        from adpulse.archive_index import ArchiveIndex
        from adpulse.disk import DiskWorkspace
        with closing(ArchiveIndex(output.with_suffix(".index.sqlite"))) as index:
            index.refresh(objects)
            index.coverage()
            with closing(DiskWorkspace(output.with_suffix(".oracle.sqlite"))) as workspace:
                result = calculate(index.packets(), release_id="fixed-fixture", storage=workspace)
                proof = fingerprints(result)
        disk_bytes = sum(p.stat().st_size for p in output.parent.glob(output.stem + ".*.sqlite*"))
    elapsed = time.monotonic()-started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    cgroup = Path('/sys/fs/cgroup')
    report = dict(mode=mode, elapsed_seconds=elapsed, warmup_seconds=warmup,
                  user_cpu_seconds=usage.ru_utime, system_cpu_seconds=usage.ru_stime,
                  process_peak_rss_kib=usage.ru_maxrss, scratch_disk_bytes=disk_bytes,
                  output_fingerprints=proof,
                  cgroup={name: (cgroup/name).read_text().strip() for name in
                          ('cpu.max', 'cpuset.cpus.effective', 'memory.max', 'memory.peak', 'memory.swap.max', 'memory.events')},
                  measurement="Includes archive verification, input ordering, full independent reference calculation and all output hashing; per-run fresh disk indexes; OS file cache shared/warm, not dropped",
                  fixture=json.loads((fixture / 'fixture.json').read_text())["file_manifest_sha256"])
    write_json(output, report)
    print(canonical(report), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=['memory', 'disk'], required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    measure(args.mode, args.fixture, args.output)
