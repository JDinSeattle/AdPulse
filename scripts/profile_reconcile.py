"""Measured entry point for the actual archived-input reconciliation container."""
import argparse
import json
import resource
import time
from pathlib import Path

from adpulse.common import write_json
from adpulse.reconciliation import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--index", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    start = time.monotonic()
    report = run(args.index, args.output.with_suffix(".sqlite"))
    usage = resource.getrusage(resource.RUSAGE_SELF)
    report.update(wall_seconds=time.monotonic()-start, process_peak_rss_kib=usage.ru_maxrss,
                  user_cpu_seconds=usage.ru_utime, system_cpu_seconds=usage.ru_stime,
                  cgroup_memory_peak_bytes=int(Path('/sys/fs/cgroup/memory.peak').read_text()),
                  cgroup_memory_max_bytes=int(Path('/sys/fs/cgroup/memory.max').read_text()),
                  cgroup_cpu_max=Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
                  cgroup_swap_max_bytes=int(Path('/sys/fs/cgroup/memory.swap.max').read_text()),
                  cgroup_memory_events=Path('/sys/fs/cgroup/memory.events').read_text().strip(),
                  environment="Actual local Kafka/Flink/ClickHouse/S3 outputs; independent disk-backed reference in a resource-limited Docker container")
    write_json(args.output, report)
    print(json.dumps(report), flush=True)
    raise SystemExit(0 if report["passed"] else 1)
