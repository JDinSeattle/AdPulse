"""Compare projected latest-value queries to full payloads on an immutable release."""
from __future__ import annotations

import argparse
import json
import tempfile
import time
from contextlib import closing
from pathlib import Path

from adpulse.common import write_json
from adpulse.disk import DiskWorkspace
from adpulse.reconciliation import actual_results, compare_streams
from adpulse.storage import ClickHouse


def run(release, output):
    db = ClickHouse()
    started = time.monotonic()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temp:
        root = Path(temp)

        def baseline(kind):
            for row in db.spooled_query("""SELECT argMax(payload,output_offset) AS payload
                FROM adpulse.results WHERE release_id={release:String} AND record_type={kind:String}
                GROUP BY output_key FORMAT JSONEachRow""",
                                       {"release": release, "kind": kind[:-1]}, directory=root):
                yield json.loads(row['payload'])

        with closing(DiskWorkspace(root/'comparison.sqlite')) as workspace:
            comparison = compare_streams(workspace,
                                         {k: baseline(k) for k in ('metrics', 'associations')},
                                         {k: actual_results(db, k, release, root) for k in ('metrics', 'associations')})
        assert comparison['passed'] and comparison['actual_associations'] > 0
        full_started = time.monotonic()
        counts = {k: sum(1 for _ in actual_results(db, k, 'live-v1', root)) for k in ('metrics', 'associations')}
    report = dict(passed=True, release=release, comparison=comparison, live_counts=counts,
                  live_read_seconds=time.monotonic()-full_started, elapsed_seconds=time.monotonic()-started,
                  environment='Real local or hosted-runner ClickHouse; 512 MiB / 120 s query limits, 64 MiB external aggregation threshold, 4 GiB query spill and 8 GiB HTTP spool caps',
                  boundary='Equality against full payloads applies to the named immutable release; live counts verify bounded result reading only, not business equality.')
    write_json(output, report)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    run(args.release, args.output)
