"""Actual deployed HTTP/index read-model acceptance; no synthetic dependency stubs."""
from __future__ import annotations

import argparse
import concurrent.futures
import statistics
import time
from pathlib import Path

import requests

from adpulse.common import write_json
from adpulse.storage import ClickHouse


def run(url, output, timeout=900):
    started = time.monotonic()
    while True:
        response = requests.get(url+'/v1/reconcile',timeout=10)
        if response.status_code == 200:
            coverage = response.json()
            if coverage['archive_complete'] and coverage['lineage_complete']:
                break
        if time.monotonic()-started > timeout:
            raise TimeoutError('Background source inspection not complete/fresh')
        time.sleep(3)
    db = ClickHouse()
    receipt = db.query('SELECT receipt_id FROM adpulse.quality FINAL WHERE disposition=\'cleaned\' LIMIT 1 FORMAT JSONEachRow')[0]['receipt_id']

    def request(path):
        began = time.perf_counter()
        response = requests.get(url+path,timeout=10)
        response.raise_for_status()
        if path.startswith('/v1/trace/'):
            row = response.json()
            assert row['receipt_id'] == receipt and row['raw'] and row['quality']
            assert all(r['value']['receipt_id'] == receipt for r in row['raw'])
        return dict(path=path,seconds=time.perf_counter()-began,bytes=len(response.content))

    paths = ['/v1/trace/'+receipt,'/v1/reconcile','/metrics']
    # Warm each read path once, then exercise 48 requests from 16 clients.
    for path in paths:
        request(path)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        observations = list(pool.map(request,paths*16))
    metrics = requests.get(url+'/metrics',timeout=10).text
    assert 'adpulse_inspection_snapshot_ready{kind="coverage"} 1.0' in metrics
    assert 'adpulse_inspection_snapshot_ready{kind="metrics"} 1.0' in metrics
    report = dict(passed=True,mode='actual local Docker HTTP, verified S3 archive index and ClickHouse-derived lineage',
                  coverage=coverage,concurrent_requests=48,client_workers=16,observations=observations,
                  median_seconds=statistics.median(r['seconds'] for r in observations),
                  max_seconds=max(r['seconds'] for r in observations),
                  snapshot_boundary='Responses expose check timestamps; no distributed MVCC or per-request re-audit claim')
    write_json(output,report)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://localhost:8080')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--timeout',type=float,default=900)
    args=parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    run(args.url,args.output,args.timeout)
