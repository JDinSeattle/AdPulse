"""Paired HTTP serving experiment; real ClickHouse, local synthetic archive fixture.

Baseline verifies the full archive on every trace. Candidate explicitly serves
precomputed/indexed data. Setup and refresh are measured separately from requests.
The fixture receipt IDs do not exist in the business DB: both quality results are
empty. Actual full-history trace/lineage correctness has a separate acceptance.
"""
from __future__ import annotations

import argparse
import hashlib
import resource
import threading
import time
from pathlib import Path

import requests
import uvicorn

from adpulse import api
from adpulse.archive import LocalObjects
from adpulse.common import canonical, write_json


def run(mode, fixture, output):
    if output.exists():
        raise FileExistsError(output)
    root = output.parent / (output.stem+'-index')
    root.mkdir(exist_ok=False)
    metrics = {'db_requests':0}
    original_post = requests.Session.post

    def counted(session, *args, **kwargs):
        metrics['db_requests'] += 1
        return original_post(session,*args,**kwargs)
    requests.Session.post = counted
    objects = LocalObjects(fixture/'objects')
    began = time.monotonic()
    if mode == 'memory':
        api.S3Objects = lambda: objects
    else:
        import os
        from contextlib import closing
        from adpulse.archive_index import ArchiveIndex
        from adpulse.inspection import operational_metrics, publish
        os.environ['ADPULSE_INSPECTION_DIR'] = str(root)
        with closing(ArchiveIndex(root/'archive.sqlite')) as index:
            index.refresh(objects)
            coverage = index.coverage()
        publish(root,'coverage',coverage,started_at=time.time()-(time.monotonic()-began))
        refresh_started = time.monotonic()
        publish(root,'metrics',operational_metrics(),started_at=time.time())
        metrics['refresh_seconds'] = time.monotonic()-refresh_started
    setup_seconds = time.monotonic()-began
    setup_db_requests = metrics['db_requests']
    server = uvicorn.Server(uvicorn.Config(api.app,host='127.0.0.1',port=8080,log_level='error'))
    thread = threading.Thread(target=server.run,daemon=True)
    thread.start()
    deadline = time.monotonic()+20
    while not server.started:
        if time.monotonic()>deadline:
            raise TimeoutError('HTTP server failed to start')
        time.sleep(0.05)
    results = []
    try:
        for route in ('trace','metrics'):
            for repeat in range(5):
                path = '/v1/trace/fixture-000000:'+str(repeat) if route=='trace' else '/metrics'
                start, calls = time.perf_counter(), metrics['db_requests']
                response = requests.get('http://127.0.0.1:8080'+path,timeout=120)
                elapsed = time.perf_counter()-start
                response.raise_for_status()
                if route=='trace':
                    result = response.json()
                    semantic = {k:result[k] for k in ('receipt_id','raw','quality')}
                    assert semantic['raw'] and not semantic['quality']
                    fingerprint = hashlib.sha256(canonical(semantic).encode()).hexdigest()
                else:
                    assert 'adpulse_flink_running_jobs' in response.text
                    fingerprint = None  # Dated monitoring samples are not byte-identical snapshots.
                results.append(dict(route=route,repeat=repeat+1,seconds=elapsed,bytes=len(response.content),
                                    db_requests=metrics['db_requests']-calls,semantic_sha256=fingerprint))
    finally:
        server.should_exit=True
        thread.join(timeout=10)
    usage=resource.getrusage(resource.RUSAGE_SELF)
    report=dict(mode=mode,setup_seconds=setup_seconds,setup_db_requests=setup_db_requests,
                refresh_seconds=metrics.get('refresh_seconds'),requests=results,
                process_peak_rss_kib=usage.ru_maxrss,
                cgroup={key:(Path('/sys/fs/cgroup')/key).read_text().strip() for key in
                        ('cpu.max','cpuset.cpus.effective','memory.max','memory.peak','memory.swap.max','memory.events')},
                boundary='Local synthetic 200k-row archive fixture, real HTTP and shared ClickHouse/PostgreSQL/Flink dependencies; fixture receipts have no DB lineage. Candidate has explicit inspection staleness semantics. Setup/refresh excluded from request latency and reported separately.')
    write_json(output,report)
    print(canonical(report),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['memory','disk'],required=True)
    parser.add_argument('--fixture',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    run(args.mode,args.fixture,args.output)
