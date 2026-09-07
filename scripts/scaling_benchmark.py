"""Run paired archive/oracle variants under identical Docker cgroup limits.

Only creates containers bearing this experiment's prefix. Does not stop any
business stack or prune/delete volumes. Run after other AdPulse experiments end.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
from pathlib import Path

from adpulse.common import canonical, write_json


def run(args):
    root = Path.cwd().resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    image = subprocess.check_output(['docker', 'image', 'inspect', args.image, '--format', '{{.Id}}'], text=True).strip()
    fixture = json.loads((args.fixture/'fixture.json').read_text())
    # Check every frozen input before starting timed workers.
    for name, sha in fixture['files'].items():
        if hashlib.sha256((args.fixture/name).read_bytes()).hexdigest() != sha:
            raise ValueError('Fixture changed: '+name)
    script = root/'scripts'/('serving_worker.py' if args.workload == 'serving' else 'scaling_worker.py')
    manifest = dict(environment='local Docker on shared physical host', image=image, baseline_commit=args.baseline_commit,
                    fixture_manifest_sha256=fixture['file_manifest_sha256'], fixture_records=fixture['records'],
                    cpus=2, cpuset=args.cpuset, memory_bytes=4*1024**3, swap_bytes=0, workload=args.workload,
                    order=[], results=[], cpu_model=platform.processor(),
                    candidate_sources={str(p.relative_to(args.candidate)):hashlib.sha256(p.read_bytes()).hexdigest()
                                       for p in (args.candidate/'adpulse').glob('*.py')},
                    limitation='CPU affinity and cgroup quotas are fixed; host disk/page cache and other user projects are shared. No production or Flink-throughput inference.')
    for repeat in range(args.repeats):
        for mode in (('memory','disk') if repeat % 2 == 0 else ('disk','memory')):
            tag = f'{mode}-{repeat+1}'
            source = args.baseline if mode == 'memory' else args.candidate
            name = 'adpulse-scaling-'+args.output.name+'-'+tag
            cmd = ['docker','run','--name',name,'--network','adpulse_default' if args.workload=='serving' else 'none','--user',f'{os.getuid()}:{os.getgid()}',
                   '--cpus','2','--cpuset-cpus',args.cpuset,'--memory','4g','--memory-swap','4g',
                   '--mount',f'type=bind,src={source.resolve()},dst=/work,readonly',
                   '--mount',f'type=bind,src={args.fixture.resolve()},dst=/fixture,readonly',
                   '--mount',f'type=bind,src={script},dst=/scaling_worker.py,readonly',
                   '--mount',f'type=bind,src={args.output.resolve()},dst=/output',
                   '-w','/work','-e','PYTHONPATH=/work','-e','ADPULSE_ROOT=/work',
                   '-e','CLICKHOUSE_URL=http://clickhouse:8123',
                   '-e','POSTGRES_DSN=postgresql://adpulse:adpulse-local@postgres:5432/adpulse',image,
                   'python','/scaling_worker.py','--mode',mode,'--fixture','/fixture','--output','/output/'+tag+'.json']
            with (args.output/(tag+'.log')).open('w') as log:
                process = subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
            inspect = json.loads(subprocess.check_output(['docker','inspect',name],text=True))[0]
            write_json(args.output/(tag+'-container.json'), {k:inspect[k] for k in ('Id','Image','State','HostConfig')})
            record = dict(mode=mode,repeat=repeat+1,exit_code=process.returncode,oom_killed=inspect['State']['OOMKilled'])
            if process.returncode == 0:
                record['measurement']=json.loads((args.output/(tag+'.json')).read_text())
            manifest['order'].append(tag)
            manifest['results'].append(record)
            write_json(args.output/'report.json',manifest)
            print(canonical(record),flush=True)
            if process.returncode:
                raise RuntimeError('Benchmark case failed; preserved report/log/container: '+tag)
    if args.workload == 'serving':
        fingerprints = [[r['semantic_sha256'] for r in case['measurement']['requests'] if r['route']=='trace'] for case in manifest['results']]
        assert all(f == fingerprints[0] for f in fingerprints), 'Trace response semantics differ'
        summary = {}
        for mode in ('memory', 'disk'):
            cases = [r['measurement'] for r in manifest['results'] if r['mode']==mode]
            summary[mode] = {'setup_seconds_median':statistics.median(r['setup_seconds'] for r in cases),
                             'setup_db_requests':[r['setup_db_requests'] for r in cases]}
            for route in ('trace', 'metrics'):
                requests = [r for case in cases for r in case['requests'] if r['route']==route]
                summary[mode][route] = dict(requests=len(requests), median_seconds=statistics.median(r['seconds'] for r in requests),
                                           max_seconds=max(r['seconds'] for r in requests), total_db_requests=sum(r['db_requests'] for r in requests))
        manifest.update(passed=True, trace_semantics_identical=True, summary=summary,
                        tradeoff='Candidate serves indexed/datetime-stamped cached results. Setup and periodic refresh have nonzero cost; this is not a like-for-like live-query speedup or measured throughput improvement.')
        write_json(args.output/'report.json',manifest)
        return manifest
    fingerprints = [r['measurement']['output_fingerprints'] for r in manifest['results']]
    if not all(f == fingerprints[0] for f in fingerprints):
        raise AssertionError('Baseline and candidate outputs differ')
    summary = {}
    for mode in ('memory','disk'):
        rows = [r['measurement'] for r in manifest['results'] if r['mode']==mode]
        summary[mode] = {key:statistics.median(r[key] for r in rows) for key in
                         ('elapsed_seconds','process_peak_rss_kib','user_cpu_seconds','system_cpu_seconds','scratch_disk_bytes')}
    manifest.update(passed=True,all_outputs_identical=True,summary=summary,
                    rss_reduction_fraction=1-summary['disk']['process_peak_rss_kib']/summary['memory']['process_peak_rss_kib'],
                    elapsed_ratio_disk_over_memory=summary['disk']['elapsed_seconds']/summary['memory']['elapsed_seconds'])
    write_json(args.output/'report.json',manifest)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture',type=Path,required=True)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--candidate',type=Path,required=True)
    parser.add_argument('--baseline-commit',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--image',default='adpulse-python:0.3.0')
    parser.add_argument('--cpuset',default='2,4')
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--workload',choices=['oracle','serving'],default='oracle')
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error('At least two paired repetitions required')
    run(args)
