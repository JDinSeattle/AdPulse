"""Real loopback HTTP with isolated, deliberately failed/expired inspection files.

No business dependencies or volumes are used. Permit exhaustion is injected in
the API process, not presented as a database-saturation load experiment.
"""
from __future__ import annotations

import argparse
import os
import tempfile
import threading
import time
from pathlib import Path

import requests
import uvicorn

from adpulse import api
from adpulse.archive_index import ArchiveIndex
from adpulse.common import write_json
from adpulse.inspection import publish


def run(output):
    observations = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temp:
        root = Path(temp)
        os.environ['ADPULSE_INSPECTION_DIR'] = str(root)
        with_index = ArchiveIndex(root/'archive.sqlite')
        with_index.close()  # Reader must also work after the last writer closed.
        coverage = dict(archive_complete=True, lineage_complete=True)
        business = 'adpulse_failure_fixture_value 7\n'
        publish(root, 'coverage', coverage, started_at=time.time())
        publish(root, 'metrics', {'prometheus': business}, started_at=time.time())
        server = uvicorn.Server(uvicorn.Config(api.app, host='127.0.0.1', port=0, log_level='error'))
        worker = threading.Thread(target=server.run, daemon=True)
        worker.start()
        deadline = time.monotonic()+10
        while not server.started:
            if time.monotonic() > deadline:
                raise TimeoutError('Isolated HTTP server startup')
            time.sleep(.02)
        port = server.servers[0].sockets[0].getsockname()[1]
        url = f'http://127.0.0.1:{port}'

        def check(case, path, status):
            response = requests.get(url+path, timeout=5)
            assert response.status_code == status, (case, response.status_code, response.text)
            observations.append(dict(case=case, status=status, retry_after=response.headers.get('Retry-After')))
            return response

        try:
            check('fresh-coverage', '/v1/reconcile', 200)
            check('closed-writer-index-readable', '/v1/trace/absent', 404)
            assert business in check('fresh-metrics', '/metrics', 200).text
            for state in ('expired', 'error'):
                for kind, payload in (('coverage', coverage), ('metrics', {'prometheus': business})):
                    snapshot = publish(root, kind, payload, started_at=time.time(),
                                       error=RuntimeError('injected dependency failure') if state == 'error' else None)
                    if state == 'expired':
                        snapshot['generated_at'] -= 1000
                        write_json(root/(kind+'.json'), snapshot)
                for path in ('/v1/reconcile', '/v1/quality', '/v1/trace/absent'):
                    assert check(state+path, path, 503).headers['Retry-After'] == '30'
                text = check(state+'-metrics', '/metrics', 200).text
                assert business not in text
                assert 'adpulse_inspection_snapshot_ready{kind="coverage"} 0.0' in text
                assert 'adpulse_inspection_snapshot_ready{kind="metrics"} 0.0' in text
            held = 0
            try:
                while api.QUERY_SLOTS.acquire(blocking=False):
                    held += 1
                response = check('injected-permit-exhaustion', '/v1/metrics', 503)
                assert response.json()['detail']['code'] == 'QUERY_OVERLOADED'
                assert response.headers['Retry-After'] == '1'
            finally:
                for _ in range(held):
                    api.QUERY_SLOTS.release()
            publish(root, 'coverage', coverage, started_at=time.time())
            publish(root, 'metrics', {'prometheus': business}, started_at=time.time())
            check('recovered-coverage', '/v1/reconcile', 200)
            assert business in check('recovered-metrics', '/metrics', 200).text
        finally:
            server.should_exit = True
            worker.join(timeout=10)
    write_json(output, dict(passed=True, observations=observations,
                            environment='Actual Uvicorn loopback HTTP; synthetic inspection snapshots and in-process permit exhaustion; no business dependency failure injected',
                            boundary='Verifies HTTP failure/expiry/recovery contract and permit rejection, not real database saturation or production failure recovery.'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    run(args.output)
