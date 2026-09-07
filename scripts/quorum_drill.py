"""Real three-broker transaction/fail-closed acceptance on ONE Docker host."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
import uuid

import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from adpulse.common import write_json, digest
from adpulse.generator import generate
from integration import wait_until

COMPOSE = ['docker', 'compose', '-f', 'deployment/compose.quorum.yaml']
BOOTSTRAP = 'localhost:29092,localhost:29093,localhost:29094'
TOPICS = ['adpulse-quorum.raw', 'adpulse-quorum.receipts']


def compose(*args):
    result = subprocess.run([*COMPOSE, *args], capture_output=True, text=True, timeout=240)
    if result.returncode:
        raise RuntimeError(result.stderr[-3000:])


def topology(admin):
    metadata = admin.list_topics(timeout=15)
    return {topic: {str(p): dict(leader=m.leader, replicas=m.replicas, isrs=m.isrs)
                    for p, m in metadata.topics[topic].partitions.items()} for topic in TOPICS}


def read_committed(expected):
    consumer = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': 'quorum-proof-' + uuid.uuid4().hex,
                         'enable.auto.commit': False, 'isolation.level': 'read_committed',
                         'auto.offset.reset': 'earliest', 'enable.partition.eof': True})
    consumer.assign([TopicPartition(t, p, 0) for t in TOPICS for p in range(3)])
    rows, eof = [], set()
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                from confluent_kafka import KafkaError
                if message.error().code() == KafkaError._PARTITION_EOF:
                    eof.add((message.topic(), message.partition()))
                    if len(eof) == 6:
                        break
                    continue
                raise RuntimeError(message.error())
            rows.append({'topic': message.topic(), 'partition': message.partition(), 'offset': message.offset(),
                         'value': json.loads(message.value())})
    finally:
        consumer.close()
    raw = {r['value']['receipt_id']: r for r in rows if r['topic'] == TOPICS[0]}
    manifests = [r['value'] for r in rows if r['topic'] == TOPICS[1]]
    acknowledged = {rid for ack in expected for rid in ack['receipt_ids']}
    receipt_records = [record for m in manifests for record in m['records']]
    # Scope to this run; previous independently labelled experiments may remain in volumes.
    wanted_batches = {ack['batch_id'] for ack in expected}
    scoped = [r for r in receipt_records if r['receipt_id'] in acknowledged]
    assert {r['receipt_id'] for r in scoped} == acknowledged
    assert acknowledged <= raw.keys()
    for record in scoped:
        row = raw[record['receipt_id']]
        assert (row['topic'], row['partition'], row['offset']) == (record['topic'], record['partition'], record['offset'])
        assert digest(row['value']) == record['sha256']
    return dict(acknowledged_receipts=len(acknowledged), verified_raw=len(acknowledged),
                verified_manifests=sum(m['batch_id'] in wanted_batches for m in manifests),
                all_partition_eofs=len(eof) == 6), rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='artifacts/operations/quorum.json')
    args = parser.parse_args()
    run_id = uuid.uuid4().hex
    report = dict(passed=False, run_id=run_id, environment='single-host-three-broker-processes',
                  replication_factor=3, min_insync_replicas=2, producer_acks='all', isolation='read_committed',
                  combined_broker_controller=True, physical_host_fault_tested=False, steps=[])
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP, 'socket.timeout.ms': 10000})
    accepted = []

    def submit(label):
        start = time.monotonic()
        data = generate(20, int(run_id[:7], 16) + len(accepted), int(time.time() * 1000), 'normal')
        response = requests.post('http://localhost:28088/v1/events', json={
            'client_batch_id': run_id + '-' + label, 'events': data['transport']}, timeout=150)
        report['steps'].append(dict(label=label, http_status=response.status_code, elapsed_seconds=round(time.monotonic() - start, 3)))
        if response.status_code == 202:
            accepted.append(response.json())
        return response

    try:
        compose('up', '-d', '--wait', 'broker-1', 'broker-2', 'broker-3')
        existing = admin.list_topics(timeout=30).topics
        missing = [NewTopic(t, 3, 3, config={'min.insync.replicas': '2', 'unclean.leader.election.enable': 'false'})
                   for t in TOPICS if t not in existing]
        for future in (admin.create_topics(missing) if missing else {}).values():
            future.result(30)
        compose('up', '-d', '--wait', 'collector')
        wait_until(lambda: all(len(p['isrs']) == 3 for parts in topology(admin).values() for p in parts.values()), label='all ISR=3')
        report['before_topology'] = topology(admin)
        assert submit('baseline').status_code == 202
        leader = report['before_topology'][TOPICS[0]]['0']['leader']
        failed_service = f'broker-{leader}'
        started = time.monotonic()
        compose('kill', '-s', 'SIGKILL', failed_service)
        wait_until(lambda: (t if (t := topology(admin))[TOPICS[0]]['0']['leader'] not in {leader, -1}
                           and all(len(p['isrs']) >= 2 for parts in t.values() for p in parts.values()) else False),
                   timeout=90, label='new leader with ISR>=2')
        assert submit('one-broker-down').status_code == 202
        report['leader_failure_to_ack_seconds'] = round(time.monotonic() - started, 3)
        report['degraded_topology'] = topology(admin)
        second = next(i for i in (1, 2, 3) if i != leader)
        compose('kill', '-s', 'SIGKILL', f'broker-{second}')
        # Combined nodes lose controller majority as well as the data write quorum.
        response = submit('no-quorum')
        assert response.status_code == 503, response.text
        report['no_quorum_failed_closed'] = True
        compose('start', failed_service, f'broker-{second}')
        wait_until(lambda: all(len(p['isrs']) == 3 for parts in topology(admin).values() for p in parts.values()),
                   timeout=120, label='all replicas caught up')
        # Reinitialize same transactional ID to abort any in-doubt transaction.
        compose('restart', 'collector')
        wait_until(lambda: requests.get('http://localhost:28088/health', timeout=5).ok, timeout=90, label='collector reinitialized')
        assert submit('quorum-restored').status_code == 202
        report['reconciliation'], rows = read_committed(accepted)
        report['unacknowledged_failed_batch_visible'] = any(r['value'].get('client_batch_id') == run_id + '-no-quorum' for r in rows)
        assert not report['unacknowledged_failed_batch_visible']
        assert report['reconciliation']['all_partition_eofs']
        report['after_topology'] = topology(admin)
        report['passed'] = True
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            compose('start', 'broker-1', 'broker-2', 'broker-3', 'collector')
            report['cleanup'] = 'all lab services restarted; volumes retained'
        finally:
            write_json(args.output, report)
            print(json.dumps(report, indent=2), flush=True)


if __name__ == '__main__':
    main()
