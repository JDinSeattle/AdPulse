"""Verify the bounded anti-join against real ClickHouse, including missing rows."""
import argparse
import json
import time
import uuid
from pathlib import Path

import requests

from adpulse.common import write_json
from adpulse.inspection import OUTSTANDING_SQL
from adpulse.storage import ClickHouse


def run(output):
    db = ClickHouse()
    cases = []
    for suffix, sql in (
        ('full-history', OUTSTANDING_SQL),
        ('missing-and-duplicate-lineage', """SELECT min(received_at) AS oldest,count() AS n FROM
          (SELECT arrayJoin(['a','b','c','d']) AS receipt_id,if(receipt_id='b',100,200) AS received_at) a
          LEFT ANTI JOIN (SELECT arrayJoin(['a','a','c','foreign']) AS receipt_id) q ON a.receipt_id=q.receipt_id
          SETTINGS join_algorithm='grace_hash',grace_hash_join_initial_buckets=16,max_bytes_in_join=67108864,
                   max_temporary_data_on_disk_size_for_query=4294967296 FORMAT JSONEachRow""")):
        qid='adpulse-scaling-'+suffix+'-'+uuid.uuid4().hex
        started=time.monotonic()
        response=requests.post(db.url,auth=db.auth,data=sql.encode(),timeout=125,
                               params={'query_id':qid,'max_memory_usage':536870912,'max_execution_time':120,'max_threads':2,
                                       'output_format_json_quote_64bit_integers':0})
        response.raise_for_status()
        value=response.json()
        if suffix=='missing-and-duplicate-lineage':
            assert value=={'oldest':100,'n':2}
        cases.append(dict(case=suffix,query_id=qid,result=value,elapsed_seconds=time.monotonic()-started,
                          memory_budget_bytes=536870912,summary=json.loads(response.headers.get('X-ClickHouse-Summary','{}'))))
    report=dict(passed=True,cases=cases,environment='Real local ClickHouse 26.3 LTS; full retained history and inline synthetic edge case',
                scope='Exact missing-receipt anti-join within a configured query budget, not a hard server-wide memory limit')
    write_json(output,report)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    run(args.output)
