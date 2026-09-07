import json
import os
import time
from pathlib import Path

import requests
from confluent_kafka.admin import AdminClient, NewTopic

from adpulse.archive import S3Objects
from adpulse.common import load_rules, canonical, digest
from adpulse.releases import connect, register
from adpulse.storage import ClickHouse


def main():
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    topics = ["adpulse.raw", "adpulse.clean", "adpulse.quality", "adpulse.quarantine", "adpulse.receipts",
              "adpulse.results.live-v1", "adpulse.cdc.public.campaign_versions", "__debezium-heartbeat.adpulse.cdc"]
    existing = admin.list_topics(timeout=30).topics
    missing = [NewTopic(t, num_partitions=3, replication_factor=1,
                       config={"cleanup.policy": "delete", "retention.ms": "604800000"}) for t in topics if t not in existing]
    futures = admin.create_topics(missing) if missing else {}
    for future in futures.values():
        future.result(30)
    for statement in Path("/app/deployment/clickhouse/init.sql").read_text().split(";"):
        if statement.strip():
            ClickHouse().query(statement)
    objects = S3Objects()
    if objects.bucket not in [b["Name"] for b in objects.client.list_buckets()["Buckets"]]:
        objects.client.create_bucket(Bucket=objects.bucket)
    rules = load_rules()
    objects.put(f"rules/{digest(rules)}.json", canonical(rules).encode())
    with connect() as db:
        present = db.execute("SELECT rules_sha256,status FROM releases WHERE release_id='live-v1'").fetchone()
    if not present:
        register("live-v1", rules, kind="live")
    elif present["rules_sha256"] != digest(rules):
        raise ValueError("Rules changed under existing release; create a new release")
    config = json.loads(Path("/app/deployment/debezium/connector.json").read_text())
    response = requests.put(f"http://connect:8083/connectors/{config['name']}/config", json=config["config"], timeout=30)
    response.raise_for_status()
    for _ in range(90):
        status = requests.get(f"http://connect:8083/connectors/{config['name']}/status", timeout=10).json()
        if status.get("tasks") and all(t["state"] == "RUNNING" for t in status["tasks"]):
            break
        time.sleep(2)
    else:
        raise RuntimeError(f"CDC connector failed: {status}")
    for _ in range(60):
        overview = requests.get("http://jobmanager:8081/overview", timeout=10).json()
        if overview.get("slots-total", 0) >= 4:
            break
        time.sleep(2)
    else:
        raise RuntimeError("Flink workers did not register")
    jobs = requests.get("http://jobmanager:8081/jobs/overview", timeout=10).json()["jobs"]
    with open("/app/jobs.jar", "rb") as jar:
        upload = requests.post("http://jobmanager:8081/jars/upload", files={"jarfile": ("adpulse-jobs.jar", jar, "application/java-archive")}, timeout=60)
    upload.raise_for_status()
    jar_id = upload.json()["filename"].split("/")[-1]
    for entry, title in [("io.adpulse.CleanJob", "cleaning and quality"), ("io.adpulse.AttributionJob", "attribution and metrics")]:
        if any(title in job["name"] and job["state"] in {"RUNNING", "RESTARTING", "CREATED"} for job in jobs):
            continue
        restore = os.getenv("CLEAN_SAVEPOINT" if entry.endswith("CleanJob") else "ATTRIBUTION_SAVEPOINT")
        if present and present["status"] != "building" and not restore:
            raise RuntimeError(f"Refusing empty-state restart of {entry} in an existing validated release. Supply its savepoint path.")
        submission = {"entryClass": entry, "parallelism": 2, "allowNonRestoredState": False}
        if restore:
            submission["savepointPath"] = restore
        result = requests.post(f"http://jobmanager:8081/jars/{jar_id}/run", json=submission, timeout=120)
        if not result.ok:
            raise RuntimeError(f"Flink submission failed: {result.text}")
        result.raise_for_status()
        print(entry, result.json(), flush=True)
    print("AdPulse initialized; live-v1 remains building until smoke reconciliation validates it.", flush=True)


if __name__ == "__main__":
    main()
