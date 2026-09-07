"""Repeatable local incident drills. Every result includes its actual execution mode."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

import requests

from adpulse import releases
from adpulse.archive import S3Objects, verified_records, reconcile
from adpulse.cli import main as cli
from adpulse.common import canonical, load_rules, now_ms, write_json
from adpulse.generator import generate, send
from adpulse.storage import ClickHouse
from integration import checkpoints, jobs, reconcile_live, run, wait_until

COMPOSE = ["docker", "compose", "-f", "deployment/compose.yaml"]
SCENARIOS = ["duplicates", "schema", "conversion-first", "sink-replay", "worker-restart", "archive-gap", "rule-rollback", "cdc-recovery", "savepoint-rescale", "late-boundary"]


def compose(*args, check=True, timeout=180):
    return subprocess.run([*COMPOSE, *args], text=True, capture_output=True, check=check, timeout=timeout)


def worker_with_tasks():
    """Choose a real failure target; fixed worker names can be idle on small CI jobs."""
    assigned = {}
    for job in jobs():
        detail = requests.get(f"http://localhost:18081/jobs/{job['jid']}", timeout=10)
        detail.raise_for_status()
        for vertex in detail.json()["vertices"]:
            response = requests.get(f"http://localhost:18081/jobs/{job['jid']}/vertices/{vertex['id']}", timeout=10)
            response.raise_for_status()
            for subtask in response.json()["subtasks"]:
                if subtask["status"] == "RUNNING":
                    worker = subtask["taskmanager-id"]
                    assigned[worker] = assigned.get(worker, 0) + 1
    candidates = []
    for service in ("taskmanager-1", "taskmanager-2"):
        container = compose("ps", "-q", service).stdout.strip()
        if not container:
            continue
        metadata = json.loads(subprocess.run(["docker", "inspect", container], check=True, capture_output=True,
                                              text=True, timeout=10).stdout)[0]
        addresses = [n["IPAddress"] for n in metadata["NetworkSettings"]["Networks"].values() if n["IPAddress"]]
        for worker, count in assigned.items():
            if any(worker.startswith(address + ":") for address in addresses):
                candidates.append(dict(service=service, taskmanager_id=worker, running_subtasks=count))
    if not candidates:
        raise ValueError("No Compose worker with observed RUNNING subtasks; refusing an ineffective fault")
    return max(candidates, key=lambda worker: worker["running_subtasks"])


def submit():
    seed = now_ms() % 1_000_000_000
    dataset = generate(users=35, seed=seed, start_ms=now_ms() - 35000, scenario="normal")
    acknowledgements = send(dataset["transport"], "http://localhost:8088", batch_prefix=f"drill-{seed}")
    return [receipt for ack in acknowledgements for receipt in ack["receipt_ids"]]


def drill(scenario):
    root = Path("artifacts/drills") / scenario
    started = time.monotonic()
    report = {"scenario": scenario, "mode": "docker-real-components", "passed": False}
    try:
        if scenario in {"duplicates", "schema", "conversion-first"}:
            report.update(run(scenario, users=80, output=str(root)))
        elif scenario == "sink-replay":
            compose("stop", "sink")
            try:
                receipts = submit()
                # Fault process dies exactly after CH writes, before consumer.commit.
                failed = compose("run", "--rm", "--no-deps", "-e", "FAULT_CRASH_AFTER_WRITE=1", "sink", check=False)
                if failed.returncode != 77:
                    raise AssertionError(f"Expected injected exit 77, got {failed.returncode}: {failed.stderr}")
                report["injected_exit_code"] = failed.returncode
                evidence = [json.loads(line) for line in failed.stdout.splitlines() if line.startswith('{"fault"')]
                if not evidence or evidence[0]["result_records"] < 1:
                    raise AssertionError("Fault process did not prove a result write before exiting")
                report["write_before_crash"] = evidence[0]
            finally:
                compose("start", "sink")
            report.update(reconcile_live(receipts))
        elif scenario == "worker-restart":
            before = checkpoints()
            if not before or not all(c.get("counts", {}).get("completed", 0) for c in before.values()):
                raise ValueError("A completed checkpoint is required before the worker drill")
            target = worker_with_tasks()
            report["fault_target"] = target
            compose("kill", "-s", "SIGKILL", target["service"])
            try:
                receipts = submit()
            finally:
                compose("start", target["service"])
            def restored_and_running():
                current = checkpoints()
                return len(jobs()) == 2 and all(j["state"] == "RUNNING" for j in jobs()) and any(
                    current[k]["counts"]["restored"] > before.get(k, {}).get("counts", {}).get("restored", 0) for k in current)
            wait_until(restored_and_running, label="worker recovery from checkpoint")
            report["running_after_seconds"] = round(time.monotonic() - started, 3)
            report.update(reconcile_live(receipts))
            after = checkpoints()
            if not any(after[k]["counts"]["restored"] > before.get(k, {}).get("counts", {}).get("restored", 0) for k in after):
                raise AssertionError("No restored checkpoint was reported after worker interruption")
            report["checkpoint_counts"] = {k: c["counts"] for k, c in after.items()}
        elif scenario == "archive-gap":
            compose("stop", "archive")
            try:
                receipts = submit()
                coverage = reconcile(verified_records(S3Objects()), expected_receipts=receipts)
                if not set(receipts) <= set(coverage["archive_missing"]):
                    raise AssertionError("Expected archive gap was not observed")
                report["observed_missing_receipts"] = len(receipts)
            finally:
                compose("start", "archive")
            report.update(reconcile_live(receipts))
        elif scenario == "rule-rollback":
            prior = releases.active()
            if not prior:
                raise ValueError("Run smoke first to validate the initial release")
            rules = load_rules()
            rules["rule_version"] = f"bad-filter-{now_ms()}"
            rules["rollback_version"] = "rules-v1"
            rules["filter_regions"] = ["US", "JP", "GB"]
            rules_path = root / "bad-rules.json"
            write_json(rules_path, rules)
            candidate = f"replay-bad-{now_ms()}"
            cli(["replay", "--from-s3", "--release", candidate, "--rules", str(rules_path), "--output", str(root / "candidate"), "--publish"])
            # An intentional bad business rule can still satisfy delivery checks.
            before = ClickHouse().snapshots(prior)
            after = ClickHouse().snapshots(candidate)
            before_impressions = sum(m["values"]["impressions"] for m in before["metrics"] if m["cohort_basis"] == "occurrence")
            after_impressions = sum(m["values"]["impressions"] for m in after["metrics"] if m["cohort_basis"] == "occurrence")
            if before_impressions <= 0 or after_impressions != 0:
                raise AssertionError("Rule impact comparison did not detect exposure removal")
            try:
                report["activation"] = releases.activate(candidate, "controlled bad-rule drill after impact comparison")
                assert releases.active() == candidate
            finally:
                report["rollback"] = releases.activate(prior, "restore validated rule after controlled drill")
            assert ClickHouse().snapshots(prior) == before
            report.update(passed=True, detected_impression_delta=after_impressions - before_impressions)
        elif scenario == "cdc-recovery":
            endpoint = "http://localhost:18083/connectors/adpulse-campaigns"
            requests.put(endpoint + "/pause", timeout=10).raise_for_status()
            wait_until(lambda: requests.get(endpoint + "/status", timeout=10).json()["connector"]["state"] == "PAUSED", label="CDC pause")
            effective = now_ms() + 7 * 86400000
            version = now_ms()
            try:
                with releases.connect() as db:
                    db.execute("INSERT INTO campaign_versions VALUES('campaign-0',%s,NULL,%s,%s)", (effective, version, '{"channel":"cdc-drill"}'))
                    db.execute("UPDATE campaign_versions SET attributes=%s WHERE campaign_id='campaign-0' AND source_version=%s", ('{"channel":"cdc-updated"}', version))
                with releases.connect() as db:
                    report["slot"] = db.execute("SELECT slot_name,active,pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint AS retained_bytes FROM pg_replication_slots WHERE slot_name='adpulse_campaigns'").fetchone()
            finally:
                requests.put(endpoint + "/resume", timeout=10).raise_for_status()

            def changes():
                events = [r["value"] for r in verified_records(S3Objects()) if r["topic"].endswith("campaign_versions") and r["value"]]
                return [e for e in events if (e.get("after") or e.get("before") or {}).get("source_version") == version]

            wait_until(lambda: {"c", "u"} <= {e["op"] for e in changes()}, label="CDC resume WAL create/update")
            with releases.connect() as db:
                db.execute("DELETE FROM campaign_versions WHERE campaign_id='campaign-0' AND source_version=%s", (version,))
            wait_until(lambda: "d" in {e["op"] for e in changes()}, label="CDC delete")
            report.update(passed=True, observed_operations=sorted({e["op"] for e in changes()}), explicit_effective_from=effective)
        elif scenario == "savepoint-rescale":
            wait_until(lambda: len(jobs()) == 2 and all(j["state"] == "RUNNING" for j in jobs()), label="healthy jobs before savepoint")
            job = next(j for j in jobs() if "attribution and metrics" in j["name"])
            base = "http://localhost:18081"
            trigger = requests.post(f"{base}/jobs/{job['jid']}/savepoints", json={"target-directory": "file:///opt/flink/state/savepoints", "cancel-job": False}, timeout=30)
            trigger.raise_for_status()
            request_id = trigger.json()["request-id"]

            def saved():
                response = requests.get(f"{base}/jobs/{job['jid']}/savepoints/{request_id}", timeout=10).json()
                if response["status"]["id"] != "COMPLETED":
                    return False
                if "failure-cause" in response.get("operation", {}):
                    raise RuntimeError(response["operation"]["failure-cause"].get("stack-trace", "savepoint failed"))
                return response["operation"]["location"]

            location = wait_until(saved, label="durable savepoint")
            with open("flink-jobs/target/adpulse-jobs.jar", "rb") as jar:
                uploaded = requests.post(base + "/jars/upload", files={"jarfile": ("adpulse-jobs.jar", jar, "application/java-archive")}, timeout=60)
            uploaded.raise_for_status()
            jar_id = uploaded.json()["filename"].split("/")[-1]
            requests.patch(f"{base}/jobs/{job['jid']}", params={"mode": "cancel"}, timeout=10).raise_for_status()
            wait_until(lambda: all(j["jid"] != job["jid"] for j in jobs()), label="old job cancellation")
            response = requests.post(f"{base}/jars/{jar_id}/run", json={"entryClass": "io.adpulse.AttributionJob", "parallelism": 3,
                                      "savepointPath": location, "allowNonRestoredState": False}, timeout=120)
            response.raise_for_status()
            restored = response.json()["jobid"]
            wait_until(lambda: any(j["jid"] == restored and j["state"] == "RUNNING" for j in jobs()), label="savepoint restored job")
            details = requests.get(f"{base}/jobs/{restored}", timeout=10).json()
            if not any(v["parallelism"] == 3 for v in details["vertices"]):
                raise AssertionError("Requested parallelism was overridden by application code")
            report.update(reconcile_live(submit()))
            report.update(savepoint=location, restored_job=restored, requested_parallelism=3,
                          restored_checkpoint=checkpoints()[restored].get("latest", {}).get("restored"))
            if not report["restored_checkpoint"]:
                raise AssertionError("Job did not report restored savepoint")
        elif scenario == "late-boundary":
            result = subprocess.run(["mvn", "-q", "-f", "flink-jobs/pom.xml", f"-Dmaven.repo.local={Path('.cache/m2').resolve()}",
                                     "-Dtest=OperatorsTest#timeoutProducesReasonAndLateClickRequiresAuditedReplay+actual24HourEdgesAndIdentityMismatch+frozenMetricRoutesToCorrectionInsteadOfResettingAggregate", "test"], capture_output=True, text=True, timeout=180)
            if result.returncode:
                raise AssertionError(result.stdout + result.stderr)
            report.update(passed=True, mode="real-flink-operator-harness-event-time", actual_business_window_ms=86400000,
                          actual_wait_ms=7200000, processing_time_accelerated=False)
        else:
            raise ValueError(scenario)
    except Exception as exc:
        report.update(passed=False, error=str(exc))
        raise
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        write_json(root / "report.json", report)
        print(canonical(report), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=[*SCENARIOS, "all"])
    args = parser.parse_args()
    for scenario in SCENARIOS if args.scenario == "all" else [args.scenario]:
        drill(scenario)
