"""Paced full-session load with periodic progress, checkpoints and resource evidence."""
from __future__ import annotations

import argparse
import json
import math
import platform
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from adpulse.common import now_ms, write_json, canonical
from adpulse.generator import generate, send
from adpulse.storage import ClickHouse


def freshness(db, prefix):
    return db.query("""SELECT count() AS visible,quantileExact(0.95)(lag) AS p95_seconds,max(lag) AS max_seconds FROM
      (SELECT receipt_id,(toUnixTimestamp64Milli(min(visible_at))-min(received_at))/1000.0 AS lag FROM adpulse.visibility
       WHERE release_id='live-v1' AND receipt_id IN
         (SELECT arrayJoin(receipt_ids) FROM adpulse.receipts FINAL
          WHERE startsWith(JSONExtractString(payload,'client_batch_id'),{prefix:String})) GROUP BY receipt_id)
      FORMAT JSONEachRow""", {"prefix": prefix})[0]


def runtime_sample():
    base = "http://localhost:18081"
    response = requests.get(base + "/jobs/overview", timeout=10)
    response.raise_for_status()
    jobs = {}
    for job in response.json()["jobs"]:
        if job["state"] in {"CANCELED", "FINISHED", "FAILED"}:
            continue
        checkpoints = requests.get(f"{base}/jobs/{job['jid']}/checkpoints", timeout=10).json()
        complete = checkpoints.get("latest", {}).get("completed", {})
        jobs[job["jid"]] = dict(state=job["state"], counts=checkpoints.get("counts", {}),
                                 checkpoint_bytes=complete.get("state_size"),
                                 checkpoint_duration_ms=complete.get("end_to_end_duration"),
                                 checkpoint_timestamp_ms=complete.get("latest_ack_timestamp"))
    names = subprocess.run(["docker", "ps", "--filter", "label=com.docker.compose.project=adpulse", "--format", "{{.Names}}"],
                           capture_output=True, text=True, check=True, timeout=10).stdout.splitlines()
    raw = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
                         capture_output=True, text=True, check=True, timeout=15).stdout
    resource = [json.loads(line) for line in raw.splitlines()]
    state = json.loads(subprocess.run(["docker", "inspect", *names], capture_output=True, text=True,
                                      check=True, timeout=10).stdout)
    return dict(jobs=jobs, containers=resource,
                container_state={s["Name"].lstrip("/"): {"oom_killed": s["State"]["OOMKilled"],
                                 "restart_count": s["RestartCount"], "status": s["State"]["Status"],
                                 "container_id": s["Id"], "started_at": s["State"]["StartedAt"]} for s in state})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--sample-seconds", type=int, default=60)
    parser.add_argument("--freshness-slo-seconds", type=float, default=60)
    parser.add_argument("--catchup-seconds", type=int, default=300)
    parser.add_argument("--output", default="artifacts/loadtest/report.json")
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.sample_seconds, args.catchup_seconds) < 1:
        parser.error("rate and durations must be positive")
    output = Path(args.output)
    if output.exists() or output.with_suffix(".samples.jsonl").exists():
        parser.error("report or samples already exist; preserve historical measurements")
    output.parent.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc).isoformat()
    seed, accepted, sequence = now_ms() % 1_000_000_000, 0, 0
    prefix = f"load-{seed}-"
    samples, errors = [], []
    stop = threading.Event()
    before = runtime_sample()
    start = time.monotonic()

    def monitor():
        previous_time, previous_count = start, 0
        db = ClickHouse()
        with output.with_suffix(".samples.jsonl").open("x") as handle:
            while not stop.wait(args.sample_seconds):
                current, count = time.monotonic(), accepted
                sample = dict(elapsed_seconds=round(current - start, 3), accepted=count,
                              interval_events_per_second=round((count - previous_count) / (current - previous_time), 3))
                previous_time, previous_count = current, count
                try:
                    sample.update(freshness=freshness(db, prefix), **runtime_sample())
                    sample["pending_visibility"] = max(0, count - sample["freshness"]["visible"])
                except Exception as exc:
                    sample["error"] = f"{type(exc).__name__}: {exc}"
                    errors.append(sample["error"])
                samples.append(sample)
                handle.write(canonical(sample) + "\n")
                handle.flush()
                print(canonical({k: sample[k] for k in ("elapsed_seconds", "accepted", "interval_events_per_second")}), flush=True)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    failure = None

    def interrupted(signum, frame):
        raise RuntimeError(f"Interrupted by signal {signum}; accepted count covers completed client sends only")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        while time.monotonic() - start < args.seconds:
            users = max(1, min(250, math.ceil(args.rate / 2)))
            data = generate(users, seed + sequence, now_ms() - users * 1000, "normal")
            acks = send(data["transport"], "http://localhost:8088", batch_prefix=f"{prefix}{sequence}")
            accepted += sum(ack["accepted"] for ack in acks)
            sequence += 1
            target = start + accepted / args.rate
            if target > time.monotonic():
                time.sleep(min(target - time.monotonic(), max(0, args.seconds - (time.monotonic() - start))))
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    finally:
        elapsed = time.monotonic() - start
        stop.set()
        thread.join(timeout=90)
    db, observed = ClickHouse(), {}
    catchup_start = time.monotonic()
    while time.monotonic() - catchup_start < args.catchup_seconds:
        observed = freshness(db, prefix)
        if observed["visible"] == accepted:
            break
        time.sleep(2)
    after = runtime_sample()
    checkpoint_deltas = {jid: {field: job["counts"].get(field, 0) - before["jobs"].get(jid, {}).get("counts", {}).get(field, 0)
                               for field in ("completed", "failed", "restored")}
                         for jid, job in after["jobs"].items()}
    runtime_healthy = (set(before["jobs"]) == set(after["jobs"]) and len(after["jobs"]) == 2
                       and all(j["state"] == "RUNNING" for s in [before, *samples, after] for j in s.get("jobs", {}).values())
                       and all(v["completed"] > 0 and v["failed"] == 0 and v["restored"] == 0 for v in checkpoint_deltas.values())
                       and all(not s["oom_killed"] and s["status"] == "running"
                               and name in before["container_state"]
                               and all(s[field] == before["container_state"][name][field]
                                       for field in ("restart_count", "container_id", "started_at"))
                               for name, s in after["container_state"].items()))
    report = dict(started_at=started_at, target_rate=args.rate, requested_duration_seconds=args.seconds,
                  duration_seconds=round(elapsed, 3), accepted=accepted, client_batch_prefix=prefix,
                  observed_events_per_second=round(accepted / elapsed, 2), freshness=observed,
                  complete_visibility=observed.get("visible") == accepted,
                  catchup_seconds=round(time.monotonic() - catchup_start, 3),
                  sustained_target_duration_met=elapsed >= 1800 and not failure,
                  target_rate_met=accepted / elapsed >= args.rate * 0.95,
                  freshness_slo_seconds=args.freshness_slo_seconds,
                  freshness_slo_met=observed.get("visible") == accepted and observed.get("p95_seconds") is not None
                                    and observed["p95_seconds"] <= args.freshness_slo_seconds,
                  runtime_healthy=runtime_healthy, checkpoint_deltas=checkpoint_deltas,
                  sample_count=len(samples), sampling_errors=errors, failure=failure,
                  environment=dict(os=platform.platform(), python=platform.python_version(),
                                   mode="single-host-single-broker", shared_developer_host=True),
                  versions=dict(flink=requests.get("http://localhost:18081/overview", timeout=10).json()["flink-version"],
                                clickhouse=db.query("SELECT version() AS v FORMAT JSONEachRow")[0]["v"]),
                  before_runtime=before, after_runtime=after)
    report["passed"] = all(report[k] for k in ("complete_visibility", "target_rate_met", "freshness_slo_met", "runtime_healthy")) and not failure and not errors
    write_json(output, report)
    print(canonical({k: v for k, v in report.items() if k not in {"before_runtime", "after_runtime"}}), flush=True)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
