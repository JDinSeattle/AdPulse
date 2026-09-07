"""Restore a stopped/failed job from its retained checkpoint with strict state mapping."""
import argparse
import time

import requests

from adpulse.common import canonical, write_json
from integration import wait_until


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--entry-class", choices=["io.adpulse.CleanJob", "io.adpulse.AttributionJob"], required=True)
    parser.add_argument("--state-path")
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--output", default="artifacts/recovery/report.json")
    args = parser.parse_args()
    base = "http://localhost:18081"
    started = time.monotonic()
    state = args.state_path
    if not state:
        response = requests.get(f"{base}/jobs/{args.job_id}/checkpoints", timeout=10)
        response.raise_for_status()
        state = response.json().get("latest", {}).get("completed", {}).get("external_path")
    if not state:
        raise ValueError("No retained state path; refusing empty-state recovery")
    old_response = requests.get(f"{base}/jobs/{args.job_id}", timeout=10)
    existing = old_response.json() if old_response.ok else {"state": "MISSING"}
    if existing.get("state") not in {"FAILED", "CANCELED", "FINISHED", "MISSING"}:
        requests.patch(f"{base}/jobs/{args.job_id}", params={"mode": "cancel"}, timeout=10).raise_for_status()
        wait_until(lambda: requests.get(f"{base}/jobs/{args.job_id}", timeout=10).json()["state"] in {"CANCELED", "FAILED"}, label="old job stopped")
    with open("flink-jobs/target/adpulse-jobs.jar", "rb") as jar:
        uploaded = requests.post(base + "/jars/upload", files={"jarfile": ("adpulse-jobs.jar", jar, "application/java-archive")}, timeout=60)
    uploaded.raise_for_status()
    jar_id = uploaded.json()["filename"].split("/")[-1]
    response = requests.post(f"{base}/jars/{jar_id}/run", json={"entryClass": args.entry_class, "parallelism": args.parallelism,
                              "savepointPath": state, "allowNonRestoredState": False}, timeout=120)
    if not response.ok:
        raise RuntimeError(response.text)
    restored = response.json()["jobid"]

    def checkpoint_after_recovery():
        status = requests.get(f"{base}/jobs/{restored}", timeout=10).json()
        checkpoints = requests.get(f"{base}/jobs/{restored}/checkpoints", timeout=10).json()
        return checkpoints if status["state"] == "RUNNING" and checkpoints.get("counts", {}).get("completed", 0) > 0 else False

    proof = wait_until(checkpoint_after_recovery, label="restored job completes a new checkpoint")
    report = dict(passed=True, previous_job=args.job_id, restored_job=restored, restored_from=state,
                  restored_checkpoint=proof["latest"].get("restored"), counts=proof["counts"],
                  recovery_seconds=round(time.monotonic() - started, 3), allow_non_restored_state=False)
    write_json(args.output, report)
    print(canonical(report))


if __name__ == "__main__":
    main()
