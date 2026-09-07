"""Pause local ingress and qualify the explicit capacity profile with retained state.

Requires existing RUNNING jobs and a built target JAR. On failure, ingress remains
paused and savepoints/journal remain available; never fall back to empty state.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from adpulse.common import write_json
from integration import jobs
from migrate_runtime import save_and_cancel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/capacity-restore"))
    args = parser.parse_args()
    current = jobs()
    if len(current) != 2 or any(j["state"] != "RUNNING" for j in current):
        raise ValueError("Expected two RUNNING source jobs")
    args.output.mkdir(parents=True, exist_ok=False)
    compose = ["docker", "compose", "--env-file", "deployment/capacity.env", "-f", "deployment/compose.yaml"]
    config = json.loads(subprocess.check_output([*compose, "config", "--format", "json"], text=True))
    for service in ("jobmanager", "taskmanager-1", "taskmanager-2"):
        properties = dict(line.strip().split(": ", 1) for line in config["services"][service]["environment"]["FLINK_PROPERTIES"].splitlines() if line.strip())
        if properties["taskmanager.memory.process.size"] != "8192m":
            raise ValueError("Capacity profile drift: expected 8192m process budgets")
    bootstrap = config["services"]["bootstrap"]["environment"]
    if (int(bootstrap["FLINK_CLEAN_PARALLELISM"]), int(bootstrap["FLINK_ATTRIBUTION_PARALLELISM"])) != (2, 6):
        raise ValueError("Capacity profile drift: expected parallelism 2/6")
    report = dict(passed=False, environment="local Docker, explicit capacity profile", started_at_epoch=time.time(),
                  jar_sha256=hashlib.sha256(Path("flink-jobs/target/adpulse-jobs.jar").read_bytes()).hexdigest(),
                  savepoints=[], restores=[])

    def journal(phase):
        report["phase"] = phase
        write_json(args.output / "report.json", report)
        print(phase, flush=True)

    try:
        subprocess.run([*compose, "stop", "collector"], check=True)
        journal("ingress-paused")
        for job in sorted(current, key=lambda j: "attribution" in j["name"].lower()):
            report["savepoints"].append(save_and_cancel(job, args.output))
            journal("saved-" + job["jid"])
        subprocess.run([*compose, "stop", "taskmanager-1", "taskmanager-2", "jobmanager"], check=True)
        subprocess.run([*compose, "up", "-d", "--no-deps", "--wait", "jobmanager", "taskmanager-1", "taskmanager-2"], check=True)
        for saved in report["savepoints"]:
            attribution = "attribution" in saved["name"].lower()
            entry = "io.adpulse.AttributionJob" if attribution else "io.adpulse.CleanJob"
            output = args.output / (entry.rsplit(".", 1)[-1] + ".json")
            subprocess.run([sys.executable, "scripts/restore_job.py", "--job-id", saved["job_id"],
                            "--entry-class", entry, "--state-path", saved["savepoint"],
                            "--parallelism", "6" if attribution else "2", "--output", str(output)], check=True)
            report["restores"].append(json.loads(output.read_text()))
            journal("restored-" + entry)
        # Start only the paused collector; existing bootstrap must not recreate jobs.
        container = subprocess.check_output([*compose, "ps", "-aq", "collector"], text=True).strip()
        subprocess.run(["docker", "start", container], check=True)
        report.update(passed=True, taskmanager_process_memory_mib_each=8192, taskmanagers=2,
                      clean_parallelism=2, attribution_parallelism=6, completed_at_epoch=time.time())
        journal("capacity-profile-restored")
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        journal("failed-inspect-retained-state-before-resuming")
        raise


if __name__ == "__main__":
    main()
