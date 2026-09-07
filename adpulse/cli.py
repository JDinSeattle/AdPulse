from __future__ import annotations

import argparse
import json
from pathlib import Path

from .archive import LocalObjects, S3Objects, archive_batch, reconcile, verified_records
from .common import canonical, digest, load_rules, now_ms, read_jsonl, write_json
from .generator import generate, receipts, save_dataset, send
from .oracle import calculate, compare
from .storage import LocalSnapshotStore, ClickHouse


def demo(output):
    root = Path(output)
    dataset = generate(users=200, scenario="mixed")
    save_dataset(dataset, root)
    packets = receipts(dataset["transport"])
    result = calculate(packets, release_id="demo-v1")
    expected = calculate(receipts(dataset["truth"]), release_id="truth")
    check = compare(expected, result)
    if not check["passed"]:
        raise AssertionError(check)
    source_rows = [dict(topic="adpulse.raw", partition=i % 3, offset=i // 3, value=p) for i, p in enumerate(packets)]
    receipt_manifest = dict(receipt_ids=[p["receipt_id"] for p in packets], count=len(packets),
                            records=[dict(receipt_id=r["value"]["receipt_id"], topic=r["topic"], partition=r["partition"],
                                          offset=r["offset"], sha256=digest(r["value"])) for r in source_rows])
    source_rows.append(dict(topic="adpulse.receipts", partition=0, offset=0, value=receipt_manifest))
    objects = LocalObjects(root / "archive")
    archive_batch(objects, source_rows)
    coverage = reconcile(verified_records(objects), result["quality"])
    store = LocalSnapshotStore(root / "snapshots.sqlite")
    for i, row in enumerate(result["metrics"] + result["associations"]):
        store.insert("adpulse.results.demo-v1", 0, i, row)
        store.insert("adpulse.results.demo-v1", 0, i, row)
    sink_check = compare(result, store.read("demo-v1"))
    report = dict(mode="local-batch-and-durable-sink-harness", flink_executed=False,
                  input_records=len(packets), truth_events=len(dataset["truth"]),
                  matched_conversions=len(result["associations"]), quality_records=len(result["quality"]),
                  oracle_comparison=check, sink_retry_comparison=sink_check, completeness=coverage)
    write_json(root / "result.json", result)
    write_json(root / "report.json", report)
    return report


def archived_dimensions(rows):
    versions = {}
    for record in rows:
        if not record["topic"].endswith("campaign_versions") or record["value"] is None:
            continue
        envelope = record["value"].get("payload", record["value"])
        if envelope is None:
            continue
        deleted = envelope.get("op") == "d"
        row = envelope.get("before" if deleted else "after") if "op" in envelope else envelope
        if not row:
            continue
        row = dict(row, deleted=deleted or row.get("deleted", False))
        if isinstance(row["attributes"], str):
            row["attributes"] = json.loads(row["attributes"])
        versions[(row["campaign_id"], row["effective_from"], row["source_version"])] = row
    return list(versions.values())


def main(argv=None):
    parser = argparse.ArgumentParser(description="AdPulse development and operations")
    sub = parser.add_subparsers(dest="command", required=True)
    d = sub.add_parser("demo", help="Run deterministic offline oracle/archive/sink invariants")
    d.add_argument("--output", default="artifacts/demo")
    g = sub.add_parser("generate", help="Generate independent business truth and transport events")
    g.add_argument("--users", type=int, default=200)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--start-ms", type=int)
    g.add_argument("--scenario", choices=["normal", "mixed", "duplicates", "schema", "out-of-order", "conversion-first", "hotspot"], default="mixed")
    g.add_argument("--output", default="artifacts/input")
    g.add_argument("--send", metavar="COLLECTOR_URL")
    r = sub.add_parser("replay", help="Compute a new isolated release from verified archive or bounded JSONL")
    r.add_argument("--input")
    r.add_argument("--from-s3", action="store_true")
    r.add_argument("--release", required=True)
    r.add_argument("--rules")
    r.add_argument("--output", default="artifacts/replay")
    r.add_argument("--publish", action="store_true")
    r.add_argument("--activate", action="store_true")
    a = sub.add_parser("activate", help="Atomically activate or roll back a validated release")
    a.add_argument("release")
    a.add_argument("--reason", required=True)
    sub.add_parser("releases")
    args = parser.parse_args(argv)
    if args.command == "demo":
        print(canonical(demo(args.output)))
    elif args.command == "generate":
        if not 1 <= args.users <= 1_000_000:
            parser.error("--users must be between 1 and 1000000")
        dataset = generate(args.users, args.seed, args.start_ms if args.start_ms is not None else now_ms(), args.scenario)
        save_dataset(dataset, args.output)
        if args.send:
            acknowledgements = send(dataset["transport"], args.send, batch_prefix=f"generated-{args.seed}")
            write_json(Path(args.output) / "acknowledgements.json", acknowledgements)
        print(canonical({"events": len(dataset["transport"]), "output": args.output}))
    elif args.command == "replay":
        if bool(args.input) == bool(args.from_s3):
            parser.error("Choose exactly one of --input and --from-s3")
        if args.activate and not args.publish:
            parser.error("--activate requires --publish")
        if args.publish and not args.from_s3:
            parser.error("Publishing requires a verified S3 archive and receipt reconciliation")
        dimensions, coverage = [], {"archive_complete": False}
        rules = load_rules(args.rules)
        if args.from_s3:
            objects = S3Objects()
            rows = verified_records(objects)
            acknowledged = ClickHouse().query("SELECT arrayJoin(receipt_ids) AS receipt_id FROM adpulse.receipts FINAL FORMAT JSONEachRow")
            if not acknowledged:
                raise ValueError("No acknowledged source scope to replay")
            expected_ids = {r["receipt_id"] for r in acknowledged}
            lineage = ClickHouse().query("SELECT receipt_id,disposition FROM adpulse.quality FINAL WHERE disposition!='signal' FORMAT JSONEachRow")
            coverage = reconcile(rows, lineage, expected_ids)
            if not coverage["archive_complete"] or not coverage["lineage_complete"]:
                raise ValueError(f"Archive or source lineage incomplete: {coverage}")
            packets = [r["value"] for r in rows if r["topic"].endswith(".raw") and r["value"]["receipt_id"] in expected_ids]
            packets.sort(key=lambda p: (p["received_at"], p["batch_id"], p["index"]))
            dimensions = archived_dimensions(rows)
            manifest_hash = digest({"archive_manifests": objects.keys("manifests/"), "receipt_ids": sorted(expected_ids)})
            objects.put(f"rules/{digest(rules)}.json", canonical(rules).encode())
        else:
            packets = read_jsonl(args.input)
            manifest_hash = digest(packets)
        result = calculate(packets, rules, args.release, dimensions)
        root = Path(args.output)
        write_json(root / "rules.json", rules)
        write_json(root / "result.json", result)
        report = dict(release_id=args.release, input_manifest_sha256=manifest_hash, rules_sha256=digest(rules), **coverage)
        if args.publish:
            from . import releases
            db = ClickHouse()
            old = releases.active()
            baseline = db.snapshots(old)["metrics"] if old else []
            releases.publish_snapshot(result, rules, manifest_hash, baseline)
            report.update(releases.wait_for_snapshot(result, db))
            releases.mark_validated(args.release, report)
            if args.activate:
                report["activation"] = releases.activate(args.release, "verified full archive replay")
            write_json(root / "result.json", result)
        write_json(root / "report.json", report)
        print(canonical(report))
    else:
        from . import releases
        print(json.dumps(releases.activate(args.release, args.reason) if args.command == "activate" else releases.list_releases(), default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
