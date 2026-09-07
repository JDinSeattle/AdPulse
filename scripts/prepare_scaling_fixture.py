"""Create a deterministic, checksummed archive fixture outside measured workloads."""
import argparse
import hashlib
from pathlib import Path

from adpulse.archive import LocalObjects, archive_batch
from adpulse.common import canonical, digest, write_json
from adpulse.generator import generate, receipts


def prepare(output, count):
    output.mkdir(parents=True, exist_ok=False)
    objects = LocalObjects(output / "objects")
    accepted, batch, input_hash = 0, 0, hashlib.sha256()
    while accepted < count:
        events = generate(users=1000, seed=10000+batch, start_ms=1788696000000+batch*1000000, scenario="mixed")["transport"]
        packets = receipts(events[:count-accepted], received_at=1789000000000+accepted, batch_id=f"fixture-{batch:06}")
        rows = [dict(topic="adpulse.raw", partition=i % 3, offset=(accepted+i)//3, value=p) for i, p in enumerate(packets)]
        # Partition derived from the global ordinal, so offsets are unique even
        # across fixture batches whose sizes are not a multiple of three.
        for i, row in enumerate(rows):
            row["partition"] = (accepted+i) % 3
            input_hash.update((canonical(row["value"])+"\n").encode())
        refs = [dict(receipt_id=r["value"]["receipt_id"], topic=r["topic"], partition=r["partition"], offset=r["offset"], sha256=digest(r["value"])) for r in rows]
        receipt = dict(count=len(rows), receipt_ids=[r["receipt_id"] for r in refs], records=refs)
        rows.append(dict(topic="adpulse.receipts", partition=0, offset=batch, value=receipt))
        archive_batch(objects, rows)
        accepted += len(packets)
        batch += 1
    files = {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(output.rglob('*')) if p.is_file()}
    report = dict(records=accepted, batches=batch, packet_stream_sha256=input_hash.hexdigest(),
                  file_manifest_sha256=digest(files), files=files,
                  scope="Deterministic synthetic mixed-schema, duplicate and reordered transport; generated outside measurement")
    write_json(output / "fixture.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--records", type=int, default=200000)
    args = parser.parse_args()
    if args.records < 1:
        parser.error("records must be positive")
    report = prepare(args.output, args.records)
    print(canonical({k: report[k] for k in ('records', 'batches', 'packet_stream_sha256', 'file_manifest_sha256')}))
