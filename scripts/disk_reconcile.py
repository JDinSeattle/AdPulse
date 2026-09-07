"""Run exhaustive disk-backed reconciliation without mutating business services."""
import argparse
import json
from pathlib import Path

from adpulse.common import write_json
from adpulse.reconciliation import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=Path, default=Path("artifacts/inspection/archive.sqlite"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", action="store_true", help="Recheck all immutable objects, including indexed ones")
    parser.add_argument("--release", default="live-v1")
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Refusing to overwrite measured evidence")
    result = run(args.index, args.output.with_suffix(".sqlite"), release=args.release, audit=args.audit)
    write_json(args.output, result)
    print(json.dumps(result), flush=True)
    raise SystemExit(0 if result["passed"] else 1)
