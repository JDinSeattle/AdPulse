from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(os.environ.get("ADPULSE_ROOT", Path(__file__).resolve().parents[1]))


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def load_rules(path=None):
    rules = json.loads(Path(path or ROOT / "contracts/rules-v1.json").read_text())
    Draft202012Validator(json.loads((ROOT / "contracts/rules.schema.json").read_text())).validate(rules)
    if rules["state_retention_ms"] < rules["attribution_window_ms"] + rules["join_wait_ms"]:
        raise ValueError("state_retention_ms must cover attribution window + join wait")
    return rules


def validator():
    return Draft202012Validator(json.loads((ROOT / "contracts/event.schema.json").read_text()))


def scope(event):
    return (event["advertiser_id"], event["app_id"], event["user_id"])


def business_key(event):
    kind = event["event_type"]
    # Business uniqueness is advertiser/app scoped; user is checked by the joins.
    return (event["advertiser_id"], event["app_id"], kind, event[f"{kind}_id"])


def write_json(path, value):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def read_jsonl(path):
    with Path(path).open() as handle:
        return [json.loads(line) for line in handle if line.strip()]
