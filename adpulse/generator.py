"""Generate business truth first, then independently corrupt transport copies."""
from __future__ import annotations

import copy
import random
from pathlib import Path

from .common import canonical, write_json


def generate(users=100, seed=7, start_ms=1788696000000, scenario="mixed"):
    rng = random.Random(seed)
    truth, expected = [], []
    for i in range(users):
        timestamp = start_ms + i * 1000
        base = {
            "schema_version": 1, "app_id": "short-video", "app_version": "1.4.0",
            "advertiser_id": f"advertiser-{i % 3}", "user_id": f"synthetic-user-{seed}-{i}",
            "campaign_id": f"campaign-{i % 4}", "trace_id": f"trace-{seed}-{i}",
        }
        if scenario == "hotspot":
            base["campaign_id"] = "campaign-0" if rng.random() < 0.9 else "campaign-1"
        impression = dict(base, event_type="impression", event_id=f"ev-i-{seed}-{i}",
                          impression_id=f"imp-{seed}-{i}", ad_id=f"ad-{i % 8}",
                          event_time=timestamp, region=["US", "JP", "GB"][i % 3],
                          experiment_id="creative-test", variant="control" if i % 2 == 0 else "treatment")
        truth.append(impression)
        if rng.random() < 0.65:
            click = dict(base, event_type="click", event_id=f"ev-c-{seed}-{i}",
                         click_id=f"click-{seed}-{i}", impression_id=impression["impression_id"],
                         event_time=timestamp + 1500)
            truth.append(click)
            if rng.random() < 0.45:
                for j in range(2 if i % 7 == 0 else 1):
                    conversion = dict(base, event_type="conversion", event_id=f"ev-v-{seed}-{i}-{j}",
                                      conversion_id=f"conv-{seed}-{i}-{j}", click_id=click["click_id"],
                                      event_time=timestamp + 5000 + j * 1000, conversion_type="purchase",
                                      value_minor=rng.randint(100, 10000), currency="USD")
                    truth.append(conversion)
                    expected.append({"conversion_id": conversion["conversion_id"],
                                     "click_id": click["click_id"], "impression_id": impression["impression_id"],
                                     "status": "matched", "reason": "MATCHED"})
    transport = copy.deepcopy(truth)
    if scenario in {"mixed", "duplicates"}:
        for event in truth[::9]:
            transport.append(copy.deepcopy(event))
            retry = dict(event, event_id=event["event_id"] + "-new-id")
            transport.append(retry)
    if scenario in {"mixed", "schema"} and truth:
        broken = dict(truth[0], event_id=f"bad-type-{seed}", event_time="not-an-integer")
        transport.append(broken)
    if scenario in {"mixed", "out-of-order"}:
        rng.shuffle(transport)
    if scenario == "conversion-first":
        transport.sort(key=lambda e: {"conversion": 0, "click": 1, "impression": 2}[e["event_type"]])
    return {"seed": seed, "truth": truth, "transport": transport, "expected_associations": expected}


def save_dataset(dataset, output):
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    for key in ("truth", "transport"):
        (root / f"{key}.jsonl").write_text("".join(canonical(e) + "\n" for e in dataset[key]))
    write_json(root / "ground-truth.json", {"seed": dataset["seed"], "associations": dataset["expected_associations"]})


def receipts(events, received_at=1788699600000, batch_id="offline"):
    return [dict(receipt_id=f"{batch_id}:{i}", batch_id=batch_id, index=i,
                 received_at=received_at + i, event=e) for i, e in enumerate(events)]


def send(events, url, batch_size=100, batch_prefix="synthetic"):
    import requests
    acknowledgements = []
    for start in range(0, len(events), batch_size):
        response = requests.post(url.rstrip("/") + "/v1/events", json={
            "client_batch_id": f"{batch_prefix}-{start}", "events": events[start:start + batch_size]
        }, timeout=60)
        response.raise_for_status()
        acknowledgements.append(response.json())
    return acknowledgements


if __name__ == "__main__":
    from .cli import main
    main(["generate", *__import__("sys").argv[1:]])
