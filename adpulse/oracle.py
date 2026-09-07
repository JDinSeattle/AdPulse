"""Independent, bounded batch oracle. Never imports the streaming implementation.

Dedup is persistent over the supplied replay scope. The streaming promise is only
10 minutes; discrepancies outside that bound are repaired in a new release.
"""
from __future__ import annotations

from copy import deepcopy

from .common import business_key, canonical, digest, load_rules, scope, validator

COUNTERS = ("impressions", "clicks", "matched_conversions", "unmatched_conversions", "converted_clicks", "value_minor")
DIMENSIONS = ("advertiser_id", "app_id", "campaign_id", "region", "app_version", "experiment_id", "variant", "currency", "channel")


def normalize(records, rules):
    validate = validator()
    events, quality, seen_ids, seen_keys = [], [], set(), set()
    for ordinal, raw in enumerate(records):
        packet = raw if "event" in raw else {"event": raw, "receipt_id": f"oracle:{ordinal}", "received_at": ordinal}
        event = deepcopy(packet["event"])
        quality_row = {"receipt_id": packet["receipt_id"], "event_id": event.get("event_id") if isinstance(event, dict) else None,
                       "rule_version": rules["rule_version"], "rule_id": "event-contract-v1", "dataset": "raw_events"}
        if isinstance(event, dict):
            for source, target in rules["field_mappings"].items():
                if source in event and target not in event:
                    event[target] = event.pop(source)
            event["received_at"] = packet["received_at"]
        errors = sorted(validate.iter_errors(event), key=lambda e: str(e.path))
        if errors:
            quality.append(dict(quality_row, disposition="quarantined", error_code="SCHEMA_INVALID", detail=errors[0].message))
            continue
        if event["schema_version"] not in rules["schema_versions"]:
            quality.append(dict(quality_row, disposition="quarantined", error_code="SCHEMA_VERSION_DISABLED"))
            continue
        if event["event_time"] < rules["effective_from"] or event.get("region") in rules["filter_regions"]:
            quality.append(dict(quality_row, disposition="filtered", error_code="RULE_FILTERED"))
            continue
        event_key = (event["advertiser_id"], event["app_id"], event["event_id"])
        biz_key = business_key(event)
        if event_key in seen_ids or biz_key in seen_keys:
            quality.append(dict(quality_row, disposition="duplicate", error_code="DUPLICATE_EVENT_ID" if event_key in seen_ids else "DUPLICATE_BUSINESS_KEY"))
            seen_ids.add(event_key)
            continue
        seen_ids.add(event_key)
        seen_keys.add(biz_key)
        event["_receipt_id"] = packet["receipt_id"]
        events.append(event)
        quality.append(dict(quality_row, disposition="cleaned", error_code="OK"))
    return events, quality


def dimension_at(dimensions, campaign_id, timestamp):
    candidates = [d for d in dimensions if d["campaign_id"] == campaign_id
                  and d["effective_from"] <= timestamp
                  and (d.get("effective_to") is None or timestamp < d["effective_to"])]
    if not candidates:
        return "unknown"
    latest = max(candidates, key=lambda d: (d["source_version"], d["effective_from"]))
    return "deleted" if latest.get("deleted") else latest.get("attributes", {}).get("channel", "unknown")


def key_for(event, impression, timestamp, cohort, currency, rules, dimensions):
    attribution = impression or event
    dims = {
        "advertiser_id": event["advertiser_id"], "app_id": event["app_id"],
        "campaign_id": attribution["campaign_id"], "region": attribution.get("region", "unknown"),
        "app_version": attribution.get("app_version", "unknown"),
        "experiment_id": impression["experiment_id"] if impression else "unknown",
        "variant": impression["variant"] if impression else "unknown", "currency": currency,
        "channel": dimension_at(dimensions, attribution["campaign_id"], timestamp),
    }
    window = timestamp // rules["window_ms"] * rules["window_ms"]
    key = canonical([*[dims[d] for d in DIMENSIONS], window, cohort, rules["policy_version"]])
    return key, dict(dims, window_start=window, cohort_basis=cohort)


def calculate(records, rules=None, release_id="replay-v1", dimensions=()):
    rules = rules or load_rules()
    events, quality = normalize(records, rules)
    impressions = {business_key(e)[:2] + (e["impression_id"],): e for e in events if e["event_type"] == "impression"}
    clicks = {business_key(e)[:2] + (e["click_id"],): e for e in events if e["event_type"] == "click"}
    joined = {}
    metrics = {}

    def add(event, impression, timestamp, cohort, currency="ALL", **values):
        key, dims = key_for(event, impression, timestamp, cohort, currency, rules, dimensions)
        row = metrics.setdefault(key, dict(metric_key=key, release_id=release_id, policy_version=rules["policy_version"],
                                           rule_version=rules["rule_version"], status="final", **dims,
                                           values=dict.fromkeys(COUNTERS, 0)))
        for name, value in values.items():
            row["values"][name] += value

    for impression in impressions.values():
        add(impression, impression, impression["event_time"], "occurrence", impressions=1)
        add(impression, impression, impression["event_time"], "impression", impressions=1)
    for key, click in clicks.items():
        impression = impressions.get(key[:2] + (click["impression_id"],))
        reason = "OK"
        if not impression:
            reason = "IMPRESSION_NOT_FOUND"
        elif scope(impression) != scope(click):
            reason = "IMPRESSION_IDENTITY_MISMATCH"
        elif impression["campaign_id"] != click["campaign_id"]:
            reason = "IMPRESSION_CAMPAIGN_MISMATCH"
        elif impression["event_time"] > click["event_time"]:
            reason = "CLICK_BEFORE_IMPRESSION"
        elif click["event_time"] - impression["event_time"] > rules["attribution_window_ms"]:
            reason = "IMPRESSION_WINDOW_EXCEEDED"
        if reason != "OK":
            impression = None
            quality.append(dict(event_id=click["event_id"], receipt_id=click["_receipt_id"], disposition="signal",
                                rule_id="exposure-link-v1", error_code=reason, rule_version=rules["rule_version"], dataset="clicks"))
        joined[key] = impression
        add(click, impression, click["event_time"], "occurrence", clicks=1)
        add(click, impression, click["event_time"], "click", clicks=1)
        if impression:
            add(click, impression, impression["event_time"], "impression", clicks=1)
    associations, converted = [], set()
    for conversion in (e for e in events if e["event_type"] == "conversion"):
        key = business_key(conversion)[:2] + (conversion["click_id"],)
        click = clicks.get(key)
        reason = "MATCHED"
        if not click:
            reason = "CLICK_NOT_FOUND"
        elif scope(click) != scope(conversion):
            reason = "IDENTITY_MISMATCH"
        elif click["campaign_id"] != conversion["campaign_id"]:
            reason = "CAMPAIGN_MISMATCH"
        elif click["event_time"] > conversion["event_time"]:
            reason = "CLICK_AFTER_CONVERSION"
        elif conversion["event_time"] - click["event_time"] > rules["attribution_window_ms"]:
            reason = "ATTRIBUTION_WINDOW_EXCEEDED"
        matched = reason == "MATCHED"
        impression = joined.get(key) if matched else None
        associations.append(dict(
            association_key=canonical([conversion["advertiser_id"], conversion["app_id"], conversion["conversion_id"]]),
            conversion_id=conversion["conversion_id"], click_id=conversion["click_id"],
            advertiser_id=conversion["advertiser_id"], app_id=conversion["app_id"],
            status="matched" if matched else "unmatched", reason=reason, release_id=release_id,
            rule_version=rules["rule_version"], policy_version=rules["policy_version"],
            event_time=conversion["event_time"], receipt_id=conversion["_receipt_id"],
            experiment_id=impression["experiment_id"] if impression else "unknown",
            variant=impression["variant"] if impression else "unknown"))
        add(conversion, impression, conversion["event_time"], "occurrence",
            **{"matched_conversions" if matched else "unmatched_conversions": 1})
        if matched:
            add(conversion, impression, click["event_time"], "click", matched_conversions=1,
                converted_clicks=int(key not in converted))
            converted.add(key)
            add(conversion, impression, click["event_time"], "conversion_value", conversion["currency"],
                value_minor=conversion["value_minor"], matched_conversions=1)
    return dict(release_id=release_id, rule_version=rules["rule_version"], rules_sha256=digest(rules),
                input_records=len(records), clean_events=len(events), quality=quality,
                associations=sorted(associations, key=lambda a: a["association_key"]),
                metrics=sorted(metrics.values(), key=lambda m: m["metric_key"]))


def compare(expected, actual):
    """Compare business outputs, independent of replay/transport output metadata."""
    left = {m["metric_key"]: m["values"] for m in expected["metrics"] if any(m["values"].values())}
    right = {m["metric_key"]: m["values"] for m in actual["metrics"] if any(m["values"].values())}
    differences = [dict(kind="metric", key=k, expected=left.get(k), actual=right.get(k))
                   for k in sorted(left.keys() | right.keys()) if left.get(k) != right.get(k)]
    for output in ("associations",):
        lmap = {r["association_key"]: (r["status"], r["reason"], r["experiment_id"], r["variant"]) for r in expected[output]}
        rmap = {r["association_key"]: (r["status"], r["reason"], r["experiment_id"], r["variant"]) for r in actual[output]}
        differences += [dict(kind="association", key=k, expected=lmap.get(k), actual=rmap.get(k))
                        for k in sorted(lmap.keys() | rmap.keys()) if lmap.get(k) != rmap.get(k)]
    return {"passed": not differences, "differences": differences}
