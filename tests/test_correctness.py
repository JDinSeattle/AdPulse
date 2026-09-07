from copy import deepcopy

import pytest

from adpulse.common import load_rules
from adpulse.generator import generate, receipts
from adpulse.oracle import calculate, compare


@pytest.fixture
def triplet():
    data = generate(users=10, scenario="normal")["truth"]
    conversion = next(e for e in data if e["event_type"] == "conversion")
    click = next(e for e in data if e.get("click_id") == conversion["click_id"] and e["event_type"] == "click")
    impression = next(e for e in data if e.get("impression_id") == click["impression_id"] and e["event_type"] == "impression")
    return deepcopy([impression, click, conversion])


@pytest.mark.parametrize("scenario", ["normal", "mixed", "duplicates", "out-of-order", "conversion-first", "schema", "hotspot"])
def test_transport_corruption_preserves_ground_truth(scenario):
    data = generate(users=150, scenario=scenario)
    expected = calculate(receipts(data["truth"]))
    actual = calculate(receipts(data["transport"]))
    assert compare(expected, actual)["passed"]
    assert len(actual["associations"]) == len(data["expected_associations"])
    assert all(a["status"] == "matched" for a in actual["associations"])
    assert len([q for q in actual["quality"] if q["disposition"] != "signal"]) == len(data["transport"])


@pytest.mark.parametrize("delta,reason", [(-1, "CLICK_AFTER_CONVERSION"), (0, "MATCHED"), (86400000, "MATCHED"), (86400001, "ATTRIBUTION_WINDOW_EXCEEDED")])
def test_inclusive_24_hour_boundary(triplet, delta, reason):
    triplet[2]["event_time"] = triplet[1]["event_time"] + delta
    assert calculate(receipts(triplet))["associations"][0]["reason"] == reason


@pytest.mark.parametrize("field,value,reason", [("user_id", "other-user", "IDENTITY_MISMATCH"), ("campaign_id", "other-campaign", "CAMPAIGN_MISMATCH"), ("advertiser_id", "other-advertiser", "CLICK_NOT_FOUND"), ("app_id", "other-app", "CLICK_NOT_FOUND")])
def test_identity_scope_isolation(triplet, field, value, reason):
    triplet[2][field] = value
    assert calculate(receipts(triplet))["associations"][0]["reason"] == reason


def test_multiple_conversions_do_not_inflate_cvr(triplet):
    triplet.append(dict(triplet[2], event_id="second-event", conversion_id="second-conversion", currency="JPY"))
    rows = calculate(receipts(triplet))["metrics"]
    clicks = [m for m in rows if m["cohort_basis"] == "click"]
    assert sum(m["values"]["clicks"] for m in clicks) == 1
    assert sum(m["values"]["converted_clicks"] for m in clicks) == 1
    assert sum(m["values"]["matched_conversions"] for m in clicks) == 2
    assert {m["currency"] for m in rows if m["cohort_basis"] == "conversion_value"} == {"USD", "JPY"}


def test_click_cohort_does_not_use_conversion_occurrence_window(triplet):
    triplet[2]["event_time"] += 3600000
    rows = calculate(receipts(triplet))["metrics"]
    click_metrics = [m for m in rows if m["cohort_basis"] == "click"]
    assert len(click_metrics) == 1
    assert click_metrics[0]["values"]["converted_clicks"] == 1
    assert click_metrics[0]["window_start"] == triplet[1]["event_time"] // 60000 * 60000


def test_experiment_propagation_ignores_current_dimension(triplet):
    dim = [{"campaign_id": triplet[0]["campaign_id"], "effective_from": 0, "source_version": 1,
            "attributes": {"channel": "new-channel", "variant": "should-not-override"}}]
    result = calculate(receipts(triplet), dimensions=dim)
    assert result["associations"][0]["variant"] == triplet[0]["variant"]
    assert all(m["channel"] == "new-channel" for m in result["metrics"])


def test_historical_dimension_does_not_invent_snapshot_history(triplet):
    dim = [{"campaign_id": triplet[0]["campaign_id"], "effective_from": triplet[0]["event_time"] + 100000,
            "source_version": 2, "attributes": {"channel": "future"}}]
    assert all(m["channel"] == "unknown" for m in calculate(receipts(triplet), dimensions=dim)["metrics"])


def test_missing_exposure_has_quality_and_unknown_group(triplet):
    result = calculate(receipts(triplet[1:]))
    assert result["associations"][0]["status"] == "matched"
    assert result["associations"][0]["variant"] == "unknown"
    assert any(q["error_code"] == "IMPRESSION_NOT_FOUND" for q in result["quality"])


def test_boolean_and_string_money_are_quarantined(triplet):
    for value in (True, "123"):
        triplet[2]["value_minor"] = value
        result = calculate(receipts(triplet))
        assert not result["associations"]
        assert result["quality"][-1]["disposition"] == "quarantined"


def test_rule_filter_and_replay_are_isolated(triplet):
    rules = load_rules()
    rules["filter_regions"] = [triplet[0]["region"]]
    rules["rule_version"] = "bad-filter"
    bad = calculate(receipts(triplet), rules, "bad")
    good = calculate(receipts(triplet), release_id="good")
    assert bad["associations"][0]["variant"] == "unknown"
    assert good["associations"][0]["variant"] == triplet[0]["variant"]
    assert any(q["disposition"] == "filtered" for q in bad["quality"])


def test_frozen_rule_schema_allowlist_is_enforced(triplet):
    rules = load_rules()
    rules["schema_versions"] = [2]
    result = calculate(receipts(triplet), rules)
    assert not result["metrics"] and not result["associations"]
    assert all(q["error_code"] == "SCHEMA_VERSION_DISABLED" for q in result["quality"])
