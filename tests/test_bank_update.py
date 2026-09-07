"""Protect user-owned bank entries when an evidence refresh races with other edits."""
import copy

import pytest

from scripts.update_experience_bank import PROJECT_ID, encode, merge, sha


def fixture():
    own = {"id": PROJECT_ID, "metrics": [{"id": "m1"}], "bullet_candidates": [{"id": "b1"}]}
    proof = {"id": "e1", "project_id": PROJECT_ID, "claim": "original"}
    exp = {"experiences": [{"id": "other", "title": "Keep this"}, own], "unknown_schema_field": 42}
    bank = {"schema_version": "1.0", "projects": {PROJECT_ID: {"evidence_ids": ["e1"]}, "other": {}},
            "evidence": [{"id": "other-e", "project_id": "other"}, proof]}
    candidate = copy.deepcopy({"experience": own, "project": bank["projects"][PROJECT_ID], "evidence": [proof]})
    candidate["base_project_sha256"] = sha(encode(candidate))
    candidate["evidence"][0]["claim"] = "verified update"
    return exp, bank, candidate


def test_refresh_rebases_other_experiences_and_preserves_schema_and_order():
    exp, bank, candidate = fixture()
    exp["experiences"][0]["title"] = "A concurrent user edit"
    before = copy.deepcopy((exp, bank))
    updated, proofs = merge(exp, bank, candidate)
    assert updated["experiences"][0] == exp["experiences"][0]
    assert updated["unknown_schema_field"] == 42
    assert proofs["evidence"][0] == bank["evidence"][0]
    assert proofs["evidence"][1]["claim"] == "verified update"
    assert (exp, bank) == before


def test_refresh_rejects_concurrent_edit_to_target():
    exp, bank, candidate = fixture()
    bank["evidence"][1]["claim"] = "User corrected the evidence"
    with pytest.raises(ValueError, match="Concurrent AdPulse"):
        merge(exp, bank, candidate)


@pytest.mark.parametrize("key", ["metrics", "bullet_candidates"])
def test_refresh_cannot_remove_stable_ids(key):
    exp, bank, candidate = fixture()
    candidate["experience"][key] = []
    with pytest.raises(ValueError, match="Stable"):
        merge(exp, bank, candidate)


def test_refresh_cannot_take_another_projects_evidence_id():
    exp, bank, candidate = fixture()
    candidate["evidence"].append({"id": "other-e", "project_id": PROJECT_ID})
    with pytest.raises(ValueError, match="ID conflict"):
        merge(exp, bank, candidate)


def test_refresh_cannot_drop_historical_evidence():
    exp, bank, candidate = fixture()
    candidate["evidence"] = []
    with pytest.raises(ValueError, match="Stable evidence"):
        merge(exp, bank, candidate)
