import json

import pytest

from adpulse.storage import lookup_parameters


def test_lookup_chunks_preserve_unique_unicode_and_escaped_identities_within_bytes():
    keys = [["release", f'\\"广告-{i}'] for i in range(80)]
    parameters = list(lookup_parameters(keys + keys, max_bytes=100))
    assert len(parameters) > 1
    assert all(len(p.encode("utf-8")) <= 100 for p in parameters)
    assert [key for p in parameters for key in json.loads(p)] == keys


def test_lookup_rejects_single_oversized_key_instead_of_truncating_identity():
    with pytest.raises(ValueError, match="byte budget"):
        list(lookup_parameters([["界" * 100]], max_bytes=100))


def test_empty_lookup_needs_no_query_and_exact_boundary_fits():
    assert list(lookup_parameters([])) == []
    assert list(lookup_parameters([["a"]], max_bytes=7)) == ['[["a"]]']
