"""
Tests for issue #321 — a clean parse_sdrf pass validates syntax, not truth.

A1: the ontology validator must check that an ``AC=`` accession exists and agrees
    with its ``NT=`` label, not just that the label resolves.
A3: OLS exact search is case-sensitive server-side, so a lower-cased query
    (``t cell``) must still resolve a capitalised label (``T cell``).
"""

import pandas as pd
import pytest

from sdrf_pipelines.ols.ols import OlsClient

pytestmark = pytest.mark.ontology


def _client_with(search=None, ols_search=None, use_cache=False):
    """Build an OlsClient without its heavy __init__, stubbing methods as instance attrs."""
    client = OlsClient.__new__(OlsClient)
    client.use_cache = use_cache
    if search is not None:
        client.search = search
    if ols_search is not None:
        client.ols_search = ols_search
    return client


def _codes(errors):
    return [e.error_code.value for e in errors]


def _make_validator():
    from sdrf_pipelines.sdrf.validators import OntologyValidator

    db = {
        "homo sapiens": [{"label": "Homo sapiens", "obo_id": "NCBITaxon:9606"}],
        "t cell": [{"label": "T cell", "obo_id": "CL:0000084"}],
    }

    def fake_search(term, ontology=None, exact=True, use_ols_cache_only=False, **kwargs):
        return db.get(term.lower(), [])

    return OntologyValidator(
        params={"ontologies": ["ncbitaxon"], "error_level": "error"},
        client=_client_with(search=fake_search),
    )


# --------------------------------------------------------------------------- A1


def test_correct_label_and_accession_pass():
    v = _make_validator()
    errors = v.validate(pd.Series(["NT=Homo sapiens;AC=NCBITaxon:9606"]), column_name="characteristics[organism]")
    assert errors == []


def test_bogus_accession_with_valid_label_is_flagged():
    """The historical silent-corruption case: real label, nonexistent accession."""
    v = _make_validator()
    errors = v.validate(pd.Series(["NT=Homo sapiens;AC=NCBITaxon:99999999"]), column_name="characteristics[organism]")
    assert _codes(errors) == ["ONTOLOGY_ACCESSION_MISMATCH"]


def test_plain_label_without_accession_is_untouched():
    """Additive: a value with no AC= must not gain a new error."""
    v = _make_validator()
    errors = v.validate(pd.Series(["Homo sapiens"]), column_name="characteristics[organism]")
    assert errors == []


def test_invalid_label_reports_once_not_twice():
    """A bad label already yields ONTOLOGY_TERM_NOT_FOUND; no duplicate accession error."""
    v = _make_validator()
    errors = v.validate(
        pd.Series(["NT=Nonexistent species;AC=NCBITaxon:9606"]), column_name="characteristics[organism]"
    )
    assert _codes(errors) == ["ONTOLOGY_TERM_NOT_FOUND"]


def test_sentinels_are_not_accession_checked():
    v = _make_validator()
    errors = v.validate(pd.Series(["not available", "not applicable"]), column_name="characteristics[organism]")
    assert errors == []


# --------------------------------------------------------------------------- A3


def test_case_insensitive_exact_recovers_capitalised_label():
    """`t cell` (lower-cased by the caller) must resolve `T cell` via the fuzzy+filter fallback."""

    def fake_ols_search(term, ontology=None, exact=True, **kwargs):
        if exact:
            return []  # OLS exact is case-sensitive: `t cell` misses `T cell`
        return [{"label": "T cell", "obo_id": "CL:0000084"}]

    client = _client_with(ols_search=fake_ols_search)
    result = client.search("t cell", ontology="cl", exact=True)
    assert result and result[0]["obo_id"] == "CL:0000084"


def test_fuzzy_fallback_keeps_only_exact_label_matches():
    """The fallback must not admit a near-but-different label."""

    def fake_ols_search(term, ontology=None, exact=True, **kwargs):
        if exact:
            return []
        return [{"label": "T cells", "obo_id": "CL:9999999"}]  # plural, not an exact match

    client = _client_with(ols_search=fake_ols_search)
    result = client.search("t cell", ontology="cl", exact=True)
    assert result == []
