# Tests for issue #321 — a clean parse_sdrf pass validates syntax, not truth.
#   A1: the ontology validator must check that an AC= accession exists and agrees with its NT= label,
#       not just that the label resolves.
#   A3: OLS exact search is case-sensitive server-side, so a lower-cased query (t cell) must still
#       resolve a capitalised label (T cell).

import pandas as pd
import pytest

from sdrf_pipelines.ols.ols import OlsClient

pytestmark = pytest.mark.ontology


def _client_with(search=None, ols_search=None, labels_for_accession=None, use_cache=False):
    """Build an OlsClient without its heavy __init__, stubbing methods as instance attrs."""
    client = OlsClient.__new__(OlsClient)
    client.use_cache = use_cache
    if search is not None:
        client.search = search
    if ols_search is not None:
        client.ols_search = ols_search
    if labels_for_accession is not None:
        client.labels_for_accession = labels_for_accession
    return client


def _codes(errors):
    return [e.error_code.value for e in errors]


# label -> exact-search hits (in the column's ontology). Only ncbitaxon here.
_LABEL_HITS = {
    "homo sapiens": [{"label": "Homo sapiens", "obo_id": "NCBITaxon:9606"}],
}
# accession -> its label + synonyms (what a by-accession lookup resolves to).
# empty set == accession does not exist; a returned None == cannot verify.
_ACC_LABELS = {
    "ncbitaxon:9606": {"homo sapiens"},
    "ncbitaxon:10090": {"mus musculus"},  # a real term, but the wrong one
    "ncbitaxon:99999999": set(),  # does not exist
    "ncit:c161786": {"data-independent acquisition", "dia"},  # valid cross-ontology
}


def _make_validator(cache_only=False):
    from sdrf_pipelines.sdrf.validators import OntologyValidator

    def fake_search(term, ontology=None, exact=True, use_ols_cache_only=False, **kwargs):
        return _LABEL_HITS.get(term.lower(), [])

    def fake_labels_for_accession(accession, use_ols_cache_only=False):
        if use_ols_cache_only:
            return None  # cannot verify offline
        return _ACC_LABELS.get(accession.lower())  # None => unknown accession, not looked up

    params = {"ontologies": ["ncbitaxon"], "error_level": "error"}
    if cache_only:
        params["use_ols_cache_only"] = True
    return OntologyValidator(
        params=params,
        client=_client_with(search=fake_search, labels_for_accession=fake_labels_for_accession),
    )


# --------------------------------------------------------------------------- A1


def _errs(v, value):
    return v.validate(pd.Series([value]), column_name="characteristics[organism]")


def test_correct_label_and_accession_pass():
    # fast path: accession is one the label resolves to
    assert _errs(_make_validator(), "NT=Homo sapiens;AC=NCBITaxon:9606") == []


def test_cross_ontology_accession_matching_by_synonym_passes():
    """An accession not in the label's own ontology, but whose label/synonym matches, is accepted."""
    _ACC_LABELS["other:1"] = {"homo sapiens", "human"}  # matches NT via label
    try:
        assert _errs(_make_validator(), "NT=Homo sapiens;AC=Other:1") == []
    finally:
        del _ACC_LABELS["other:1"]


def test_wrong_accession_resolving_to_other_term_is_error():
    import logging

    errors = _errs(_make_validator(), "NT=Homo sapiens;AC=NCBITaxon:10090")  # that's mouse
    assert _codes(errors) == ["ONTOLOGY_ACCESSION_MISMATCH"]
    assert all(e.error_type == logging.ERROR for e in errors)


def test_nonexistent_accession_is_error():
    import logging

    errors = _errs(_make_validator(), "NT=Homo sapiens;AC=NCBITaxon:99999999")
    assert _codes(errors) == ["ONTOLOGY_ACCESSION_MISMATCH"]
    assert all(e.error_type == logging.ERROR for e in errors)


def test_cache_only_downgrades_to_warning():
    """When agreement cannot be verified offline, it is a warning, not an error."""
    import logging

    errors = _errs(_make_validator(cache_only=True), "NT=Homo sapiens;AC=NCBITaxon:10090")
    assert _codes(errors) == ["ONTOLOGY_ACCESSION_MISMATCH"]
    assert all(e.error_type == logging.WARNING for e in errors)


def test_plain_label_without_accession_is_untouched():
    assert _errs(_make_validator(), "Homo sapiens") == []


def test_invalid_label_reports_once_not_twice():
    """A bad label already yields ONTOLOGY_TERM_NOT_FOUND; no duplicate accession error."""
    errors = _errs(_make_validator(), "NT=Nonexistent species;AC=NCBITaxon:9606")
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
