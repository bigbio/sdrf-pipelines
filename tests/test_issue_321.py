"""Tests for issue #321 — a clean parse_sdrf pass validates syntax, not truth.

A1: the ontology validator must check that an ``AC=`` accession exists and agrees
    with its ``NT=`` label, not just that the label resolves.
A3: OLS exact search is case-sensitive server-side, so a lower-cased query
    (``t cell``) must still resolve a capitalised label (``T cell``).
"""

import pandas as pd
import pytest

from sdrf_pipelines.ols.ols import OlsClient

pytestmark = pytest.mark.ontology


class _FakeOntologyClient(OlsClient):
    """OlsClient stub with a tiny in-memory ontology; no network/cache."""

    def __init__(self):  # skip the heavy real __init__
        pass

    def search(self, term, ontology=None, exact=True, use_ols_cache_only=False, **kwargs):
        db = {
            "homo sapiens": [{"label": "Homo sapiens", "obo_id": "NCBITaxon:9606"}],
            "t cell": [{"label": "T cell", "obo_id": "CL:0000084"}],
        }
        return db.get(term.lower(), [])


def _codes(errors):
    return [e.error_code.value for e in errors]


def _make_validator():
    from sdrf_pipelines.sdrf.validators import OntologyValidator

    return OntologyValidator(
        params={"ontologies": ["ncbitaxon"], "error_level": "error"},
        client=_FakeOntologyClient(),
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

    class _CaseClient(OlsClient):
        def __init__(self):
            self.use_cache = False

        def ols_search(self, term, ontology=None, exact=True, **kwargs):
            if exact:
                return []  # OLS exact is case-sensitive: `t cell` misses `T cell`
            return [{"label": "T cell", "obo_id": "CL:0000084"}]

    client = _CaseClient()
    result = client.search("t cell", ontology="cl", exact=True)
    assert result and result[0]["obo_id"] == "CL:0000084"


def test_fuzzy_fallback_keeps_only_exact_label_matches():
    """The fallback must not admit a near-but-different label."""

    class _NoisyClient(OlsClient):
        def __init__(self):
            self.use_cache = False

        def ols_search(self, term, ontology=None, exact=True, **kwargs):
            if exact:
                return []
            return [{"label": "T cells", "obo_id": "CL:9999999"}]  # plural, not an exact match

    client = _NoisyClient()
    result = client.search("t cell", ontology="cl", exact=True)
    assert result == []
