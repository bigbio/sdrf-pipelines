"""Tests for issue #341 - a term missing from the bundled snapshot is not a hard error offline."""

import logging

import pandas as pd
import pytest

pytestmark = pytest.mark.ontology


def _client(search):
    from sdrf_pipelines.ols.ols import OlsClient

    c = OlsClient.__new__(OlsClient)
    c.use_cache = False
    c.search = search  # type: ignore[method-assign]
    return c


def _validator(cache_only):
    from sdrf_pipelines.sdrf.validators import OntologyValidator

    def fake_search(term, ontology=None, exact=True, use_ols_cache_only=False, **kwargs):
        # the snapshot knows 'single cell' but not the newer 'study sample'
        return [{"label": "single cell", "obo_id": "PRIDE:0000897"}] if term.lower() == "single cell" else []

    params = {"ontologies": ["pride"], "error_level": "error"}
    if cache_only:
        params["use_ols_cache_only"] = True
    return OntologyValidator(params=params, client=_client(fake_search))


def _run(v, value):
    return v.validate(pd.Series([value]), column_name="characteristics[sample type]")


def test_known_term_passes_offline():
    assert _run(_validator(cache_only=True), "single cell") == []


def test_term_newer_than_snapshot_is_a_warning_offline():
    """PRIDE:0001013 'study sample' post-dates the bundled snapshot; it must not fail the file."""
    errors = _run(_validator(cache_only=True), "study sample")
    assert [e.error_code.value for e in errors] == ["ONTOLOGY_TERM_NOT_FOUND"]
    assert all(e.error_type == logging.WARNING for e in errors)
    assert "may be newer than" in str(errors[0])


def test_unknown_term_is_still_an_error_online():
    """With OLS reachable a miss is authoritative, so it stays an error."""
    errors = _run(_validator(cache_only=False), "study sample")
    assert [e.error_code.value for e in errors] == ["ONTOLOGY_TERM_NOT_FOUND"]
    assert all(e.error_type == logging.ERROR for e in errors)
