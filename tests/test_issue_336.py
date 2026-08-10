# Tests for issue #336 — proteomics data acquisition method under PRIDE:0000659.
#   - AC= must be a descendant of the configured parent_accession
#   - NT=/AC= mismatch remains an error (issue #321)
#   - plain / non-lowercase encodings warn when recommend_nt_ac is enabled

import logging

import pandas as pd
import pytest

pytestmark = pytest.mark.ontology


def _client_with(search=None, labels_for_accession=None, is_under_parent=None, use_cache=False):
    from sdrf_pipelines.ols.ols import OlsClient

    client = OlsClient.__new__(OlsClient)
    client.use_cache = use_cache
    if search is not None:
        client.search = search
    if labels_for_accession is not None:
        client.labels_for_accession = labels_for_accession
    if is_under_parent is not None:
        client.is_under_parent = is_under_parent
    return client


def _codes(errors):
    return [e.error_code.value for e in errors]


_LABEL_HITS = {
    "data-dependent acquisition": [{"label": "Data-dependent acquisition", "obo_id": "PRIDE:0000627"}],
    "data-independent acquisition": [{"label": "Data-independent acquisition", "obo_id": "PRIDE:0000450"}],
    "selected reaction monitoring": [{"label": "selected reaction monitoring", "obo_id": "PRIDE:0000630"}],
    "parallel reaction monitoring": [{"label": "parallel reaction monitoring", "obo_id": "PRIDE:0000629"}],
}

_ACC_LABELS: dict[str, set[str] | None] = {
    "pride:0000627": {"data-dependent acquisition"},
    "pride:0000450": {"data-independent acquisition"},
    "pride:0000630": {"selected reaction monitoring"},
    "pride:0000629": {"parallel reaction monitoring"},
    "pride:0000449": {"gel image file uri"},
    "pride:0000531": {"itraq113"},
    "pride:0000311": {"obsolete selected reaction monitoring"},
    "ms:1003221": {"data-dependent acquisition"},
    "ms:1000206": {"selected reaction monitoring"},
    "ncit:c161786": {"data-independent acquisition", "dia"},
}

_UNDER_PARENT = {
    ("pride:0000627", "pride:0000659"): True,
    ("pride:0000450", "pride:0000659"): True,
    ("pride:0000630", "pride:0000659"): True,
    ("pride:0000629", "pride:0000659"): True,
    ("pride:0000449", "pride:0000659"): False,
    ("pride:0000531", "pride:0000659"): False,
    ("pride:0000311", "pride:0000659"): False,
    ("ms:1003221", "pride:0000659"): False,
    ("ms:1000206", "pride:0000659"): False,
    ("ncit:c161786", "pride:0000659"): False,
}


def _make_validator(cache_only=False, recommend=True):
    from sdrf_pipelines.sdrf.validators import OntologyValidator

    def fake_search(term, ontology=None, exact=True, use_ols_cache_only=False, **kwargs):
        return _LABEL_HITS.get(term.lower(), [])

    def fake_labels_for_accession(accession, use_ols_cache_only=False):
        if use_ols_cache_only:
            return None
        return _ACC_LABELS.get(accession.lower())

    def fake_is_under_parent(accession, parent_accession, use_ols_cache_only=False):
        if use_ols_cache_only:
            return None
        return _UNDER_PARENT.get((accession.lower(), parent_accession.lower()), False)

    params = {
        "ontologies": ["pride"],
        "error_level": "error",
        "parent_accession": "PRIDE:0000659",
        "recommend_nt_ac": recommend,
    }
    if cache_only:
        params["use_ols_cache_only"] = True
    return OntologyValidator(
        params=params,
        client=_client_with(
            search=fake_search,
            labels_for_accession=fake_labels_for_accession,
            is_under_parent=fake_is_under_parent,
        ),
    )


def _errs(value, **kwargs):
    return _make_validator(**kwargs).validate(
        pd.Series([value]), column_name="comment[proteomics data acquisition method]"
    )


def test_good_nt_ac_passes():
    assert _errs("NT=data-dependent acquisition;AC=PRIDE:0000627") == []
    assert _errs("NT=data-independent acquisition;AC=PRIDE:0000450") == []
    assert _errs("NT=selected reaction monitoring;AC=PRIDE:0000630") == []


def test_plain_lowercase_passes_with_encoding_warning():
    errors = _errs("data-dependent acquisition")
    assert _codes(errors) == ["ONTOLOGY_ENCODING_RECOMMENDATION"]
    assert all(e.error_type == logging.WARNING for e in errors)


def test_title_case_plain_warns_for_case_and_encoding():
    errors = _errs("Data-dependent acquisition")
    assert _codes(errors) == ["ONTOLOGY_ENCODING_RECOMMENDATION"]
    assert "lowercase" in errors[0].message.lower() or "lowercase" in str(errors[0]).lower()


def test_title_case_nt_with_good_ac_warns_for_case_only():
    errors = _errs("NT=Data-dependent acquisition;AC=PRIDE:0000627")
    assert _codes(errors) == ["ONTOLOGY_ENCODING_RECOMMENDATION"]
    assert all(e.error_type == logging.WARNING for e in errors)


def test_wrong_pride_accession_is_error():
    errors = _errs("NT=data-dependent acquisition;AC=PRIDE:0000449")
    codes = set(_codes(errors))
    assert "ONTOLOGY_ACCESSION_MISMATCH" in codes or "ONTOLOGY_NOT_UNDER_PARENT" in codes
    assert any(e.error_type == logging.ERROR for e in errors)


def test_foreign_ms_accession_is_error():
    errors = _errs("NT=data-dependent acquisition;AC=MS:1003221")
    assert "ONTOLOGY_NOT_UNDER_PARENT" in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors)


def test_foreign_ncit_accession_is_error():
    errors = _errs("NT=data-independent acquisition;AC=NCIT:C161786")
    assert "ONTOLOGY_NOT_UNDER_PARENT" in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors)


def test_srm_ms_accession_is_error():
    errors = _errs("NT=selected reaction monitoring;AC=MS:1000206")
    assert "ONTOLOGY_NOT_UNDER_PARENT" in _codes(errors)


def test_unrecognizable_free_text_is_error():
    errors = _errs("not a real acquisition method")
    assert _codes(errors) == ["ONTOLOGY_TERM_NOT_FOUND"]


def test_cache_only_downgrades_parent_check_to_warning():
    errors = _errs("NT=data-dependent acquisition;AC=MS:1003221", cache_only=True)
    assert "ONTOLOGY_NOT_UNDER_PARENT" in _codes(errors)
    assert all(
        e.error_type == logging.WARNING
        for e in errors
        if e.error_code.value == "ONTOLOGY_NOT_UNDER_PARENT"
    )


def test_recommend_flag_can_be_disabled():
    assert _errs("data-dependent acquisition", recommend=False) == []
