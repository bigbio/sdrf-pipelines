# Tests for issue #336 — proteomics data acquisition method under PRIDE:0000659.
#   - AC= must be a descendant of the configured parent_accession
#   - NT=/AC= mismatch remains an error (issue #321)
#   - plain / missing-AC encodings warn when recommend_nt_ac is enabled (case is not enforced)

import logging
from typing import Any, Callable

import pandas as pd
import pytest

from sdrf_pipelines.utils.error_codes import ErrorCode

pytestmark = pytest.mark.ontology


def _client_with(
    search: Callable[..., Any] | None = None,
    labels_for_accession: Callable[..., Any] | None = None,
    is_under_parent: Callable[..., Any] | None = None,
    use_cache: bool = False,
) -> Any:
    from sdrf_pipelines.ols.ols import OlsClient

    client = OlsClient.__new__(OlsClient)
    client.use_cache = use_cache
    # Stub methods on the instance for the test double; mypy flags method assignment, which is
    # exactly what we intend here.
    if search is not None:
        client.search = search  # type: ignore[method-assign]
    if labels_for_accession is not None:
        client.labels_for_accession = labels_for_accession  # type: ignore[method-assign]
    if is_under_parent is not None:
        client.is_under_parent = is_under_parent  # type: ignore[method-assign]
    return client


def _codes(errors: list[Any]) -> list[str]:
    return [e.error_code.value for e in errors]


_LABEL_HITS: dict[str, list[dict[str, str]]] = {
    "data-dependent acquisition": [{"label": "Data-dependent acquisition", "obo_id": "PRIDE:0000627"}],
    "data-independent acquisition": [{"label": "Data-independent acquisition", "obo_id": "PRIDE:0000450"}],
    "selected reaction monitoring": [{"label": "selected reaction monitoring", "obo_id": "PRIDE:0000630"}],
    "parallel reaction monitoring": [{"label": "parallel reaction monitoring", "obo_id": "PRIDE:0000629"}],
    # A recognized PRIDE label whose accession is NOT under PRIDE:0000659 — lets us exercise the
    # parent-ancestry check in isolation (NT and AC agree, so the agreement check stays silent).
    "gel image file uri": [{"label": "Gel image file URI", "obo_id": "PRIDE:0000449"}],
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

_UNDER_PARENT: dict[tuple[str, str], bool] = {
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


def _make_validator(cache_only: bool = False, recommend: bool = True) -> Any:
    from sdrf_pipelines.sdrf.validators import OntologyValidator

    def fake_search(
        term: str, ontology: str | None = None, exact: bool = True, use_ols_cache_only: bool = False, **kwargs: Any
    ) -> list[dict[str, str]]:
        return _LABEL_HITS.get(term.lower(), [])

    def fake_labels_for_accession(accession: str, use_ols_cache_only: bool = False) -> set[str] | None:
        if use_ols_cache_only:
            return None
        return _ACC_LABELS.get(accession.lower())

    def fake_is_under_parent(accession: str, parent_accession: str, use_ols_cache_only: bool = False) -> bool | None:
        if use_ols_cache_only:
            return None
        return _UNDER_PARENT.get((accession.lower(), parent_accession.lower()), False)

    params: dict[str, Any] = {
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


def _errs(value: str, **kwargs: Any) -> list[Any]:
    return _make_validator(**kwargs).validate(
        pd.Series([value]), column_name="comment[proteomics data acquisition method]"
    )


def test_good_nt_ac_passes() -> None:
    assert _errs("NT=data-dependent acquisition;AC=PRIDE:0000627") == []
    assert _errs("NT=data-independent acquisition;AC=PRIDE:0000450") == []
    assert _errs("NT=selected reaction monitoring;AC=PRIDE:0000630") == []


def test_plain_lowercase_passes_with_encoding_warning() -> None:
    errors = _errs("data-dependent acquisition")
    assert _codes(errors) == [ErrorCode.ONTOLOGY_ENCODING_RECOMMENDATION.value]
    assert all(e.error_type == logging.WARNING for e in errors)


def test_title_case_plain_warns_for_encoding_only() -> None:
    # Case is not enforced (OLS label as written): a plain label warns only to recommend NT+AC.
    errors = _errs("Data-dependent acquisition")
    assert _codes(errors) == [ErrorCode.ONTOLOGY_ENCODING_RECOMMENDATION.value]
    assert "lowercase" not in str(errors[0]).lower()


def test_title_case_nt_with_good_ac_passes() -> None:
    # NT= using the OLS label as written + a matching descendant AC= is the recommended form.
    assert _errs("NT=Data-dependent acquisition;AC=PRIDE:0000627") == []


def test_nt_ac_mismatch_is_error() -> None:
    # The label resolves to PRIDE:0000627 but AC= points at an unrelated term -> agreement error.
    errors = _errs("NT=data-dependent acquisition;AC=PRIDE:0000449")
    assert ErrorCode.ONTOLOGY_ACCESSION_MISMATCH.value in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors)


def test_agreeing_accession_not_under_parent_is_error() -> None:
    # NT and AC agree (both PRIDE:0000449 "Gel image file URI") but the accession is not a
    # descendant of PRIDE:0000659 -> the parent-ancestry check must fire on its own.
    errors = _errs("NT=gel image file uri;AC=PRIDE:0000449")
    assert ErrorCode.ONTOLOGY_NOT_UNDER_PARENT.value in _codes(errors)
    assert ErrorCode.ONTOLOGY_ACCESSION_MISMATCH.value not in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors if e.error_code == ErrorCode.ONTOLOGY_NOT_UNDER_PARENT)


def test_foreign_ms_accession_is_error() -> None:
    errors = _errs("NT=data-dependent acquisition;AC=MS:1003221")
    assert ErrorCode.ONTOLOGY_NOT_UNDER_PARENT.value in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors)


def test_foreign_ncit_accession_is_error() -> None:
    errors = _errs("NT=data-independent acquisition;AC=NCIT:C161786")
    assert ErrorCode.ONTOLOGY_NOT_UNDER_PARENT.value in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors)


def test_srm_ms_accession_is_error() -> None:
    errors = _errs("NT=selected reaction monitoring;AC=MS:1000206")
    assert ErrorCode.ONTOLOGY_NOT_UNDER_PARENT.value in _codes(errors)
    assert any(e.error_type == logging.ERROR for e in errors if e.error_code == ErrorCode.ONTOLOGY_NOT_UNDER_PARENT)


def test_unrecognizable_free_text_is_error() -> None:
    errors = _errs("not a real acquisition method")
    assert _codes(errors) == [ErrorCode.ONTOLOGY_TERM_NOT_FOUND.value]
    assert all(e.error_type == logging.ERROR for e in errors)


def test_cache_only_downgrades_parent_check_to_warning() -> None:
    errors = _errs("NT=data-dependent acquisition;AC=MS:1003221", cache_only=True)
    assert ErrorCode.ONTOLOGY_NOT_UNDER_PARENT.value in _codes(errors)
    assert all(e.error_type == logging.WARNING for e in errors if e.error_code == ErrorCode.ONTOLOGY_NOT_UNDER_PARENT)


def test_recommend_flag_can_be_disabled() -> None:
    assert _errs("data-dependent acquisition", recommend=False) == []
