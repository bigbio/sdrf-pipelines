"""Tests for issue #370 - opt-in strict handling of terms that are not found in the column's ontologies."""

import logging

import pandas as pd
import pytest
from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli
from sdrf_pipelines.utils.error_codes import ErrorCode

pytestmark = pytest.mark.ontology

MADE_UP_ORGANISM = "glorpus imaginarius"

_COLUMNS = [
    "source name",
    "characteristics[organism]",
    "characteristics[organism part]",
    "characteristics[disease]",
    "characteristics[cell type]",
    "characteristics[biological replicate]",
    "assay name",
    "technology type",
    "comment[technical replicate]",
    "comment[fraction identifier]",
    "comment[label]",
    "comment[instrument]",
    "comment[cleavage agent details]",
    "comment[proteomics data acquisition method]",
    "comment[data file]",
]


def _row(i: int, organism: str) -> list[str]:
    return [
        f"sample {i}",
        organism,
        "liver",
        "normal",
        "hepatocyte",
        str(i),
        f"run {i}",
        "proteomic profiling by mass spectrometry",
        "1",
        "1",
        "NT=label free sample;AC=MS:1002038",
        "NT=Q Exactive;AC=MS:1001911",
        "NT=Trypsin;AC=MS:1001251",
        "NT=Data-Dependent Acquisition;AC=NCIT:C161785",
        f"run_{i}.raw",
    ]


def _write_sdrf(tmp_path, organism: str):
    path = tmp_path / "synthetic.sdrf.tsv"
    lines = ["\t".join(_COLUMNS)] + ["\t".join(_row(i, organism)) for i in (1, 2)]
    path.write_text("\n".join(lines) + "\n")
    return path


def _validate_cli(sdrf_file, *extra: str):
    runner = CliRunner()
    return runner.invoke(
        cli,
        ["validate-sdrf", "--sdrf_file", str(sdrf_file), "--use_ols_cache_only", *extra],
        catch_exceptions=False,
    )


def _validate_api(organism: str, strict_ontology: bool):
    from sdrf_pipelines.sdrf.schemas import SchemaRegistry, SchemaValidator
    from sdrf_pipelines.sdrf.sdrf import SDRFDataFrame

    df = pd.DataFrame([_row(i, organism) for i in (1, 2)], columns=_COLUMNS)
    validator = SchemaValidator(SchemaRegistry())
    return validator.validate(
        SDRFDataFrame(df), "ms-proteomics", use_ols_cache_only=True, strict_ontology=strict_ontology
    )


def _not_found(errors):
    return [e for e in errors if e.error_code == ErrorCode.ONTOLOGY_TERM_NOT_FOUND]


def test_unknown_organism_is_a_warning_by_default():
    """Default behaviour is unchanged: offline, a miss is an unverified warning."""
    misses = _not_found(_validate_api(MADE_UP_ORGANISM, strict_ontology=False))
    assert misses
    assert all(e.error_type == logging.WARNING for e in misses)
    assert all(e.context.get("unverified") for e in misses)


def test_unknown_organism_is_an_error_when_strict():
    errors = _validate_api(MADE_UP_ORGANISM, strict_ontology=True)
    misses = _not_found(errors)
    assert misses
    assert all(e.error_type == logging.ERROR for e in misses)
    assert {e.column for e in misses} == {"characteristics[organism]"}


def test_strict_does_not_turn_unverifiable_accession_checks_into_errors():
    """Lookups that could not be done offline stay warnings, also in strict mode."""
    errors = _validate_api("homo sapiens", strict_ontology=True)
    assert not [e for e in errors if e.error_type == logging.ERROR]
    unverified = [e for e in errors if e.error_code == ErrorCode.ONTOLOGY_ACCESSION_MISMATCH]
    assert unverified
    assert all(e.error_type == logging.WARNING and e.context.get("unverified") for e in unverified)


def test_cli_default_exits_zero_but_does_not_claim_everything_is_fine(tmp_path):
    result = _validate_cli(_write_sdrf(tmp_path, MADE_UP_ORGANISM))
    assert result.exit_code == 0, result.output
    assert "could not be verified without a live ontology lookup" in result.output
    assert "Most seems to be fine" not in result.output


def test_cli_strict_ontology_fails_on_unknown_organism(tmp_path):
    result = _validate_cli(_write_sdrf(tmp_path, MADE_UP_ORGANISM), "--strict-ontology")
    assert result.exit_code == 1, result.output
    assert "There were validation errors." in result.output


def test_cli_strict_ontology_passes_a_known_organism(tmp_path):
    result = _validate_cli(_write_sdrf(tmp_path, "homo sapiens"), "--strict-ontology")
    assert result.exit_code == 0, result.output
