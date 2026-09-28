from pathlib import Path

from sdrf_pipelines.sdrf.update_validation import UpdateChange, validate_sdrf_update


def _write(path: Path, headers: list[str], rows: list[list[str]]) -> Path:
    path.write_text("\t".join(headers) + "\n" + "\n".join("\t".join(row) for row in rows) + "\n", encoding="utf-8")
    return path


def _changes(base: Path, candidate: Path) -> list[UpdateChange]:
    return [finding.change for finding in validate_sdrf_update(base, candidate)]


def test_cv_rich_value_to_plain_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "comment[dissociation method]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "NT=HCD;AC=PRIDE:0000590"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "HCD"]])

    assert _changes(base, candidate) == [UpdateChange.CONTROLLED_VOCABULARY_INFORMATION_LOSS]


def test_template_removal_is_flagged_by_occurrence(tmp_path: Path):
    headers = [
        "source name",
        "assay name",
        "comment[data file]",
        "comment[sdrf template]",
        "comment[sdrf template]",
    ]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "ms-proteomics", "single-cell"]])
    candidate = _write(tmp_path / "candidate.tsv", headers[:-1], [["s1", "a1", "f1.raw", "ms-proteomics"]])

    findings = validate_sdrf_update(base, candidate)
    assert len(findings) == 1
    assert findings[0].change == UpdateChange.TEMPLATE_CHANGED
    assert findings[0].column is not None
    assert findings[0].column.occurrence == 1


def test_duplicate_template_occurrences_are_compared_independently(tmp_path: Path):
    headers = [
        "source name",
        "assay name",
        "comment[data file]",
        "comment[sdrf template]",
        "comment[sdrf template]",
    ]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "ms-proteomics", "single-cell"]])
    candidate = _write(
        tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "ms-proteomics", "clinical-metadata"]]
    )

    findings = validate_sdrf_update(base, candidate)
    assert len(findings) == 1
    assert findings[0].change == UpdateChange.TEMPLATE_CHANGED
    assert findings[0].column is not None
    assert findings[0].column.occurrence == 1


def test_nonempty_to_reserved_word_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "characteristics[cell type]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "T cell"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "not available"]])

    assert _changes(base, candidate) == [UpdateChange.VALUE_REPLACED_BY_RESERVED_WORD]


def test_internal_controller_token_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "characteristics[sample prep batch]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "batch 1"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "blocked_set"]])

    assert _changes(base, candidate) == [UpdateChange.INTERNAL_VALUE]


def test_ontology_accession_change_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "characteristics[cell type]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "NT=T cell;AC=CL:0000084"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "NT=T cell;AC=CL:0000542"]])

    assert _changes(base, candidate) == [UpdateChange.ONTOLOGY_ACCESSION_CHANGED]


def test_scientific_value_change_is_reported(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "comment[instrument]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "instrument A"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "instrument B"]])

    assert _changes(base, candidate) == [UpdateChange.SCIENTIFIC_VALUE_CHANGED]


def test_file_assay_relationship_change_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a2", "f1.raw"]])

    assert _changes(base, candidate) == [UpdateChange.RELATIONSHIP_CHANGED]


def test_row_reordering_is_not_a_change(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "comment[instrument]"]
    rows = [["s1", "a1", "f1.raw", "A"], ["s2", "a2", "f2.raw", "B"]]
    base = _write(tmp_path / "base.tsv", headers, rows)
    candidate = _write(tmp_path / "candidate.tsv", headers, list(reversed(rows)))

    assert validate_sdrf_update(base, candidate) == []


def test_new_metadata_and_new_rows_are_allowed(tmp_path: Path):
    base_headers = ["source name", "assay name", "comment[data file]", "comment[instrument]"]
    candidate_headers = base_headers + ["characteristics[cell type]"]
    base = _write(tmp_path / "base.tsv", base_headers, [["s1", "a1", "f1.raw", "A"]])
    candidate = _write(
        tmp_path / "candidate.tsv",
        candidate_headers,
        [["s1", "a1", "f1.raw", "A", "T cell"], ["s2", "a2", "f2.raw", "B", "B cell"]],
    )

    assert validate_sdrf_update(base, candidate) == []


def test_empty_value_can_be_enriched(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "characteristics[cell type]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", ""]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "T cell"]])

    assert validate_sdrf_update(base, candidate) == []


def test_nonempty_to_empty_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]", "characteristics[cell type]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "T cell"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", ""]])

    assert _changes(base, candidate) == [UpdateChange.VALUE_CLEARED]


def test_ambiguous_row_identity_fails_closed(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw"], ["s2", "a2", "f1.raw"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw"]])

    assert _changes(base, candidate) == [UpdateChange.AMBIGUOUS_ROW_IDENTITY]


def test_removed_row_is_flagged(tmp_path: Path):
    headers = ["source name", "assay name", "comment[data file]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw"], ["s2", "a2", "f2.raw"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw"]])

    assert _changes(base, candidate) == [UpdateChange.ROW_REMOVED]


def test_cli_writes_machine_readable_diagnostics(tmp_path: Path):
    from click.testing import CliRunner

    from sdrf_pipelines.parse_sdrf import cli

    headers = ["source name", "assay name", "comment[data file]", "comment[instrument]"]
    base = _write(tmp_path / "base.tsv", headers, [["s1", "a1", "f1.raw", "A"]])
    candidate = _write(tmp_path / "candidate.tsv", headers, [["s1", "a1", "f1.raw", "B"]])
    output = tmp_path / "findings.tsv"

    result = CliRunner().invoke(
        cli,
        ["validate-sdrf-update", "--base", str(base), "--candidate", str(candidate), "--out", str(output)],
    )

    assert result.exit_code == 1
    assert "SCIENTIFIC_VALUE_CHANGED" in result.output
    text = output.read_text(encoding="utf-8")
    assert "column_occurrence" in text
    assert "f1.raw" in text
