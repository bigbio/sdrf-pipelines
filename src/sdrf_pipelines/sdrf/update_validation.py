"""Deterministic preservation checks for updates to existing SDRF files."""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from sdrf_pipelines.sdrf.specification import ANONYMIZED, NOT_APPLICABLE, NOT_AVAILABLE, POOLED


class UpdateChange(str, Enum):
    """Machine-readable classes of potentially destructive SDRF updates."""

    AMBIGUOUS_ROW_IDENTITY = "AMBIGUOUS_ROW_IDENTITY"
    ROW_REMOVED = "ROW_REMOVED"
    COLUMN_REMOVED = "COLUMN_REMOVED"
    TEMPLATE_CHANGED = "TEMPLATE_CHANGED"
    VALUE_CLEARED = "VALUE_CLEARED"
    VALUE_REPLACED_BY_RESERVED_WORD = "VALUE_REPLACED_BY_RESERVED_WORD"
    INTERNAL_VALUE = "INTERNAL_VALUE"
    CONTROLLED_VOCABULARY_INFORMATION_LOSS = "CONTROLLED_VOCABULARY_INFORMATION_LOSS"
    ONTOLOGY_ACCESSION_CHANGED = "ONTOLOGY_ACCESSION_CHANGED"
    ONTOLOGY_LABEL_CHANGED = "ONTOLOGY_LABEL_CHANGED"
    RELATIONSHIP_CHANGED = "RELATIONSHIP_CHANGED"
    SCIENTIFIC_VALUE_CHANGED = "SCIENTIFIC_VALUE_CHANGED"


@dataclass(frozen=True)
class ColumnOccurrence:
    """A literal SDRF header plus its zero-based occurrence among equal headers."""

    name: str
    occurrence: int

    def display(self) -> str:
        return f"{self.name}#{self.occurrence + 1}"


@dataclass(frozen=True)
class UpdateFinding:
    """One update that requires explicit review before an SDRF replacement."""

    change: UpdateChange
    row_identity: str | None
    column: ColumnOccurrence | None
    old_value: str | None
    new_value: str | None
    message: str


@dataclass(frozen=True)
class _LosslessSDRF:
    columns: tuple[ColumnOccurrence, ...]
    rows: tuple[tuple[str, ...], ...]


_RESERVED_WORDS = {NOT_AVAILABLE.casefold(), NOT_APPLICABLE.casefold(), POOLED.casefold(), ANONYMIZED.casefold()}
_INTERNAL_TOKENS = {"blocked_set"}
_CV_PART_RE = re.compile(r"(?:^|;)\s*(NT|AC)\s*=\s*([^;]+)", re.IGNORECASE)
_RELATIONSHIP_COLUMNS = {"source name", "assay name", "comment[data file]"}
_TEMPLATE_COLUMN = "comment[sdrf template]"
_IDENTITY_COLUMN = "comment[data file]"


def _normalize_header(value: str) -> str:
    return value.strip().casefold()


def _column_occurrences(headers: list[str]) -> tuple[ColumnOccurrence, ...]:
    counts: defaultdict[str, int] = defaultdict(int)
    result: list[ColumnOccurrence] = []
    for header in headers:
        name = _normalize_header(header)
        result.append(ColumnOccurrence(name=name, occurrence=counts[name]))
        counts[name] += 1
    return tuple(result)


def _read_lossless_sdrf(path: str | Path) -> _LosslessSDRF:
    """Read an SDRF without allowing pandas to rename duplicate headers."""
    data_rows: list[list[str]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader((line for line in handle if line.strip() and not line.startswith("#")), delimiter="\t")
        data_rows = list(reader)

    if not data_rows:
        raise ValueError(f"No valid data found in SDRF: {path}")

    headers = data_rows[0]
    if not headers:
        raise ValueError(f"SDRF has no columns: {path}")
    width = len(headers)
    for row_number, row in enumerate(data_rows[1:], start=2):
        if len(row) != width:
            raise ValueError(f"SDRF row {row_number} has {len(row)} fields; expected {width}: {path}")

    return _LosslessSDRF(columns=_column_occurrences(headers), rows=tuple(tuple(row) for row in data_rows[1:]))


def _column_index(sdrf: _LosslessSDRF, column: ColumnOccurrence) -> int | None:
    try:
        return sdrf.columns.index(column)
    except ValueError:
        return None


def _identity_map(sdrf: _LosslessSDRF) -> tuple[dict[str, tuple[str, ...]] | None, str | None]:
    identity_columns = [col for col in sdrf.columns if col.name == _IDENTITY_COLUMN]
    if len(identity_columns) != 1:
        return None, f"expected exactly one '{_IDENTITY_COLUMN}' column, found {len(identity_columns)}"
    idx = _column_index(sdrf, identity_columns[0])
    assert idx is not None
    identities = [row[idx].strip() for row in sdrf.rows]
    if any(not identity for identity in identities):
        return None, f"'{_IDENTITY_COLUMN}' contains empty values"
    duplicates = sorted(value for value, count in Counter(identities).items() if count > 1)
    if duplicates:
        return None, f"'{_IDENTITY_COLUMN}' is not unique: {', '.join(duplicates[:3])}"
    return dict(zip(identities, sdrf.rows, strict=True)), None


def _cv_parts(value: str) -> dict[str, str]:
    return {match.group(1).upper(): match.group(2).strip() for match in _CV_PART_RE.finditer(value)}


def _classify_change(column: ColumnOccurrence, old: str, new: str) -> UpdateChange:
    old_stripped = old.strip()
    new_stripped = new.strip()
    old_cv = _cv_parts(old_stripped)
    new_cv = _cv_parts(new_stripped)

    if column.name == _TEMPLATE_COLUMN:
        return UpdateChange.TEMPLATE_CHANGED
    if not new_stripped:
        return UpdateChange.VALUE_CLEARED
    if new_stripped.casefold() in _INTERNAL_TOKENS and new_stripped.casefold() != old_stripped.casefold():
        return UpdateChange.INTERNAL_VALUE
    if new_stripped.casefold() in _RESERVED_WORDS and new_stripped.casefold() != old_stripped.casefold():
        return UpdateChange.VALUE_REPLACED_BY_RESERVED_WORD
    if "AC" in old_cv and "AC" not in new_cv:
        return UpdateChange.CONTROLLED_VOCABULARY_INFORMATION_LOSS
    if old_cv.get("AC") and new_cv.get("AC") and old_cv["AC"].casefold() != new_cv["AC"].casefold():
        return UpdateChange.ONTOLOGY_ACCESSION_CHANGED
    if old_cv.get("NT") and new_cv.get("NT") and old_cv["NT"].casefold() != new_cv["NT"].casefold():
        return UpdateChange.ONTOLOGY_LABEL_CHANGED
    if column.name in _RELATIONSHIP_COLUMNS:
        return UpdateChange.RELATIONSHIP_CHANGED
    return UpdateChange.SCIENTIFIC_VALUE_CHANGED


def validate_sdrf_update(base: str | Path, candidate: str | Path) -> list[UpdateFinding]:
    """Compare an existing SDRF with a candidate replacement.

    Additive rows/columns and empty-to-non-empty enrichment are allowed. Existing non-empty
    metadata changes are returned as review-blocking findings. Rows are aligned only by a
    unique, exact ``comment[data file]`` identity; ambiguous identity fails closed.
    """
    base_sdrf = _read_lossless_sdrf(base)
    candidate_sdrf = _read_lossless_sdrf(candidate)
    base_rows, base_identity_error = _identity_map(base_sdrf)
    candidate_rows, candidate_identity_error = _identity_map(candidate_sdrf)

    if base_identity_error or candidate_identity_error:
        details = "; ".join(
            part
            for part in (
                f"base: {base_identity_error}" if base_identity_error else None,
                f"candidate: {candidate_identity_error}" if candidate_identity_error else None,
            )
            if part
        )
        return [
            UpdateFinding(
                change=UpdateChange.AMBIGUOUS_ROW_IDENTITY,
                row_identity=None,
                column=ColumnOccurrence(_IDENTITY_COLUMN, 0),
                old_value=None,
                new_value=None,
                message=f"Cannot deterministically align SDRF rows ({details}).",
            )
        ]

    assert base_rows is not None and candidate_rows is not None
    findings: list[UpdateFinding] = []

    for identity in sorted(set(base_rows) - set(candidate_rows)):
        findings.append(
            UpdateFinding(
                change=UpdateChange.ROW_REMOVED,
                row_identity=identity,
                column=ColumnOccurrence(_IDENTITY_COLUMN, 0),
                old_value=identity,
                new_value=None,
                message="Existing SDRF row/data file is absent from the candidate.",
            )
        )

    candidate_columns = set(candidate_sdrf.columns)
    for column in base_sdrf.columns:
        if column not in candidate_columns:
            findings.append(
                UpdateFinding(
                    change=(
                        UpdateChange.TEMPLATE_CHANGED
                        if column.name == _TEMPLATE_COLUMN
                        else UpdateChange.COLUMN_REMOVED
                    ),
                    row_identity=None,
                    column=column,
                    old_value=None,
                    new_value=None,
                    message="Existing SDRF column occurrence is absent from the candidate.",
                )
            )

    shared_columns = [column for column in base_sdrf.columns if column in candidate_columns]
    base_indices = {column: idx for idx, column in enumerate(base_sdrf.columns)}
    candidate_indices = {column: idx for idx, column in enumerate(candidate_sdrf.columns)}

    for identity in sorted(set(base_rows) & set(candidate_rows)):
        base_row = base_rows[identity]
        candidate_row = candidate_rows[identity]
        for column in shared_columns:
            old = base_row[base_indices[column]]
            new = candidate_row[candidate_indices[column]]
            if old == new or not old.strip():
                continue
            change = _classify_change(column, old, new)
            findings.append(
                UpdateFinding(
                    change=change,
                    row_identity=identity,
                    column=column,
                    old_value=old,
                    new_value=new,
                    message=f"Existing value changed in {column.display()}.",
                )
            )

    return findings
