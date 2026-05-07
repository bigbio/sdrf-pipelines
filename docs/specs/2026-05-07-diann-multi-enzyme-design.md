# Multi-enzyme support in `parse_sdrf convert-diann`

**Issue:** [bigbio/sdrf-pipelines#294](https://github.com/bigbio/sdrf-pipelines/issues/294)
**Status:** Design approved
**Date:** 2026-05-07

## Problem

When an SDRF file declares more than one `comment[cleavage agent details]` column
(e.g., a Lys-C + Trypsin double-digest), `parse_sdrf convert-diann` silently
picks the first column. `diann_config.cfg` ends up with a single-enzyme `--cut`
rule, the predicted spectral library is much smaller than intended, and there
is no warning or error.

Root cause: `_extract_enzyme` in `src/sdrf_pipelines/converters/diann/diann.py`
reads `row["comment[cleavage agent details]"]`. When pandas loads an SDRF with
duplicate column headers it renames them to `.1`, `.2`, etc., so the second
enzyme is silently ignored. The same bug exists in `openms.py` but is **out of
scope** for this spec — see issue #294 follow-up.

## Goal

Detect all enzyme columns in an SDRF row and combine their DIA-NN `--cut`
patterns into one rule. Be tolerant: if one declared enzyme is unknown to our
`ENZYME_SPECIFICITY` table, warn and proceed with the known subset rather than
failing the whole conversion.

## Non-goals

- Fixing the same bug in the OpenMS converter (separate issue/PR).
- Changing the contents of `ENZYME_SPECIFICITY`.
- Adding a CLI flag — multi-enzyme handling is automatic.
- Cross-row mixed-enzyme support beyond the existing
  `if len(enzymes) > 1: raise` guard. Different files must still declare the
  same enzyme set.

## Design

### 1. Column detection

Pandas `read_csv` renames duplicate headers as `name`, `name.1`, `name.2`, ….
Add a helper that finds all enzyme columns:

```python
def _find_enzyme_columns(sdrf: pd.DataFrame) -> list[str]:
    return [
        c for c in sdrf.columns
        if c == "comment[cleavage agent details]"
        or c.startswith("comment[cleavage agent details].")
    ]
```

Mirrors the pattern already used in `maxquant.py:754`. Returns columns in
SDRF column order.

### 2. Per-row enzyme extraction

Replace `_extract_enzyme(row) -> str` with
`_extract_enzymes(row, enzyme_cols) -> tuple[str, ...]`:

- Iterates the provided enzyme columns.
- Skips empty cells, `"nan"`, `"not available"`.
- Parses each `NT=...` value, normalizes via `ENZYME_NAME_MAPPINGS`.
- Deduplicates while preserving order (so `Lys-C` appearing twice collapses to one).
- Returns a tuple of normalized enzyme names.
- Raises `ValueError` only if **zero** enzymes are extracted from a row that
  has at least one enzyme column (preserves existing strictness for the
  single-enzyme case).

### 3. Storage shape

`file_data[raw]["enzyme"]` becomes `tuple[str, ...]` (was `str`). The
cross-file consistency check at `diann.py:82-86` is unchanged in spirit —
`set(tuples)` still detects mismatch:

```python
enzymes_per_file = {fd["enzyme"] for fd in file_data.values()}
if len(enzymes_per_file) > 1:
    raise ValueError(f"Inconsistent enzyme sets across files: {enzymes_per_file}")
enzymes = enzymes_per_file.pop()  # tuple[str, ...]
```

The error message updates to mention "enzyme sets" instead of "enzymes" so
multi-enzyme cases are clear in the message.

### 4. Cut-rule combiner

```python
def _combine_cut_rules(self, enzymes: tuple[str, ...]) -> str | None:
    rules: list[str] = []
    unknown: list[str] = []
    for e in enzymes:
        rule = ENZYME_SPECIFICITY.get(e)
        if rule is None:
            unknown.append(e)
        else:
            rules.append(rule)

    if unknown and rules:
        known = [e for e in enzymes if e not in unknown]
        self.add_warning(
            f"Unknown enzyme(s) {unknown} in multi-enzyme SDRF — no --cut rule "
            f"available for them. Proceeding with known enzymes only: {known}."
        )

    if not rules:
        return None  # all unknown; caller falls back to existing "no --cut" warning

    # Positive (cleavage) tokens UNION across enzymes — anything any enzyme
    # cleaves stays in. Negative (!*X) tokens INTERSECTION across enzymes —
    # a "do not cleave" constraint only survives if EVERY enzyme imposes it.
    # This matters for /P variants: combining Lys-C (K*,!*P) with Trypsin/P
    # (K*,R*) must yield K*,R* (no !*P), because Trypsin/P cleaves K-P / R-P
    # and the user's intent for a double digest is the union of cleavage sites.
    positive_lists: list[list[str]] = []
    negative_sets: list[set[str]] = []
    for rule in rules:
        positives: list[str] = []
        negatives: set[str] = set()
        for tok in (t.strip() for t in rule.split(",")):
            if not tok:
                continue
            if tok.startswith("!"):
                negatives.add(tok)
            elif tok not in positives:
                positives.append(tok)
        positive_lists.append(positives)
        negative_sets.append(negatives)

    merged_positives: list[str] = []
    for pos in positive_lists:
        for tok in pos:
            if tok not in merged_positives:
                merged_positives.append(tok)

    merged_negatives = set.intersection(*negative_sets) if negative_sets else set()

    return ",".join(merged_positives + sorted(merged_negatives))
```

Behavior matrix:

| Declared enzymes              | Output                                 |
| ----------------------------- | -------------------------------------- |
| `(Trypsin,)` (single)         | `--cut K*,R*,!*P` (unchanged path)     |
| `(Lys-C, Trypsin)`            | `--cut K*,R*,!*P` + info warning       |
| `(Trypsin, Foo-ase)` mixed    | `--cut K*,R*,!*P` + unknown warning    |
| `(Foo-ase, Bar-ase)` all unknown | no `--cut` + existing unknown warning |

Token order: positive (cleavage) tokens first in the order each rule
contributes them, negation tokens (`!*P`) appended last (sorted for
determinism).

Combination semantics:

| Enzyme set                                | Combined `--cut` | Reason |
| ----------------------------------------- | ---------------- | ------ |
| Lys-C (`K*,!*P`) + Trypsin (`K*,R*,!*P`)  | `K*,R*,!*P`      | Both forbid K/R-P, intersection keeps `!*P`. |
| Lys-C (`K*,!*P`) + Trypsin/P (`K*,R*`)    | `K*,R*`          | Trypsin/P cleaves K-P / R-P, intersection drops `!*P`. |
| Lys-C/P (`K*`) + Trypsin (`K*,R*,!*P`)    | `K*,R*`          | Lys-C/P cleaves K-P, intersection drops `!*P`. |
| Trypsin (`K*,R*,!*P`) + Asp-N (`*B,*D`)   | `K*,R*,*B,*D`    | Asp-N has no negations; intersection is empty. |

### 5. Wiring it up

- `_write_config(self, enzyme, ...)` becomes
  `_write_config(self, enzymes: tuple[str, ...], ...)`.
- Inside, replace the direct `ENZYME_SPECIFICITY.get(enzyme)` lookup with:
  - if `len(enzymes) == 1`: existing single-enzyme path (preserves the
    "Unknown enzyme '<name>'" warning when missing).
  - else: call `_combine_cut_rules(enzymes)`; if it returns `None`, emit the
    existing "no --cut rule generated" warning.
- `_filemap_row` writes the joined name: `"+".join(fd["enzyme"])`. Single
  enzyme produces an unchanged value.

### 6. Edge cases

- **Empty cell in one of two enzyme columns per row:** treated as single-enzyme
  row (extraction skips empty cells).
- **Same enzyme declared twice:** dedup after normalization, treated as single.
- **One row has both enzymes, another row has only one:** the cross-file
  consistency check fires (same as today's "Multiple enzymes not supported").
- **All declared enzymes unknown:** combiner returns `None`, single existing
  warning is emitted by `_write_config`. Behavior unchanged from today.

## Testing

New tests in `tests/test_convert_diann.py`:

1. `test_multi_enzyme_lys_c_trypsin_combined`
   - SDRF row with two `comment[cleavage agent details]` columns: Lys-C and Trypsin.
   - Asserts `--cut K*,R*,!*P` in cfg.
   - Asserts `Enzyme` column in `diann_design.tsv` is `Lys-C+Trypsin`.
   - Asserts a combination warning was emitted.

2. `test_multi_enzyme_unknown_warns_and_proceeds`
   - Two enzyme columns: Trypsin and a fake unknown enzyme.
   - Asserts `--cut K*,R*,!*P` (Trypsin's rule alone).
   - Asserts an unknown-enzyme warning was emitted naming the fake one.
   - No exception.

3. `test_multi_enzyme_inconsistent_across_files`
   - Row 1 declares (Lys-C, Trypsin); row 2 declares (Trypsin) only.
   - Asserts `ValueError` with the existing inconsistency message.

4. `test_single_enzyme_regression`
   - Existing single-column SDRF still produces same cfg/design output, no
     new warnings.

5. `test_multi_enzyme_same_enzyme_twice`
   - Both columns declare Trypsin.
   - Asserts dedup: `--cut K*,R*,!*P`, `Enzyme` column = `Trypsin` (not `Trypsin+Trypsin`).

6. `test_multi_enzyme_drops_negation_when_one_enzyme_lacks_it`
   - Lys-C (`K*,!*P`) + Trypsin/P (`K*,R*`).
   - Asserts combined `--cut K*,R*` (no `!*P`), because Trypsin/P does not
     forbid cleaving before proline. Guards against a regression where naive
     token union would incorrectly keep `!*P`.

## Files affected

- `src/sdrf_pipelines/converters/diann/diann.py` — primary changes (extraction,
  combiner, `_write_config`, `_filemap_row`, `_extract_file_data`).
- `tests/test_convert_diann.py` — new tests.
- No changes to `constants.py` or other converters.
