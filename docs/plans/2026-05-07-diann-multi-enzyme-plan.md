# DIA-NN Multi-Enzyme Implementation Plan

> **For agentic workers:** Implement this plan task-by-task using the executing-plans flow. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `parse_sdrf convert-diann` so that multiple `comment[cleavage agent details]` columns in an SDRF row are detected, combined into a single DIA-NN `--cut` rule (positives unioned, negations intersected), and reflected in `diann_design.tsv`.

**Architecture:** Add a `_find_enzyme_columns` helper to detect duplicate-renamed pandas columns (`comment[cleavage agent details]`, `…].1`, …). Refactor `_extract_enzyme(row) -> str` into `_extract_enzymes(row, cols) -> tuple[str, ...]`. Store `file_data["enzyme"]` as a tuple. Add `_combine_cut_rules(enzymes)` that unions positive cleavage tokens and intersects negation tokens (so `/P` enzymes correctly relax `!*P`). Update `_write_config` to dispatch by enzyme count, and `_filemap_row` to emit `+`-joined names.

**Tech Stack:** Python 3.10+, pandas, pytest. No new dependencies.

**Spec:** [docs/specs/2026-05-07-diann-multi-enzyme-design.md](../specs/2026-05-07-diann-multi-enzyme-design.md)

---

## File Structure

| File | Role |
| --- | --- |
| `src/sdrf_pipelines/converters/diann/diann.py` | Primary implementation. `_extract_enzyme` → `_extract_enzymes`, new `_combine_cut_rules`, signature change `_write_config(enzyme: str → enzymes: tuple[str,...])`, `_filemap_row` joins names. |
| `tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv` | New test fixture: 2 rows × 2 enzyme columns (Lys-C + Trypsin). |
| `tests/data/diann/multi_enzyme_lys_c_trypsin_p.sdrf.tsv` | New test fixture: Lys-C + Trypsin/P (drops `!*P`). |
| `tests/data/diann/multi_enzyme_unknown.sdrf.tsv` | New test fixture: Trypsin + bogus enzyme name. |
| `tests/data/diann/multi_enzyme_inconsistent.sdrf.tsv` | New test fixture: row 1 has both enzymes, row 2 has only Trypsin. |
| `tests/data/diann/multi_enzyme_same.sdrf.tsv` | New test fixture: same enzyme in both columns. |
| `tests/test_convert_diann.py` | New `TestDiannMultiEnzyme` class with 6 tests. |

No file splits or refactors beyond what the change requires.

---

## Task 1: Fixture — Lys-C + Trypsin SDRF

**Files:**
- Create: `tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv`

- [ ] **Step 1: Write the fixture file (TSV with two `comment[cleavage agent details]` columns)**

Use real tabs between columns. Two rows so the per-file consistency check is exercised.

```
source name	characteristics[organism]	assay name	comment[label]	comment[instrument]	comment[cleavage agent details]	comment[cleavage agent details]	comment[modification parameters]	comment[modification parameters]	comment[precursor mass tolerance]	comment[fragment mass tolerance]	comment[data file]
Sample 1	Homo sapiens	run 1	AC=MS:1002038;NT=label free sample	AC=MS:1001742;NT=LTQ Orbitrap Velos	NT=Lys-C;AC=MS:1001309	NT=Trypsin;AC=MS:1001251	NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4	NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35	10 ppm	20 ppm	sample1.raw
Sample 2	Homo sapiens	run 2	AC=MS:1002038;NT=label free sample	AC=MS:1001742;NT=LTQ Orbitrap Velos	NT=Lys-C;AC=MS:1001309	NT=Trypsin;AC=MS:1001251	NT=Carbamidomethyl;TA=C;MT=fixed;AC=UNIMOD:4	NT=Oxidation;MT=variable;TA=M;AC=UNIMOD:35	5 ppm	15 ppm	sample2.raw
```

To produce reliable tabs, use `printf '...\t...\n'` or copy from `label_free.sdrf.tsv` and edit. Easiest:

```bash
cp tests/data/diann/label_free.sdrf.tsv tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv
```

Then in an editor add a second `comment[cleavage agent details]` column (with `NT=Trypsin;AC=MS:1001251` values) immediately after the existing one, and change the existing column's values from `NT=Trypsin/P;AC=MS:1001313` to `NT=Lys-C;AC=MS:1001309`.

- [ ] **Step 2: Verify the fixture has two enzyme columns**

```bash
head -1 tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv | tr '\t' '\n' | grep -c "comment\[cleavage agent details\]"
```
Expected: `2`

- [ ] **Step 3: Verify pandas parses with renamed duplicate**

```bash
python -c "import pandas as pd; df = pd.read_csv('tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv', sep='\t'); print([c for c in df.columns if 'cleavage' in c])"
```
Expected: `['comment[cleavage agent details]', 'comment[cleavage agent details].1']`

- [ ] **Step 4: Commit**

```bash
git add tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv
git commit -m "test: add Lys-C + Trypsin double-digest SDRF fixture"
```

---

## Task 2: Fixture — Lys-C + Trypsin/P SDRF

**Files:**
- Create: `tests/data/diann/multi_enzyme_lys_c_trypsin_p.sdrf.tsv`

This fixture exercises the negation-intersection rule: Trypsin/P does not have `!*P`, so the combined `--cut` must drop `!*P`.

- [ ] **Step 1: Copy and edit**

```bash
cp tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv tests/data/diann/multi_enzyme_lys_c_trypsin_p.sdrf.tsv
```

In an editor change every `NT=Trypsin;AC=MS:1001251` cell to `NT=Trypsin/P;AC=MS:1001313`. Lys-C values stay the same.

- [ ] **Step 2: Verify**

```bash
grep -c "Trypsin/P" tests/data/diann/multi_enzyme_lys_c_trypsin_p.sdrf.tsv
```
Expected: `2` (one per data row).

- [ ] **Step 3: Commit**

```bash
git add tests/data/diann/multi_enzyme_lys_c_trypsin_p.sdrf.tsv
git commit -m "test: add Lys-C + Trypsin/P SDRF fixture"
```

---

## Task 3: Fixture — Unknown enzyme alongside known

**Files:**
- Create: `tests/data/diann/multi_enzyme_unknown.sdrf.tsv`

- [ ] **Step 1: Copy and edit**

```bash
cp tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv tests/data/diann/multi_enzyme_unknown.sdrf.tsv
```

In an editor change every `NT=Lys-C;AC=MS:1001309` cell to `NT=BogusProtease;AC=MS:9999999`. Trypsin column unchanged.

- [ ] **Step 2: Verify**

```bash
grep -c "BogusProtease" tests/data/diann/multi_enzyme_unknown.sdrf.tsv
```
Expected: `2`

- [ ] **Step 3: Commit**

```bash
git add tests/data/diann/multi_enzyme_unknown.sdrf.tsv
git commit -m "test: add unknown-enzyme + Trypsin SDRF fixture"
```

---

## Task 4: Fixture — Inconsistent enzyme set across files

**Files:**
- Create: `tests/data/diann/multi_enzyme_inconsistent.sdrf.tsv`

- [ ] **Step 1: Copy and edit**

```bash
cp tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv tests/data/diann/multi_enzyme_inconsistent.sdrf.tsv
```

In an editor, on the second data row only, change the Lys-C cell (first enzyme column) from `NT=Lys-C;AC=MS:1001309` to an empty string. Result: row 1 has (Lys-C, Trypsin); row 2 has (Trypsin) only — different enzyme tuples.

- [ ] **Step 2: Verify**

```bash
awk -F'\t' 'NR>1 {print NR": col6=["$6"] col7=["$7"]"}' tests/data/diann/multi_enzyme_inconsistent.sdrf.tsv
```
Expected: row 2 line shows `col6=[]`, row 3 (sample2) shows `col6=[]` only on the right row. Adjust column indices to match the actual fixture columns; the goal is one row with two enzymes, the other with one.

- [ ] **Step 3: Commit**

```bash
git add tests/data/diann/multi_enzyme_inconsistent.sdrf.tsv
git commit -m "test: add inconsistent-enzyme-set SDRF fixture"
```

---

## Task 5: Fixture — Same enzyme declared twice

**Files:**
- Create: `tests/data/diann/multi_enzyme_same.sdrf.tsv`

- [ ] **Step 1: Copy and edit**

```bash
cp tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv tests/data/diann/multi_enzyme_same.sdrf.tsv
```

In an editor change every Lys-C cell to also be `NT=Trypsin;AC=MS:1001251`. Both enzyme columns now declare Trypsin.

- [ ] **Step 2: Verify**

```bash
awk -F'\t' 'NR>1 {print $0}' tests/data/diann/multi_enzyme_same.sdrf.tsv | grep -c "Trypsin.*Trypsin"
```
Expected: `2` (both data rows).

- [ ] **Step 3: Commit**

```bash
git add tests/data/diann/multi_enzyme_same.sdrf.tsv
git commit -m "test: add duplicate-enzyme SDRF fixture"
```

---

## Task 6: Failing test — combined Lys-C + Trypsin

**Files:**
- Modify: `tests/test_convert_diann.py` (append a new class at end)

- [ ] **Step 1: Append the new test class**

```python
class TestDiannMultiEnzyme:
    def test_lys_c_trypsin_combined_cut_rule(self, diann_data_dir, on_tmpdir):
        sdrf_file = str(diann_data_dir / "multi_enzyme_lys_c_trypsin.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config = (on_tmpdir / "diann_config.cfg").read_text()
        assert "--cut K*,R*,!*P" in config

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        assert all(df["Enzyme"] == "Lys-C+Trypsin")
```

- [ ] **Step 2: Run and verify failure**

```bash
pytest tests/test_convert_diann.py::TestDiannMultiEnzyme::test_lys_c_trypsin_combined_cut_rule -v
```
Expected: FAIL. The current code reads only the first enzyme column, so `--cut` is `K*,!*P` (Lys-C only) and `Enzyme` is just `Lys-C`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_convert_diann.py
git commit -m "test: add failing multi-enzyme combination test (#294)"
```

---

## Task 7: Add column-detection helper

**Files:**
- Modify: `src/sdrf_pipelines/converters/diann/diann.py` (add helper near top of `DiaNN` class, before `_extract_file_data`)

- [ ] **Step 1: Add helper method**

Insert this method into the `DiaNN` class, right above `_extract_file_data` (around line 142):

```python
    @staticmethod
    def _find_enzyme_columns(sdrf: pd.DataFrame) -> list[str]:
        """Return all `comment[cleavage agent details]` columns, including
        pandas-renamed duplicates (e.g. `…].1`, `…].2`).
        """
        return [
            c for c in sdrf.columns
            if c == "comment[cleavage agent details]"
            or c.startswith("comment[cleavage agent details].")
        ]
```

- [ ] **Step 2: Verify import / syntax with a smoke test**

```bash
python -c "from sdrf_pipelines.converters.diann.diann import DiaNN; import pandas as pd; df = pd.read_csv('tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv', sep='\t'); print(DiaNN._find_enzyme_columns(df))"
```
Expected: `['comment[cleavage agent details]', 'comment[cleavage agent details].1']`

- [ ] **Step 3: Commit**

```bash
git add src/sdrf_pipelines/converters/diann/diann.py
git commit -m "feat(diann): add _find_enzyme_columns helper for duplicate column detection"
```

---

## Task 8: Refactor `_extract_enzyme` to `_extract_enzymes`

**Files:**
- Modify: `src/sdrf_pipelines/converters/diann/diann.py:333-347`

- [ ] **Step 1: Replace the method**

Replace the entire `_extract_enzyme` method (currently at lines 333-347) with:

```python
    def _extract_enzymes(self, row: pd.Series, enzyme_cols: list[str]) -> tuple[str, ...]:
        """Extract all declared enzymes for a row, in column order, deduplicated.

        Skips empty / "not available" cells. Normalizes via ENZYME_NAME_MAPPINGS.
        Returns a tuple of normalized enzyme names. Empty tuple if no cleavage
        agent column is provided; raises ValueError if columns exist but every
        cell is empty (preserves the prior single-column strictness).
        """
        if not enzyme_cols:
            raise ValueError("Missing comment[cleavage agent details] column")

        names: list[str] = []
        for col in enzyme_cols:
            raw_val = str(row.get(col, "")).strip()
            if not raw_val or raw_val.lower() in ("nan", "not available"):
                continue
            nt_match = re.search(r"NT=(.+?)(;|$)", raw_val)
            enzyme_name = nt_match.group(1).strip() if nt_match else raw_val
            normalized = ENZYME_NAME_MAPPINGS.get(enzyme_name.lower(), enzyme_name)
            if normalized not in names:
                names.append(normalized)

        if not names:
            raise ValueError("Row has no usable cleavage agent value")

        return tuple(names)
```

- [ ] **Step 2: Run a syntax/lint sanity check**

```bash
python -c "from sdrf_pipelines.converters.diann.diann import DiaNN"
```
Expected: no error.

- [ ] **Step 3: Commit**

```bash
git add src/sdrf_pipelines/converters/diann/diann.py
git commit -m "refactor(diann): _extract_enzyme returns tuple of names"
```

---

## Task 9: Update `_extract_file_data` to use the new tuple

**Files:**
- Modify: `src/sdrf_pipelines/converters/diann/diann.py:142-219` (within `_extract_file_data`)

- [ ] **Step 1: Find the enzyme columns once and pass them down**

In `_extract_file_data`, just below the existing `mod_cols = ...` line (around line 152), add:

```python
        enzyme_cols = self._find_enzyme_columns(sdrf)
```

- [ ] **Step 2: Initialize `enzyme` field as None still, but typed as tuple**

The line `"enzyme": None,` (around line 162) does not need to change at the dict-literal level, but the assignment site does.

Replace the block (around lines 183-185):

```python
            # Enzyme (first row wins)
            if fd["enzyme"] is None:
                fd["enzyme"] = self._extract_enzyme(row)
```

with:

```python
            # Enzymes (first row wins). May be a tuple of multiple enzymes
            # when the SDRF declares more than one cleavage-agent column.
            if fd["enzyme"] is None:
                fd["enzyme"] = self._extract_enzymes(row, enzyme_cols)
```

- [ ] **Step 3: Smoke-test the extractor**

```bash
python -c "
from sdrf_pipelines.converters.diann.diann import DiaNN
import pandas as pd
df = pd.read_csv('tests/data/diann/multi_enzyme_lys_c_trypsin.sdrf.tsv', sep='\t')
fd = DiaNN()._extract_file_data(df)
for f, d in fd.items():
    print(f, d['enzyme'])
"
```
Expected: each line ends with `('Lys-C', 'Trypsin')`.

- [ ] **Step 4: Commit**

```bash
git add src/sdrf_pipelines/converters/diann/diann.py
git commit -m "refactor(diann): plumb enzyme tuple through _extract_file_data"
```

---

## Task 10: Update cross-file consistency check and `_write_config` signature

**Files:**
- Modify: `src/sdrf_pipelines/converters/diann/diann.py:82-86` (the consistency check) and `:584-586` (`_write_config` signature) and `:133-135` (call site)

- [ ] **Step 1: Update the cross-file check (around lines 82-86)**

Replace:

```python
        # Get enzyme (must be consistent across experiment)
        enzymes = {fd["enzyme"] for fd in file_data.values()}
        if len(enzymes) > 1:
            raise ValueError(f"Multiple enzymes not supported: {enzymes}")
        enzyme = enzymes.pop()
```

with:

```python
        # Enzyme set (tuple of normalized names) must be consistent across files.
        enzyme_sets = {fd["enzyme"] for fd in file_data.values()}
        if len(enzyme_sets) > 1:
            raise ValueError(f"Inconsistent enzyme sets across files: {enzyme_sets}")
        enzymes = enzyme_sets.pop()  # tuple[str, ...]
```

- [ ] **Step 2: Update the call to `_write_config` (around lines 133-135)**

Replace:

```python
        # Write config file
        self._write_config(
            enzyme, diann_fixed, diann_var, plex_info, tolerance_summary, scan_range_summary, monitor_mods
        )
```

with:

```python
        # Write config file
        self._write_config(
            enzymes, diann_fixed, diann_var, plex_info, tolerance_summary, scan_range_summary, monitor_mods
        )
```

- [ ] **Step 3: Update the `_write_config` signature (around lines 584-586)**

Replace:

```python
    def _write_config(
        self,
        enzyme: str,
```

with:

```python
    def _write_config(
        self,
        enzymes: tuple[str, ...],
```

- [ ] **Step 4: Run the existing test suite to confirm we haven't broken single-enzyme cases yet (the cut-rule body still uses old `enzyme` variable — expect failures here, that's fine)**

```bash
pytest tests/test_convert_diann.py -x -q 2>&1 | head -30
```
Expected: failures referencing `enzyme` being undefined or wrong type. We fix that in Task 11.

- [ ] **Step 5: Commit (WIP — broken state, fixed in next task)**

```bash
git add src/sdrf_pipelines/converters/diann/diann.py
git commit -m "refactor(diann): change _write_config signature to enzyme tuple (WIP)"
```

---

## Task 11: Implement `_combine_cut_rules` and rewire `_write_config`

**Files:**
- Modify: `src/sdrf_pipelines/converters/diann/diann.py` (add `_combine_cut_rules` near `_write_config`, rewrite the cut-rule emission block)

- [ ] **Step 1: Add the combiner method just above `_write_config`**

Insert in the `DiaNN` class (just before `def _write_config`):

```python
    def _combine_cut_rules(self, enzymes: tuple[str, ...]) -> str | None:
        """Combine DIA-NN --cut rules across multiple enzymes.

        - Positives (cleavage tokens) are unioned across enzymes (first-seen order).
        - Negations (e.g. !*P) are intersected: a "do not cleave" constraint
          only survives if EVERY contributing enzyme imposes it. This makes
          /P variants (which lack !*P) correctly relax the proline restriction.
        - Unknown enzymes (not in ENZYME_SPECIFICITY) are warned about and
          skipped. Returns None if every enzyme is unknown.
        """
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
            return None

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

- [ ] **Step 2: Rewrite the cut-rule emission block in `_write_config`**

Find the existing block (around lines 597-602):

```python
        # Enzyme cut rule
        cut_rule = ENZYME_SPECIFICITY.get(enzyme)
        if cut_rule:
            parts.append(f"--cut {cut_rule}")
        else:
            self.add_warning(f"Unknown enzyme '{enzyme}', no --cut rule generated")
```

Replace with:

```python
        # Enzyme cut rule. Single-enzyme path preserves the existing
        # "Unknown enzyme" warning; multi-enzyme path delegates to combiner.
        if len(enzymes) == 1:
            single = enzymes[0]
            cut_rule = ENZYME_SPECIFICITY.get(single)
            if cut_rule:
                parts.append(f"--cut {cut_rule}")
            else:
                self.add_warning(f"Unknown enzyme '{single}', no --cut rule generated")
        else:
            combined = self._combine_cut_rules(enzymes)
            if combined:
                parts.append(f"--cut {combined}")
                self.add_warning(
                    f"Combined {len(enzymes)} cleavage agents {list(enzymes)} into --cut {combined}"
                )
            else:
                self.add_warning(
                    f"All enzymes {list(enzymes)} unknown, no --cut rule generated"
                )
```

- [ ] **Step 3: Run the existing test suite**

```bash
pytest tests/test_convert_diann.py -x -q 2>&1 | tail -20
```
Expected: all single-enzyme regression tests pass. The new multi-enzyme test from Task 6 now passes too.

- [ ] **Step 4: Commit**

```bash
git add src/sdrf_pipelines/converters/diann/diann.py
git commit -m "feat(diann): combine multi-enzyme --cut rules (positives union, negations intersect)"
```

---

## Task 12: Update `_filemap_row` to emit joined enzyme name

**Files:**
- Modify: `src/sdrf_pipelines/converters/diann/diann.py:696-712` (`_filemap_row`)

- [ ] **Step 1: Locate the `Enzyme` field in `_filemap_row`**

The existing line near 710 is `"Enzyme": fd["enzyme"],`. Since `fd["enzyme"]` is now a tuple, replace:

```python
            "Enzyme": fd["enzyme"],
```

with:

```python
            "Enzyme": "+".join(fd["enzyme"]),
```

- [ ] **Step 2: Run the multi-enzyme test added in Task 6**

```bash
pytest tests/test_convert_diann.py::TestDiannMultiEnzyme::test_lys_c_trypsin_combined_cut_rule -v
```
Expected: PASS — both `--cut K*,R*,!*P` and `Enzyme == "Lys-C+Trypsin"` assertions hold.

- [ ] **Step 3: Run the full suite**

```bash
pytest tests/test_convert_diann.py -q
```
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add src/sdrf_pipelines/converters/diann/diann.py
git commit -m "feat(diann): join multi-enzyme names with '+' in design.tsv"
```

---

## Task 13: Add Lys-C + Trypsin/P regression test

**Files:**
- Modify: `tests/test_convert_diann.py` (extend `TestDiannMultiEnzyme`)

- [ ] **Step 1: Append the test inside `TestDiannMultiEnzyme`**

```python
    def test_lys_c_trypsin_p_drops_negation(self, diann_data_dir, on_tmpdir):
        """Trypsin/P has no !*P; intersection must drop the negation."""
        sdrf_file = str(diann_data_dir / "multi_enzyme_lys_c_trypsin_p.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config = (on_tmpdir / "diann_config.cfg").read_text()
        assert "--cut K*,R*" in config
        assert "!*P" not in config.split("--cut ", 1)[1].split(" ", 1)[0]

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        assert all(df["Enzyme"] == "Lys-C+Trypsin/P")
```

- [ ] **Step 2: Run**

```bash
pytest tests/test_convert_diann.py::TestDiannMultiEnzyme::test_lys_c_trypsin_p_drops_negation -v
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_convert_diann.py
git commit -m "test(diann): regression for /P variant intersection rule"
```

---

## Task 14: Add unknown-enzyme warning test

**Files:**
- Modify: `tests/test_convert_diann.py` (extend `TestDiannMultiEnzyme`)

- [ ] **Step 1: Append the test**

```python
    def test_unknown_enzyme_warns_and_proceeds(self, diann_data_dir, on_tmpdir):
        """Unknown enzyme alongside a known one: warn, drop unknown, keep going."""
        sdrf_file = str(diann_data_dir / "multi_enzyme_unknown.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config = (on_tmpdir / "diann_config.cfg").read_text()
        # Trypsin's rule survives intact
        assert "--cut K*,R*,!*P" in config

        # A warning naming the unknown enzyme was emitted
        assert any("BogusProtease" in msg for msg in converter.warnings)

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        # Both names are still recorded in the design TSV
        assert all(df["Enzyme"] == "BogusProtease+Trypsin")
```

- [ ] **Step 2: Run**

```bash
pytest tests/test_convert_diann.py::TestDiannMultiEnzyme::test_unknown_enzyme_warns_and_proceeds -v
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_convert_diann.py
git commit -m "test(diann): unknown-enzyme path warns and proceeds"
```

---

## Task 15: Add inconsistent-across-files test

**Files:**
- Modify: `tests/test_convert_diann.py`

- [ ] **Step 1: Append the test**

```python
    def test_inconsistent_enzyme_sets_raises(self, diann_data_dir, on_tmpdir):
        """Different enzyme tuples per file → ValueError."""
        sdrf_file = str(diann_data_dir / "multi_enzyme_inconsistent.sdrf.tsv")
        converter = DiaNN()
        with pytest.raises(ValueError, match="Inconsistent enzyme sets"):
            converter.diann_convert(sdrf_file)
```

- [ ] **Step 2: Run**

```bash
pytest tests/test_convert_diann.py::TestDiannMultiEnzyme::test_inconsistent_enzyme_sets_raises -v
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_convert_diann.py
git commit -m "test(diann): inconsistent enzyme sets across files raises"
```

---

## Task 16: Add same-enzyme-twice dedup test

**Files:**
- Modify: `tests/test_convert_diann.py`

- [ ] **Step 1: Append the test**

```python
    def test_same_enzyme_twice_dedups(self, diann_data_dir, on_tmpdir):
        """Two columns declaring Trypsin must collapse to a single-enzyme run."""
        sdrf_file = str(diann_data_dir / "multi_enzyme_same.sdrf.tsv")
        converter = DiaNN()
        converter.diann_convert(sdrf_file)

        config = (on_tmpdir / "diann_config.cfg").read_text()
        assert "--cut K*,R*,!*P" in config

        df = pd.read_csv(on_tmpdir / "diann_design.tsv", sep="\t")
        # No "+" — single enzyme name only
        assert all(df["Enzyme"] == "Trypsin")
```

- [ ] **Step 2: Run**

```bash
pytest tests/test_convert_diann.py::TestDiannMultiEnzyme::test_same_enzyme_twice_dedups -v
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_convert_diann.py
git commit -m "test(diann): duplicate-enzyme columns dedup to single run"
```

---

## Task 17: Final regression run + lint

**Files:** none modified.

- [ ] **Step 1: Run the full DIA-NN test module**

```bash
pytest tests/test_convert_diann.py -v
```
Expected: every test PASS, including the 5 new ones in `TestDiannMultiEnzyme`.

- [ ] **Step 2: Run the broader test suite (sanity)**

```bash
pytest tests/ -q --ignore=tests/test_validate_sdrf.py 2>&1 | tail -10
```
Expected: no new failures attributable to this change.

- [ ] **Step 3: Run ruff/lint if configured**

```bash
ruff check src/sdrf_pipelines/converters/diann/diann.py tests/test_convert_diann.py 2>/dev/null || python -m ruff check src/sdrf_pipelines/converters/diann/diann.py tests/test_convert_diann.py 2>/dev/null || echo "ruff not available, skipping"
```

- [ ] **Step 4: No commit needed (verification step)** — but if any lint fixes were made, commit them as `chore: lint fixes`.

---

## Self-Review Notes

- Spec coverage: column detection (Task 7), per-row tuple extraction (Task 8), file_data plumbing (Task 9), consistency check + signature (Task 10), combiner with intersection semantics (Task 11), design TSV name join (Task 12), and 5 of the 5 spec tests (Tasks 6, 13, 14, 15, 16) — single-enzyme regression covered by the existing test suite re-run in Task 17.
- No placeholders. Every step shows the exact code or command.
- Type consistency: `_extract_enzymes` returns `tuple[str, ...]`; `file_data["enzyme"]` is `tuple[str, ...]` from Task 9 onward; `_write_config` signature uses `tuple[str, ...]` (Task 10); `_combine_cut_rules` returns `str | None` (Task 11). `_filemap_row` consumes the tuple via `"+".join` (Task 12). Callers use the consistent `enzymes` plural variable name from Task 10 onward.
