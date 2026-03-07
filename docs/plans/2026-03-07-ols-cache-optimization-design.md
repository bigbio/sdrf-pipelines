# OLS Cache Optimization Design

**Date:** 2026-03-07
**Status:** Proposed

## Problem

The current `cache_search()` in `ols.py` loads all 18 ontology parquet files (~3.3M terms) into a single concatenated DataFrame, then filters with `df["label"].str.lower() == term.lower()` per lookup. This costs **729ms/query**. With 150–1600 lookups per SDRF file, validating 307 files takes ~6 hours.

Additional bottlenecks:
- `get_cache_parquet_files()` loads and concatenates ALL parquets just to extract ontology names
- Every CLI invocation pays the full loading cost even if only a few ontologies are needed
- No query-level deduplication — the same `(term, ontology)` pair is looked up repeatedly across rows

## Decision

Two-tier cache: **Dict index as default** (no new dependencies), **DuckDB as optional** backend for users with large ontology workloads.

## Architecture

```
┌─────────────────────────────────────────────┐
│              CacheBackend (ABC)              │
│  search(term, ontology) → list[dict]        │
│  get_ontologies() → list[str]               │
│  load_ontology(name, parquet_path)          │
├─────────────────────────────────────────────┤
│  ┌──────────────┐    ┌───────────────────┐  │
│  │ DictBackend  │    │  DuckDBBackend    │  │
│  │ (default)    │    │  (optional)       │  │
│  │              │    │                   │  │
│  │ Per-ontology │    │ Per-ontology      │  │
│  │ dict index:  │    │ tables from       │  │
│  │ label→[terms]│    │ parquet files     │  │
│  │              │    │ (lazy loading)    │  │
│  │ 0.01ms/query │    │ 0.3ms/query       │  │
│  └──────────────┘    └───────────────────┘  │
└─────────────────────────────────────────────┘
```

## Components

### 1. CacheBackend ABC

Abstract base class defining the cache interface:

```python
class CacheBackend(ABC):
    @abstractmethod
    def search(self, term: str, ontology: str | None, full_search: bool = False) -> list[dict[str, str]]: ...

    @abstractmethod
    def get_ontologies(self) -> list[str]: ...

    @abstractmethod
    def load_ontology(self, name: str, path: str) -> None: ...
```

### 2. DictBackend (default)

- **No new dependencies** — uses only Python builtins and pandas (already required).
- **Per-ontology lazy loading**: Only reads a parquet when that ontology is first queried.
- **Dict index structure**: `{ontology_lower: {label_lower: [{"ontology_name": str, "label": str, "obo_id": str}]}}`.
- **Build cost**: ~10s per ontology using `itertuples()` — paid once per ontology, only for ontologies actually queried.
- **Query cost**: 0.01ms (dict key lookup).

### 3. DuckDBBackend (optional)

- **Optional dependency**: Mirrors the existing `OLS_AVAILABLE` pattern:
  ```python
  try:
      import duckdb
      DUCKDB_AVAILABLE = True
  except ImportError:
      DUCKDB_AVAILABLE = False
  ```
- **Per-ontology lazy tables**: `CREATE TABLE {ontology} AS SELECT * FROM read_parquet('{path}')` — created on first query for that ontology.
- **Query cost**: 0.3ms (SQL with index).
- **Memory efficient**: DuckDB manages its own columnar storage, doesn't duplicate into Python dicts.
- **Best for**: Users loading many large ontologies (gaz 664K terms, ncbitaxon 2.7M terms).

### 4. Lazy Ontology Name Detection

Replace `get_cache_parquet_files()` which currently loads ALL parquets and concatenates them just to extract ontology names. New approach:
- Parse ontology name from **filename** (e.g., `efo.parquet` → `efo`).
- Store `{ontology_name: parquet_path}` mapping without reading any data.
- Load actual data only when `cache_search` is called for that ontology.

### 5. Column-Level Deduplication

Rather than caching individual query results, dedup happens **before** querying the backend. The validation logic should:

1. For each ontology-validated column, extract the **unique values** from the column.
2. Query the backend once per unique value.
3. Map the results back to all rows sharing that value.

This reduces lookups from `rows × columns` (~150–1600) to `unique_values × columns` (~50–200). Typical SDRF files have high redundancy — 153 unique values from 8370 cells in benchmarks.

**Note:** The validator already does this at `validators.py:412` with `for x in value.unique()`. No additional dedup work needed at the backend level.

### 6. Backend Selection

In `OlsClient.__init__()`:

```python
if use_duckdb and DUCKDB_AVAILABLE:
    self._backend = DuckDBBackend(parquet_files)
elif use_duckdb and not DUCKDB_AVAILABLE:
    logger.warning("duckdb not installed, falling back to dict backend")
    self._backend = DictBackend(parquet_files)
else:
    self._backend = DictBackend(parquet_files)
```

CLI flag: `--use_duckdb` on `validate-sdrf` command.

## Changes Required

| File | Change |
|------|--------|
| `ols.py` | Add `CacheBackend` ABC, `DictBackend`, `DuckDBBackend` classes |
| `ols.py` | Refactor `cache_search` to delegate to backend + add query dedup |
| `ols.py` | Replace `get_cache_parquet_files` with filename-based ontology detection |
| `parse_sdrf.py` | Add `--use_duckdb` CLI flag to `validate-sdrf` |
| `pyproject.toml` | Add `duckdb` to optional dependencies group (e.g., `[project.optional-dependencies] speed = ["duckdb"]`) |

## Expected Performance

| Scenario | Current | Dict (default) | DuckDB (optional) |
|----------|---------|----------------|-------------------|
| First query (cold ontology) | 729ms | ~10s build (once) | ~0.5s table create (once) |
| Subsequent queries | 729ms | 0.01ms | 0.3ms |
| 307 files validation | ~6 hours | ~5–10 min | ~10–15 min |
| Memory (18 ontologies) | ~2GB DataFrame | ~1.5GB dict | ~200MB (DuckDB managed) |

Dict is faster per-query but uses more memory. DuckDB is slightly slower per-query but much more memory efficient — the right choice when loading all 18 ontologies including the large ones.

## Non-Goals

- Replacing the parquet file format (it works well as a distribution format)
- Changing the OLS online API fallback behavior
- Adding a persistent on-disk index (parquets already serve this role)
- Making DuckDB a hard dependency
