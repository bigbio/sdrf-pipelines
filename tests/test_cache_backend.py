"""Tests for the parquet-backed ontology cache (DictBackend)."""

import pandas as pd
import pytest

from sdrf_pipelines.ols.ols import DictBackend


@pytest.fixture
def backend(tmp_path):
    df = pd.DataFrame(
        {
            "accession": ["T:1", "T:2", "T:3", "T:4"],
            "ontology": ["test", "test", "test", "test"],
            "label": ["Homo sapiens", "mus musculus", "homo sapiens", "Study Sample"],
        }
    )
    path = tmp_path / "test.parquet"
    df.to_parquet(path, engine="fastparquet")
    other = pd.DataFrame({"accession": ["O:1"], "ontology": ["other"], "label": ["Homo sapiens"]})
    other_path = tmp_path / "other.parquet"
    other.to_parquet(other_path, engine="fastparquet")
    b = DictBackend()
    b.load_ontology("test", str(path))
    b.load_ontology("other", str(other_path))
    return b


def test_lookup_is_case_insensitive_and_keeps_file_order(backend):
    hits = backend.search("HOMO SAPIENS", "test")
    assert [h["obo_id"] for h in hits] == ["T:1", "T:3"]
    assert hits[0] == {"ontology_name": "test", "label": "Homo sapiens", "obo_id": "T:1"}


def test_unknown_term_and_unknown_ontology(backend):
    assert backend.search("no such term", "test") == []
    assert backend.search("homo sapiens", "missing") == []


def test_search_without_ontology_needs_full_search(backend):
    assert backend.search("homo sapiens", None) == []
    hits = backend.search("homo sapiens", None, full_search=True)
    assert sorted(h["obo_id"] for h in hits) == ["O:1", "T:1", "T:3"]


def test_loaded_state_and_names(backend):
    assert backend.is_ontology_loaded("TEST")
    assert not backend.is_ontology_loaded("nope")
    assert backend.get_ontologies() == ["test", "other"]


def test_loading_twice_keeps_one_table(backend, tmp_path):
    before = backend.search("study sample", "test")
    backend.load_ontology("test", str(tmp_path / "does-not-exist.parquet"))  # already loaded: not read again
    assert backend.search("study sample", "test") == before
