import pytest

from sdrf_pipelines.ols.ols import OlsClient

pytestmark = pytest.mark.ontology


def test_ontology():
    ols = OlsClient()
    ontology_list = ols.ols_search("homo sapiens", ontology="ncbitaxon")
    assert len(ontology_list) > 0


def test_ontology_cache():
    ols = OlsClient()
    ontology_list = ols.ols_search(
        "homo sapiens",
        ontology="ncbitaxon",
    )
    assert len(ontology_list) > 0


def test_ontology_from_cache():
    ols = OlsClient()
    ontology_list = ols.cache_search("homo sapiens", ontology="NCBITaxon")
    assert len(ontology_list) > 0
