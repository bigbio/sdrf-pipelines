from sdrf_pipelines.ols.ols import OlsClient


def test_ontology():
    ols = OlsClient()
    ontology_list = ols.search("homo sapiens", ontology="NCBITaxon")
    print(ontology_list)
    assert len(ontology_list) > 0
