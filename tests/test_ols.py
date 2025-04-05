import pytest
from sdrf_pipelines.ols.ols import OlsClient, get_obo_accession, read_obo_file, read_owl_file
from rdflib import Graph, URIRef, Literal, RDF, RDFS, OWL
import pandas as pd


class TestOlsClientReal:
    @pytest.fixture(scope="class")
    def ols_client(self):
        """Fixture to create an OlsClient instance for real tests."""
        return OlsClient(use_cache=False)

    def test_get_term_efo(self, ols_client):
        """Test get_term method with a real EFO term."""
        result = ols_client.get_term("efo", "http://www.ebi.ac.uk/efo/EFO_0000408")
        assert result["label"] == "disease"
        assert result["ontology_name"] == "efo"

    def test_get_term_go(self, ols_client):
        """Test get_term method with a real GO term."""
        result = ols_client.get_term("go", "http://purl.obolibrary.org/obo/GO_0008150")
        assert result["label"] == "biological_process"
        assert result["ontology_name"] == "go"

    def test_get_ancestors_efo(self, ols_client):
        """Test get_ancestors method with a real EFO term."""
        result = ols_client.get_ancestors("efo", "http://www.ebi.ac.uk/efo/EFO_0000408")
        assert len(result) > 0
        assert any(term["obo_id"] == "BFO:0000016" for term in result)
        assert any(term["label"] == "material property" for term in result)

    def test_get_ancestors_empty(self, ols_client):
        """Test get_ancestors method with a term that has no ancestors."""
        with pytest.raises(KeyError):
            ols_client.get_ancestors("efo", "http://www.ebi.ac.uk/efo/EFO_0000001")

    def test_search_efo(self, ols_client):
        """Test search method with a real EFO term."""
        result = ols_client.search("disease", ontology="efo")
        assert len(result) > 0
        assert any(term["obo_id"] == "EFO:0000408" for term in result)
        assert any(term["label"] == "disease" for term in result)

    def test_search_go(self, ols_client):
        """Test search method with a real GO term."""
        result = ols_client.search("biological_process", ontology="go")
        assert len(result) > 0
        assert any(term["obo_id"] == "GO:0008150" for term in result)
        assert any(term["label"] == "biological_process" for term in result)

    def test_search_empty(self, ols_client):
        """Test search method with empty response."""
        result = ols_client.search("nonexistent term")
        assert result == []

    def test_search_exact(self, ols_client):
        """Test search method with exact match."""
        result = ols_client.search("disease", ontology="efo", exact=True)
        assert len(result) > 0
        assert any(term["obo_id"] == "EFO:0000408" for term in result)
        assert any(term["label"] == "disease" for term in result)

    def test_ols_search_efo(self, ols_client):
        """Test ols_search method with a real EFO term."""
        result = ols_client.ols_search("disease", ontology="efo")
        assert len(result) > 0
        assert any(term["obo_id"] == "EFO:0000408" for term in result)
        assert any(term["label"] == "disease" for term in result)

    def test_ols_search_go(self, ols_client):
        """Test ols_search method with a real GO term."""
        result = ols_client.ols_search("biological_process", ontology="go")
        assert len(result) > 0
        assert any(term["obo_id"] == "GO:0008150" for term in result)
        assert any(term["label"] == "biological_process" for term in result)

    def test_ols_search_empty(self, ols_client):
        """Test ols_search method with empty response."""
        result = ols_client.ols_search("nonexistent term", exact=True)
        assert result == []

    def test_besthit_efo(self, ols_client):
        """Test besthit method with a real EFO term."""
        result = ols_client.besthit("disease", ontology="efo")
        assert result["obo_id"] == "EFO:0000408"
        assert result["label"] == "disease"

    def test_besthit_go(self, ols_client):
        """Test besthit method with a real GO term."""
        result = ols_client.besthit("biological_process", ontology="go")
        assert result["obo_id"] == "GO:0008150"
        assert result["label"] == "biological_process"

    def test_besthit_empty(self, ols_client):
        """Test besthit method with empty response."""
        result = ols_client.besthit("nonexistent term")
        assert result is None

    def test_get_obo_accession(self):
        """Test get_obo_accession function."""
        assert get_obo_accession("http://www.ebi.ac.uk/efo/EFO_0000001") == "EFO:0000001"
        assert get_obo_accession("http://purl.obolibrary.org/obo/GO_0000001") == "GO:0000001"
        assert get_obo_accession("http://purl.obolibrary.org/obo/GO#0000001") == "GO:0000001"

    def test_read_obo_file(self, tmp_path):
        """Test read_obo_file function."""
        obo_content = """
        format-version: 1.2
        ontology: test_ontology

        [Term]
        id: TEST:0000001
        name: test term 1

        [Term]
        id: TEST:0000002
        name: test term 2
        """
        obo_file = tmp_path / "test.obo"
        obo_file.write_text(obo_content)
        result = read_obo_file(str(obo_file), "test_ontology")
        assert len(result) == 2
        assert result[0]["accession"] == "TEST:0000001"
        assert result[0]["label"] == "test term 1"
        assert result[0]["ontology"] == "test_ontology"

    def test_read_owl_file(self, tmp_path):
        """Test read_owl_file function."""
        g = Graph()
        term_uri1 = URIRef("http://example.com/TEST_0000001")
        term_uri2 = URIRef("http://example.com/TEST_0000002")
        g.add((term_uri1, RDF.type, OWL.Class))
        g.add((term_uri1, RDFS.label, Literal("test term 1")))
        g.add((term_uri2, RDF.type, OWL.Class))
        g.add((term_uri2, RDFS.label, Literal("test term 2")))

        owl_file = tmp_path / "test.owl"
        g.serialize(destination=str(owl_file), format="xml")
        result = read_owl_file(str(owl_file), "test_ontology")
        assert len(result) == 2

    def test_build_ontology_index_obo(self, tmp_path):
        """Test build_ontology_index function with OBO file."""
        obo_content = """
        format-version: 1.2
        ontology: test_ontology

        [Term]
        id: TEST:0000001
        name: test term 1

        [Term]
        id: TEST:0000002
        name: test term 2
        """
        obo_file = tmp_path / "test.obo"
        obo_file.write_text(obo_content)
        output_file = tmp_path / "test.parquet"
        OlsClient.build_ontology_index(str(obo_file), str(output_file), ontology_name="test_ontology")
        assert output_file.exists()
        df = pd.read_parquet(output_file)
        assert len(df) == 2

    def test_build_ontology_index_owl(self, tmp_path):
        """Test build_ontology_index function with OWL file."""
        g = Graph()
        term_uri1 = URIRef("http://example.com/TEST_0000001")
        term_uri2 = URIRef("http://example.com/TEST_0000002")
        g.add((term_uri1, RDF.type, OWL.Class))
        g.add((term_uri1, RDFS.label, Literal("test term 1")))
        g.add((term_uri2, RDF.type, OWL.Class))
        g.add((term_uri2, RDFS.label, Literal("test term 2")))

        owl_file = tmp_path / "test.owl"
        g.serialize(destination=str(owl_file), format="xml")
        output_file = tmp_path / "test.parquet"
        OlsClient.build_ontology_index(str(owl_file), str(output_file), ontology_name="test_ontology")
        assert output_file.exists()
        df = pd.read_parquet(output_file)
        assert len(df) == 2

    def test_build_ontology_index_invalid_file(self, tmp_path):
        """Test build_ontology_index function with invalid file."""
        with pytest.raises(ValueError):
            OlsClient.build_ontology_index(None)

    def test_build_ontology_index_invalid_owl_file(self, tmp_path):
        """Test build_ontology_index function with invalid owl file."""
        owl_file = tmp_path / "test.owl"
        owl_file.write_text("")
        with pytest.raises(ValueError):
            OlsClient.build_ontology_index(str(owl_file))

    def test_build_ontology_index_invalid_owl_file_no_ontology_name(self, tmp_path):
        """Test build_ontology_index function with invalid owl file and no ontology name."""
        owl_file = tmp_path / "test.owl"
        owl_file.write_text("")
        with pytest.raises(ValueError):
            OlsClient.build_ontology_index(str(owl_file), ontology_name=None)
