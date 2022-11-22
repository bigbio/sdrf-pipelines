from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli
from sdrf_pipelines.zooma.zooma import SlimOlsClient
from sdrf_pipelines.zooma.zooma import Zooma


def test_validate_srdf():
    """
    :return:
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["validate-sdrf", "--sdrf_file", "testdata/PXD000288.sdrf.tsv", "--check_ms"])

    print(result.output)
    assert "ERROR" not in result.output


def test_convert_openms():
    """
    :return:
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["convert-openms", "-t2", "l", "-s", "testdata/sdrf.tsv"])
    print("convert to openms" + result.output)
    assert "ERROR" not in result.output


def test_bioontologies():
    keyword = "human"
    client = Zooma()
    results = client.recommender(keyword, filters="ontologies:[nbcitaxon]")
    ols_terms = client.process_zooma_results(results)
    print(ols_terms)

    ols_client = SlimOlsClient()
    for ols_term in ols_terms:
        terms = ols_client.get_term_from_url(ols_term["ols_url"], ontology="ncbitaxon")
        print(*terms, sep="\n")

    keyword = "Lung adenocarcinoma"
    client = Zooma()
    results = client.recommender(keyword)
    ols_terms = client.process_zooma_results(results)
    print(ols_terms)


if __name__ == "__main__":
    test_bioontologies()
    test_validate_srdf()
    test_convert_openms()
