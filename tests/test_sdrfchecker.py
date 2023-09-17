import pytest
from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli
from sdrf_pipelines.zooma.zooma import SlimOlsClient
from sdrf_pipelines.zooma.zooma import Zooma

from .helpers import compare_files
from .helpers import run_and_check_status_code


def test_validate_srdf_errors_on_bad_file(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "erroneous/PXD000288/PXD000288.sdrf.tsv"
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf), "--check_ms"], 1)

    expected_error = (
        "The following columns are mandatory and not present in the SDRF: comment[technical replicate] -- ERROR"
    )
    assert "ERROR" in result.output.upper(), result.output
    assert expected_error in result.output, result.output


def test_validate_srdf_fails_on_bad_file2(shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / "PXD001819/PXD001819.sdrf.tsv"
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf), "--check_ms"], 1)

    expected_error = "The following columns are mandatory and not present in the SDRF: characteristics[biological replicate] -- ERROR"
    assert expected_error in result.output, result.output


reference_samples = [
    "reference/PXD002137/PXD002137.sdrf.tsv",
    "reference/PDC000126/PDC000126.sdrf.tsv",
    "reference/PXD008934/PXD008934.sdrf.tsv",
    "reference/PXD006482/PXD006482.sdrf.tsv",
    "reference/PXD004684/PXD004684.sdrf.tsv",
]


@pytest.mark.parametrize("file_subpath", reference_samples)
def test_on_reference_sdrf(file_subpath, shared_datadir, on_tmpdir):
    """
    :return:
    """
    test_sdrf = shared_datadir / file_subpath
    result = run_and_check_status_code(cli, ["validate-sdrf", "--sdrf_file", str(test_sdrf), "--check_ms"])
    assert "ERROR" not in result.output.upper(), result.output


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
