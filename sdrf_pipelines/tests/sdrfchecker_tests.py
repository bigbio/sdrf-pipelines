from click.testing import CliRunner

from sdrf_pipelines.parse_sdrf import cli
from sdrf_pipelines.zooma.zooma import Zooma, SlimOlsClient


def test_validate_srdf():
  """
  Test the default behaviour of the vcf-to-proteindb tool
  :return:
  """
  runner = CliRunner()
  result = runner.invoke(cli, ['validate-sdrf', '--sdrf_file', 'testdata/sdrf.tsv', '--check_ms'])

  print(result.output)
  assert 'ERROR' not in result.output

def test_convert_openms():
  """
    Test the default behaviour of the vcf-to-proteindb tool
    :return:
    """
  runner = CliRunner()
  result = runner.invoke(cli, ['convert-openms', '-t2', '-l', '-s','testdata/sdrf.tsv'])
  print('convert to openms' + result.output)
  assert 'ERROR' not in result.output


def test_bioontologies():
  keyword = 'human'
  client = Zooma()
  results = client.recommender(keyword, filters="ontologies:[nbcitaxon]")
  ols_terms = client.process_zumma_results(results)
  print(ols_terms)

  ols_client = SlimOlsClient()
  for a in ols_terms:
    terms = ols_client.get_term_from_url(a['ols_url'], ontology="ncbitaxon")
    [print(x) for x in terms]

  keyword = 'Lung adenocarcinoma'
  client = Zooma()
  results = client.recommender(keyword)
  ols_terms = client.process_zumma_results(results)
  print(ols_terms)


if __name__ == '__main__':
  test_bioontologies()
  test_validate_srdf()
  test_convert_openms()
