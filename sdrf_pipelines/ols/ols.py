"""
OLS API wrapper

Original code borrowed from
 https://github.com/cthoyt/ols-client/blob/master/src/ols_client/client.py

- Removed ontology and term methods.
- Added details/parameters for all search methods

TODO: check input parameters are valid
TODO: handle requests.exceptions.ConnectionError when traffic is too high and API goes down
"""

import glob
import logging
import os.path
import urllib.parse

import duckdb
import pandas as pd
import requests
from pronto import Ontology

OLS = "https://www.ebi.ac.uk/ols4"

__all__ = ["OlsClient"]

logger = logging.getLogger(__name__)

API_SUGGEST = "/api/suggest"
API_SEARCH = "/api/search"
API_SELECT = "/api/select"
API_TERM = "/api/ontologies/{ontology}/terms/{iri}"
API_ANCESTORS = "/api/ontologies/{ontology}/terms/{iri}/ancestors"
API_PROPERTIES = "/api/ontologies/{ontology}/properties?lang=en"


def _concat_str_or_list(input_str):
    """
    Always returns a comma joined list, whether the input is a
    single string or an iterable
    @:param input_str String to join
    """

    if type(input_str) is str:
        return input_str

    return ",".join(input_str)


def _dparse(iri):
    """
    Double url encode the IRI, which is required
    @:param iri in the OLS
    """
    return urllib.parse.quote_plus(urllib.parse.quote_plus(iri))


class OlsTerm:
    def __init__(self, iri: str = None, term: str = None, ontology: str = None) -> None:
        self._iri = iri
        self._term = term
        self._ontology = ontology

    def __str__(self) -> str:
        return f"{self._term} -- {self._ontology} -- {self._iri}"


def get_cache_parquet_files():
    """
    This function returns a list of parquet files in the cache directory.
    """
    cache_dir = os.path.join(os.path.dirname(__file__))
    parquet_files = os.path.join(cache_dir, "*.parquet")
    if len(glob.glob(parquet_files)) == 0:
        logger.info("No parquet files found in %s", parquet_files)
        return (parquet_files, [])

    # select from all the parquets the ontology names and return a list of the unique ones
    # use for reading all the parquets the duckdb library.
    query = """SELECT DISTINCT ontology FROM read_parquet('{}')""".format(parquet_files)
    df = duckdb.query(query).fetchdf()

    if df is None or df.empty:
        return parquet_files, []

    ontologies = df.ontology.unique().tolist()
    return parquet_files, ontologies


class OlsClient:
    def __init__(self, ols_base=None, ontology=None, field_list=None, query_fields=None, use_cache=True):
        """
        @:param ols_base: The base URL for the OLS
        @:param ontology: The name of the ontology
        @:param field_list: The list of fields to return
        @:param query_fields: The list of fields to query
        @:param use_cache: Whether to use cache which are local files with the same terms
        """
        self.base = (ols_base if ols_base else OLS).rstrip("/")
        self.session = requests.Session()

        self.ontology = ontology if ontology else None
        self.field_list = field_list if field_list else None
        self.query_fields = query_fields if query_fields else None

        self.ontology_suggest = self.base + API_SUGGEST
        self.ontology_select = self.base + API_SELECT
        self.ontology_search = self.base + API_SEARCH
        self.ontology_term = self.base + API_TERM
        self.ontology_ancestors = self.base + API_ANCESTORS

        if use_cache:
            self.use_cache = use_cache
            parquet_ontologies, ontologies = get_cache_parquet_files()
            if len(parquet_ontologies) == 0:
                self.use_cache = False
            else:
                self.parquet_files = parquet_ontologies
                self.ontologies = ontologies
        else:
            self.use_cache = False

    def build_ontology_index(self, ontology_file: str, output_file: str = None):
        """
        Builds an index of an ontology file OBO format. The output file will be a parquet file containing only three columns:
        - the accession of the term in the form of ONTOLOGY:NUMBER (e.g. GO:0000001) the name of the term and the number.
        - The name of the term.
        - The ontology in which the term is found (e.g. GO).
        All information should be in lower case and also the file will be compressed.
        @:param ontology_file: The name of the ontology
        @:param output_file: The name of the output file
        """

        if ontology_file is None or not os.path.isfile(ontology_file):
            raise ValueError(f"File {ontology_file} is None or does not exist")

        # check an extension of the ontology file
        if not ontology_file.lower().endswith(".obo"):
            raise ValueError(f"File {ontology_file} is not an OBO format, at the moment only OBO is supported")

        if output_file is None or not output_file.lower().endswith(".parquet"):
            output_file = os.path.splitext(ontology_file)[0] + ".parquet"

        logger.info("Building index of %s", ontology_file)
        ontology = Ontology(ontology_file)

        df = pd.DataFrame(
            [
                {
                    "accession": ontology[term].id,
                    "label": ontology[term].name,
                    "ontology": ontology.metadata.ontology,
                }
                for term in ontology
            ]
        )

        df.to_parquet(output_file, compression="gzip")
        logger.info("Index has finished, output file: %s", output_file)

    def besthit(self, name, **kwargs):
        """
        select a first element of the /search API response
        """
        search_resp = self.search(name, **kwargs)
        if search_resp:
            return search_resp[0]

        return None

    def get_term(self, ontology, iri):
        """
        Gets the data for a given term
            Args:
            ontology: The name of the ontology
            iri: The IRI of a term
        """

        url = self.ontology_term.format(ontology=ontology, iri=_dparse(iri))
        response = self.session.get(url)
        return response.json()

    def get_ancestors(self, ont, iri):
        """
        Gets the data for a given term
        @param ont: The name of the ontology
        @param iri:The IRI of a term
        """
        url = self.ontology_ancestors.format(ontology=ont, iri=_dparse(iri))
        response = self.session.get(url)
        try:
            return response.json()["_embedded"]["terms"]
        except KeyError as ex:
            logger.warning("Term was found but ancestor lookup returned an empty response: %s", response.json())
            raise ex

    def search(
        self,
        name: str,
        query_fields=None,
        ontology: str = None,
        field_list=None,
        children_of=None,
        exact: bool = None,
        bytype: str = "class",
        rows: int = 10,
        num_retries: int = 10,
        start: int = 0,
    ):
        """
        Searches the OLS with the given term

        @:param query_fields: By default, the search is performed over term labels,
        synonyms, descriptions, identifiers and annotation properties. This option allows
        to specify the fields to query, the defaults are
        `{label, synonym, description, short_form, obo_id, annotations, logical_description, iri}`
        @:param exact: Forces exact match if not `None`
        @:param bytype: restrict to terms one of {class,property,individual,ontology}
        @:param childrenOf: Search only under a certain term.
        @:param rows: number of rows to query on each call of OLS search
        @:param num_retries: Number of retries to OLS when it fails.
        """
        params = {"q": name}
        if ontology is not None:
            ontology = ontology.lower()

        if exact:
            params["exact"] = "on"

        if bytype:
            params["type"] = _concat_str_or_list(bytype)

        if rows:
            params["rows"] = rows

        if ontology:
            params["ontology"] = _concat_str_or_list(ontology)
        elif self.ontology:
            params["ontology"] = _concat_str_or_list(self.ontology)

        if query_fields:
            params["queryFields"] = _concat_str_or_list(query_fields)
        elif self.query_fields:
            params["queryFields"] = _concat_str_or_list(self.query_fields)

        if field_list:
            params["fieldList"] = _concat_str_or_list(field_list)
        elif self.field_list:
            params["fieldList"] = _concat_str_or_list(self.field_list)

        if children_of is None:
            children_of = []
        if len(children_of) > 0:
            params["childrenOf"] = _concat_str_or_list(children_of)

        if start:
            params["start"] = start

        docs_found = []

        for retry_num in range(num_retries):
            try:
                req = self.session.get(self.ontology_search, params=params)
                logger.debug("Request to OLS search API term %s, status code %s", name, req.status_code)

                if req.status_code != 200:
                    logger.error("OLS search term %s error tried number %s", name, retry_num)
                    req.raise_for_status()
                else:
                    if req.json()["response"]["numFound"] == 0:
                        if exact:
                            logger.debug("OLS exact search returned empty response for %s", name)
                        else:
                            logger.debug("OLS search returned empty response for %s", name)
                        return docs_found
                    elif len(req.json()["response"]["docs"]) < rows:
                        return req.json()["response"]["docs"]
                    else:
                        docs_found = req.json()["response"]["docs"]
                        docs_found.extend(
                            self.search(
                                name,
                                query_fields=query_fields,
                                ontology=ontology,
                                field_list=field_list,
                                children_of=children_of,
                                exact=exact,
                                bytype=bytype,
                                rows=rows,
                                num_retries=num_retries,
                                start=(rows + start),
                            )
                        )
                        return docs_found

                if req.status_code == 200 and req.json()["response"]["numFound"] == 0:
                    if exact:
                        logger.debug("OLS exact search returned empty response for %s", name)
                    else:
                        logger.debug("OLS search returned empty response for %s", name)
                    return None
                elif req.status_code != 200 and req.json()["response"]["numFound"] > 0:
                    if len(req.json()["response"]["docs"]) <= rows:
                        return req.json()["response"]["docs"]
                    else:
                        start = 0
                        docs_found = req.json()["response"]["docs"]

            except Exception as ex:
                logger.exception(
                    "OLS error searching the following term -- %s iteration %s.\n%e", req.url, retry_num, ex
                )

        return docs_found

    def suggest(self, name, ontology=None):
        """Suggest terms from an optional list of ontologies

        .. seealso:: https://www.ebi.ac.uk/ols/docs/api#_suggest_term
        """
        params = {"q": name}
        if ontology:
            params["ontology"] = ",".join(ontology)
        response = self.session.get(self.ontology_suggest, params=params)
        response.raise_for_status()

        if response.json()["response"]["numFound"]:
            return response.json()["response"]["docs"]
        logger.debug("OLS suggest returned empty response for %s", name)
        return None

    def select(self, name, ontology=None, field_list=None):
        """Select terms,
        Tuned specifically to support applications such as autocomplete.

        .. see also:: https://www.ebi.ac.uk/ols4/docs/api#_select
        """
        params = {"q": name}
        if ontology:
            params["ontology"] = ",".join(ontology)
        if field_list:
            params["fieldList"] = ",".join(field_list)
        response = self.session.get(self.ontology_select, params=params)
        response.raise_for_status()

        if response.json()["response"]["numFound"]:
            return response.json()["response"]["docs"]
        logger.debug("OLS select returned empty response for %s", name)
        return None

    def cache_search(self, term: str, ontology: str, full_search: bool = False) -> list:
        """
        Search a term in cache files and return them as list.
        @param term: The name of the term
        @param ontology: The name of the ontology
        """
        is_cached = False
        if ontology is not None:
            for cache_ontologies in self.ontologies:
                if cache_ontologies.lower() == ontology.lower():
                    is_cached = True
                    break
        if not is_cached and not full_search:
            return []

        if ontology is not None:
            query = """SELECT * FROM read_parquet('{}') WHERE lower(label) = lower('{}') AND lower(ontology) = lower('{}')""".format(
                self.parquet_files, term, ontology
            )
        else:
            query = """SELECT * FROM read_parquet('{}') WHERE lower(label) = lower('{}')""".format(
                self.parquet_files, term
            )
        df = duckdb.query(query).fetchdf()

        if df is None or df.empty:
            return []

        terms = []
        for index, row in df.iterrows():
            terms.append({"ontology_name": row.ontology, "label": row.label, "obo_id": row.accession})

        return terms