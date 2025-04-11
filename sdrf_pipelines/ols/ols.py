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
from importlib.resources import files
from typing import Union

import duckdb
import pandas as pd
import rdflib
import requests

OLS = "https://www.ebi.ac.uk/ols4"

__all__ = ["OlsClient"]

logger = logging.getLogger(__name__)

API_SUGGEST = "/api/suggest"
API_SEARCH = "/api/search"
API_SELECT = "/api/select"
API_TERM = "/api/ontologies/{ontology}/terms/{iri}"
API_ANCESTORS = "/api/ontologies/{ontology}/terms/{iri}/ancestors"
API_PROPERTIES = "/api/ontologies/{ontology}/properties?lang=en"


def _concat_str_or_list(input_str: Union[str, list]) -> str:
    """
    Always returns a comma joined list, whether the input is a
    single string or an iterable

    Parameters:
        input_str (str or list): The input string or list to be concatenated

    Returns:
        str: A comma-separated string if input_str is a list, or the input_str itself if it's a string
    """

    if isinstance(input_str, str):
        return input_str

    return ",".join(input_str)


def _dparse(iri: str) -> str:
    """
    Double url encode the IRI, which is required.

    Parameters:
        iri (str): The IRI to be encoded

    Returns:
        str: The double url encoded IRI
    """
    return urllib.parse.quote_plus(urllib.parse.quote_plus(iri))


class OlsTerm:

    def __init__(self, iri: str = None, term: str = None, ontology: str = None) -> None:
        self._iri = iri
        self._term = term
        self._ontology = ontology

    def __str__(self) -> str:
        return f"{self._term} -- {self._ontology} -- {self._iri}"


def get_cache_parquet_files() -> tuple:
    """
    This function returns a list of parquet files in the cache directory.

    Returns:
        tuple: A tuple containing the parquet files pattern and a list of unique ontology names
    """

    # For getting a pattern string
    parquet_files_pattern = str(files(__package__).joinpath("*.parquet"))
    parquet_files = glob.glob(parquet_files_pattern)

    if not parquet_files:
        logger.info("No parquet files found in %s", parquet_files_pattern)
        return parquet_files_pattern, []

    # select from all the parquets the ontology names and return a list of the unique ones
    # use for reading all the parquets the duckdb library.
    df = duckdb.execute(
        """SELECT DISTINCT ontology FROM read_parquet(?)""", (parquet_files,)
    ).fetchdf()

    if df is None or df.empty:
        return parquet_files, []

    ontologies = df.ontology.unique().tolist()
    return parquet_files, ontologies


def get_obo_accession(uri: str) -> Union[str, None]:
    """
    Get the OBO accession from the URI.
    The URI is expected to be in the form of 'http://www.ebi.ac.uk/efo/EFO_0000001'
    Example: Convert 'http://www.ebi.ac.uk/efo/EFO_0000001' to 'EFO:0000001'

    Parameters:
        uri (str): The URI to convert

    Returns:
        str: The OBO accession in the form of 'PREFIX:IDENTIFIER'
    """
    try:
        fragment = uri.split("/")[-1]
        if "#" in uri:
            prefix, identifier = fragment.split("#")
        else:
            prefix, identifier = fragment.split("_")

        return f"{prefix}:{identifier}"
    except Exception as ex:
        logger.error("Error converting URI %s to OBO accession: %s", uri, ex)

    return None


def read_owl_file(ontology_file: str, ontology_name=None) -> list:
    """
    Reads an OWL file and returns a list of OlsTerms

    Parameters:
        ontology_file (str): The name of the ontology file
        ontology_name (str): The name of the ontology

    Returns:
        list: A list of dictionaries containing the ontology terms
    """
    g = rdflib.Graph()
    g.parse(ontology_file, format="xml")
    terms_info = []

    for s, _, _ in g.triples((None, rdflib.RDF.type, rdflib.OWL.Class)):
        term_id = str(s)
        for _, _, name in g.triples((s, rdflib.RDFS.label, None)):
            term_name = str(name)
            terms_info.append(
                {
                    "accession": get_obo_accession(term_id),
                    "label": term_name,
                    "ontology": ontology_name,
                }
            )

    # remove terms with no label or accession
    terms_info = [
        term for term in terms_info if "label" in term and "accession" in term
    ]
    return terms_info


def read_obo_file(ontology_file: str, ontology_name=None) -> list:
    """
    Reads an OBO file and returns a list of OlsTerms

    Parameters:
        ontology_file (str): The name of the ontology file
        ontology_name (str): The name of the ontology

    Returns:
        list: A list of dictionaries containing the ontology terms
    """

    def split_terms(content_str: str) -> list:
        return content_str.split("[Term]")[1:]  # Skip the header and split by [Term]

    def get_ontology_name(content_str: str) -> Union[str, None]:
        lines = content_str.split("\n")
        for line in lines:
            if line.startswith("ontology:"):
                return line.split("ontology:")[1].strip()
        return None

    def parse_term(term: str, ontology_name_param: str) -> dict:
        term_info = {}
        lines = term.strip().split("\n")
        for line in lines:
            if line.strip().startswith("id:"):
                term_info["accession"] = line.split("id:")[1].strip()
                term_info["ontology"] = ontology_name_param
            elif line.strip().startswith("name:"):
                term_info["label"] = line.split("name:")[1].strip()
        return term_info

    with open(ontology_file, "r") as file:
        content = file.read()

    terms = split_terms(content)
    ontology_name = (
        get_ontology_name(content) if ontology_name is None else ontology_name
    )
    terms_info = [parse_term(term, ontology_name) for term in terms]

    return terms_info


class OlsClient:
    def __init__(
        self,
        ols_base: str = None,
        ontology: str = None,
        field_list: list = None,
        query_fields: list = None,
        use_cache: bool = True,
    ):
        """
        The Ols client is a wrapper around the OLS API.

        Parameters:
            ols_base (str): The base URL of the OLS API
            ontology (str): The name of the ontology
            field_list (list): A list of fields to return
            query_fields (list): A list of fields to search
            use_cache (bool): Whether to use the cache
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

    @staticmethod
    def build_ontology_index(
        ontology_file: str, output_file: str = None, ontology_name: str = None
    ):
        """
        Builds an index of an ontology file OBO format. The output file will be a parquet file containing only three columns:
        - the accession of the term in the form of ONTOLOGY:NUMBER (e.g. GO:0000001) the name of the term and the number.
        - The name of the term.
        - The ontology in which the term is found (e.g. GO).
        All information should be in lower case and also the file will be compressed.

        Parameters:
            ontology_file (str): The name of the ontology file
            output_file (str): The name of the output file
            ontology_name (str): The name of the ontology

        """

        if ontology_file is None or not os.path.isfile(ontology_file):
            raise ValueError(f"File {ontology_file} is None or does not exist")

        # check an extension of the ontology file
        owl_file = False
        if not ontology_file.lower().endswith(".obo"):
            owl_file = True
            if ontology_name is None:
                raise ValueError("Ontology name is required for OWL files")

        if output_file is None or not output_file.lower().endswith(".parquet"):
            output_file = os.path.splitext(ontology_file)[0] + ".parquet"

        logger.info("Building index of %s", ontology_file)

        if owl_file:
            terms = read_owl_file(ontology_file, ontology_name=ontology_name)
            terms = [term for term in terms if "label" in term]
            df = pd.DataFrame(terms)
        else:
            terms = read_obo_file(ontology_file, ontology_name=ontology_name)
            # remove terms with no label
            terms = [term for term in terms if "label" in term]
            df = pd.DataFrame(terms)

        # Convert to lowercase as needed
        df["accession"] = df["accession"].str.lower()
        df["label"] = df["label"].str.lower()
        df["ontology"] = df["ontology"].str.lower()

        # Enforce data types (schema)
        df["accession"] = df["accession"].astype("string")  # Ensuring a string name
        df["label"] = df["label"].astype("string")  # Ensuring a string name
        df["ontology"] = df["ontology"].astype("string")  # Ensuring a string name

        # Remove terms with no label or accession and print a warning
        df = df.dropna(subset=["label", "accession"])
        if df.empty:
            logger.warning("No terms found in %s", ontology_file)
            raise ValueError(f"No terms found in {ontology_file}")
        logger.info("Terms found in %s: %s", ontology_file, len(df))

        df.to_parquet(output_file, compression="gzip", index=False)
        logger.info("Index has finished, output file: %s", output_file)

    def besthit(self, name, **kwargs) -> Union[dict, None]:
        """
        select a first element of the /search API response
        """
        search_resp = self.search(name, **kwargs)
        if search_resp:
            return search_resp[0]

        return None

    def get_term(self, ontology: str, iri: str) -> dict:
        """
        Gets the data for a given term

        Parameters:
            ontology (str): The name of the ontology
            iri (str): The IRI of a term

        Returns:
            dict: The term data
        """

        url = self.ontology_term.format(ontology=ontology, iri=_dparse(iri))
        response = self.session.get(url)
        return response.json()

    def get_ancestors(self, ontology: str, iri: str):
        """
        Gets the data for a given term

        Parameters:
            ontology (str): The name of the ontology
            iri (str): The IRI of a term

        Returns:
            dict: The term data
        """
        url = self.ontology_ancestors.format(ontology=ontology, iri=_dparse(iri))
        response = self.session.get(url)
        try:
            return response.json()["_embedded"]["terms"]
        except KeyError as ex:
            logger.warning(
                "Term was found but ancestor lookup returned an empty response: %s",
                response.json(),
            )
            raise ex

    def search(
        self,
        term: str,
        ontology: str = None,
        exact=True,
        use_ols_cache_only: bool = False,
        **kwargs,
    ):
        """
        Search a term in the OLS

        Parameters:
            term (str): The name of the term
            ontology (str): The name of the ontology
            exact (bool): Whether to search for an exact match
            use_ols_cache_only (bool): Whether to use the cache only

        Returns:
            list: A list of terms found
        """
        if use_ols_cache_only:
            terms = self.cache_search(term, ontology)
        else:
            terms = self.ols_search(term, ontology=ontology, exact=exact, **kwargs)
            if terms is None and self.use_cache:
                terms = self.cache_search(term, ontology)
        return terms

    def _perform_ols_search(self, params, name: str, exact: bool, retry_num: int = 0):
        """
        Perform the OLS search and return the results.

        Parameters:
            params (dict): The search parameters
            name (str): The name of the term
            exact (bool): Whether to search for an exact match
            retry_num (int): The number of retries

        Returns:
            list: A list of terms found
        """
        try:
            req = self.session.get(self.ontology_search, params=params)
            logger.debug(
                "Request to OLS search API term %s, status code %s",
                name,
                req.status_code,
            )

            if req.status_code != 200:
                logger.error(
                    "OLS search term %s error, retry number %s", name, retry_num
                )
                req.raise_for_status()

            response_json = req.json()
            num_found = response_json["response"]["numFound"]
            docs = response_json["response"]["docs"]

            if num_found == 0:
                logger.debug(
                    "OLS %s search returned empty response for %s",
                    "exact" if exact else "",
                    name,
                )
                return []

            return docs
        except Exception as ex:
            logger.exception("OLS error searching term %s. Error: %s", name, ex)

    def ols_search(
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
        Search a term in the OLS API

        Parameters:
            name (str): The name of the term
            query_fields (list): A list of fields to search
            ontology (str): The name of the ontology
            field_list (list): A list of fields to return
            children_of (str): The IRI of a parent term
            exact (bool): Whether to search for an exact match
            bytype (str): The name of term to search for
            rows (int): The number of rows to return
            num_retries (int): The number of retries
            start (int): The starting index for pagination

        Returns:
            list: A list of terms found
        """
        params = {
            "q": name,
            "name": _concat_str_or_list(bytype),
            "rows": rows,
            "start": start,
        }
        if ontology:
            params["ontology"] = _concat_str_or_list(ontology.lower())
        elif self.ontology:
            params["ontology"] = _concat_str_or_list(self.ontology)

        if exact:
            params["exact"] = "on"

        if query_fields:
            params["queryFields"] = _concat_str_or_list(query_fields)
        elif self.query_fields:
            params["queryFields"] = _concat_str_or_list(self.query_fields)

        if field_list:
            params["fieldList"] = _concat_str_or_list(field_list)
        elif self.field_list:
            params["fieldList"] = _concat_str_or_list(self.field_list)

        if children_of:
            params["childrenOf"] = _concat_str_or_list(children_of)

        docs_found = []

        for retry_num in range(num_retries):
            docs = self._perform_ols_search(
                params, name=name, exact=exact, retry_num=retry_num
            )
            if docs:
                docs_found.extend(docs)
                if len(docs) < rows:
                    return docs_found

            start += rows
            params["start"] = start

        return docs_found

    def cache_search(self, term: str, ontology: str, full_search: bool = False) -> list:
        """
        Search a term in cache files and return them as list.

        Parameters:
            term (str): The name of the term
            ontology (str): The name of the ontology
            full_search (bool): Whether to perform a full search

        Returns:
            list: A list of terms found
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
            # Query for case-insensitive search and ensure all fields are cast to string
            duckdb_conn = duckdb.execute(
                """SELECT CAST(accession AS VARCHAR) AS accession,
                          CAST(label AS VARCHAR) AS label,
                          CAST(ontology AS VARCHAR) AS ontology
                   FROM read_parquet(?)
                   WHERE lower(CAST(label AS VARCHAR)) = lower(?)
                     AND lower(CAST(ontology AS VARCHAR)) = lower(?)""",
                (self.parquet_files, term, ontology),
            )
        else:
            # Query for case-insensitive search without ontology
            duckdb_conn = duckdb.execute(
                """SELECT CAST(accession AS VARCHAR) AS accession,
                          CAST(label AS VARCHAR) AS label,
                          CAST(ontology AS VARCHAR) AS ontology
                   FROM read_parquet(?)
                   WHERE lower(CAST(label AS VARCHAR)) = lower(?)""",
                (self.parquet_files, term),
            )
        df = duckdb_conn.fetchdf()

        if df is None or df.empty:
            return []

        terms = []
        for _, row in df.iterrows():
            terms.append(
                {
                    "ontology_name": row.ontology,
                    "label": row.label,
                    "obo_id": row.accession,
                }
            )

        return terms