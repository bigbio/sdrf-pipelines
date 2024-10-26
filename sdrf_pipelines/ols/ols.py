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
import pkg_resources
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


def _concat_str_or_list(input_str):
    """
    Always returns a comma joined list, whether the input is a
    single string or an iterable
    @:param input_str String to join
    """

    if isinstance(input_str, str):
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
    parquet_files_pattern = pkg_resources.resource_filename(__name__, "*.parquet")
    parquet_files = glob.glob(parquet_files_pattern)

    if not parquet_files:
        logger.info("No parquet files found in %s", parquet_files_pattern)
        return parquet_files_pattern, []

    # select from all the parquets the ontology names and return a list of the unique ones
    # use for reading all the parquets the duckdb library.
    df = duckdb.execute("""SELECT DISTINCT ontology FROM read_parquet(?)""", (parquet_files,)).fetchdf()

    if df is None or df.empty:
        return parquet_files, []

    ontologies = df.ontology.unique().tolist()
    return parquet_files, ontologies


def get_obo_accession(uri):
    # Example: Convert 'http://www.ebi.ac.uk/efo/EFO_0000001' to 'EFO:0000001'
    try:
        if "#" in uri:
            fragment = uri.split("#")[-1]
        else:
            fragment = uri.split("/")[-1]

        prefix, identifier = fragment.split("_")
        return f"{prefix}:{identifier}"
    except Exception as ex:
        logger.error("Error converting URI %s to OBO accession: %s", uri, ex)

    return None


def read_owl_file(ontology_file, ontology_name=None):
    """
    Reads an OWL file and returns a list of OlsTerms
    @:param ontology_file: The name of the ontology
    @:param ontology_name: The name of the ontology
    """
    g = rdflib.Graph()
    g.parse(ontology_file, format="xml")
    terms_info = []

    for s, _, _ in g.triples((None, rdflib.RDF.type, rdflib.OWL.Class)):
        term_id = str(s)
        for _, _, name in g.triples((s, rdflib.RDFS.label, None)):
            term_name = str(name)
            terms_info.append({"accession": get_obo_accession(term_id), "label": term_name, "ontology": ontology_name})

    # remove terms with no label or accession
    terms_info = [term for term in terms_info if "label" in term and "accession" in term]
    return terms_info


def read_obo_file(ontology_file, ontology_name=None):
    """
    Reads an OBO file and returns a list of OlsTerms
    @:param ontology_file: The name of the ontology
    @:param ontology_name: The name of the ontology
    """

    def split_terms(content):
        terms = content.split("[Term]")[1:]  # Skip the header and split by [Term]
        return terms

    def get_ontology_name(content):
        lines = content.split("\n")
        for line in lines:
            if line.startswith("ontology:"):
                return line.split("ontology:")[1].strip()
        return None

    def parse_term(term, ontology_name):
        term_info = {}
        lines = term.strip().split("\n")
        for line in lines:
            if line.startswith("id:"):
                term_info["accession"] = line.split("id:")[1].strip()
                term_info["ontology"] = ontology_name
            elif line.startswith("name:"):
                term_info["label"] = line.split("name:")[1].strip()
        return term_info

    with open(ontology_file, "r") as file:
        content = file.read()

    terms = split_terms(content)
    ontology_name = get_ontology_name(content) if ontology_name is None else ontology_name
    terms_info = [parse_term(term, ontology_name) for term in terms]

    return terms_info


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

    @staticmethod
    def build_ontology_index(ontology_file: str, output_file: str = None, ontology_name: str = None):
        """
        Builds an index of an ontology file OBO format. The output file will be a parquet file containing only three columns:
        - the accession of the term in the form of ONTOLOGY:NUMBER (e.g. GO:0000001) the name of the term and the number.
        - The name of the term.
        - The ontology in which the term is found (e.g. GO).
        All information should be in lower case and also the file will be compressed.
        @:param ontology_file: The name of the ontology
        @:param output_file: The name of the output file
        @:param ontology_name: The name of the ontology
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
        df["accession"] = df["accession"].astype("string")  # Ensuring a string type
        df["label"] = df["label"].astype("string")  # Ensuring a string type
        df["ontology"] = df["ontology"].astype("string")  # Ensuring a string type

        # Remove terms with no label or accession and print a warning
        df = df.dropna(subset=["label", "accession"])
        if df.empty:
            logger.warning("No terms found in %s", ontology_file)
            raise ValueError(f"No terms found in {ontology_file}")
        logger.info("Terms found in %s: %s", ontology_file, len(df))

        df.to_parquet(output_file, compression="gzip", index=False)
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

    def search(self, term: str, ontology: str = None, exact=True, use_ols_cache_only: bool = False, **kwargs):
        """
        Search a term in the OLS
        @:param term: The name of the term
        @:param ontology: The name of the ontology
        @:param exact: Forces exact match if not `None`
        """
        if use_ols_cache_only:
            terms = self.cache_search(term, ontology)
        else:
            terms = self.ols_search(term, ontology=ontology, exact=exact, **kwargs)
            if terms is None and self.use_cache:
                terms = self.cache_search(term, ontology)
        return terms

    def _perform_ols_search(self, params, name, exact, retry_num=0):
        try:
            req = self.session.get(self.ontology_search, params=params)
            logger.debug("Request to OLS search API term %s, status code %s", name, req.status_code)

            if req.status_code != 200:
                logger.error("OLS search term %s error, retry number %s", name, retry_num)
                req.raise_for_status()

            response_json = req.json()
            num_found = response_json["response"]["numFound"]
            docs = response_json["response"]["docs"]

            if num_found == 0:
                logger.debug("OLS %s search returned empty response for %s", "exact" if exact else "", name)
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
        params = {"q": name, "type": _concat_str_or_list(bytype), "rows": rows, "start": start}
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
            docs = self._perform_ols_search(params, name=name, exact=exact, retry_num=retry_num)
            if docs:
                docs_found.extend(docs)
                if len(docs) < rows:
                    return docs_found

            start += rows
            params["start"] = start

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
            terms.append({"ontology_name": row.ontology, "label": row.label, "obo_id": row.accession})

        return terms
