"""
OLS API wrapper

Original code borrowed from
 https://github.com/cthoyt/ols-client/blob/master/src/ols_client/client.py

- Removed ontology and term methods.
- Added details/parameters for all search methods

TODO: check input parameters are valid
TODO: handle requests.exceptions.ConnectionError when traffic is too high and API goes down
"""

import logging
import os.path
import urllib.parse
from pathlib import Path
from typing import Any

# Try to import OLS dependencies - these are optional and only needed for ontology validation
try:
    import pooch
    import rdflib
    import requests

    OLS_AVAILABLE = True
except ImportError:
    OLS_AVAILABLE = False

import pandas as pd

try:
    from sdrf_pipelines import __version__
except ImportError:
    __version__ = "dev"

OLS = "https://www.ebi.ac.uk/ols4"

__all__ = ["OlsClient", "OLS_AVAILABLE"]

logger = logging.getLogger(__name__)

API_SUGGEST = "/api/suggest"
API_SEARCH = "/api/search"
API_SELECT = "/api/select"
API_TERM = "/api/ontologies/{ontology}/terms/{iri}"
API_ANCESTORS = "/api/ontologies/{ontology}/terms/{iri}/ancestors"
API_PROPERTIES = "/api/ontologies/{ontology}/properties?lang=en"

# Ontology file names
ONTOLOGY_FILES = [
    "bto.parquet",
    "chebi.parquet",
    "cl.parquet",
    "clo.parquet",
    "efo.parquet",
    "mondo.parquet",
    "ncbitaxon.parquet",
    "ncit.parquet",
    "pato.parquet",
    "pride.parquet",
    "psi-ms.parquet",
    "uberon.parquet",
    "unimod.parquet",
]

# Pooch registry with SHA256 hashes for all ontology parquet files
ONTOLOGY_REGISTRY = {
    "bto.parquet": "sha256:21801b276e7e8579e548ef8bedde7bf619bf1bdd9415948fd97440c153f9da60",
    "chebi.parquet": "sha256:df67582ed67d25e06bf388bb1bfaf8aa24dc37b6355b01a8569091c19fb085b1",
    "cl.parquet": "sha256:7518368ad07950ed02c80b3bc40235c1c2331d1b0886dff1dce36dac052dfe3f",
    "clo.parquet": "sha256:d7fa1bb3a4eef179d2d06afe22695682bb888337d9d7122a5576a036830d5e33",
    "efo.parquet": "sha256:22acf52b85b2c79788629a5f345c79558e25bb534c1e83fec19772b42cf7e309",
    "mondo.parquet": "sha256:d9c37abe3067e7d66f0f07ec4917f277eb3ad23bf198f4d9a40d1e7a1a07d2ab",
    "ncbitaxon.parquet": "sha256:e8ffb653aa7bd98111633f462c74c326f10a21d779d909f3f20c6443543bb27e",
    "ncit.parquet": "sha256:9f9b3ef71c718b8f3e458c41256b72307736133fb0a0626097b6e8c12dce9ffe",
    "pato.parquet": "sha256:3562e992d89af36303978ddff9c0b59b6b18d5aff20e9030491b19cfc054b382",
    "pride.parquet": "sha256:de6d9878b10041ea1db52d8206dd3e58d98df895e411697cbf617cc08eed2952",
    "psi-ms.parquet": "sha256:a897c46c4e707cf3ff6299d15dd2fa5a3bfbcd960ca6e2e9878e0451e55f411e",
    "uberon.parquet": "sha256:564fd8ae93d15e5b95db8074160f1866105d321c95492da75a5e900f8dd16249",
    "unimod.parquet": "sha256:2e4e9657253d94ed8be8f8b78f4238631b87949433a25af243ec4ee11feeb08d",
}

# Pooch configuration for downloading ontology files from GitHub
if OLS_AVAILABLE:
    # Determine version string for URL
    # Parse version to extract branch information
    # Formats from setuptools-scm with custom local scheme:
    #   - "1.0.0" (release tag) -> v1.0.0
    #   - "1.0.0.dev5+g1a2b3c" (main dev) -> main
    #   - "1.0.0.dev5+feature_branch.g1a2b3c" (PR branch) -> feature/branch

    import re

    def _parse_version_to_branch(version: str) -> str:
        """Parse version string to determine git ref (tag or branch)"""
        # Extract branch name from local version identifier
        # Format: +<branch>.<node> or +<node>
        local_match = re.search(r"\+([a-zA-Z0-9_]+)(?:\.[a-zA-Z0-9]+)?", version)
        if local_match:
            branch_part = local_match.group(1)
            # If it looks like a commit hash (starts with 'g'), it's main
            if branch_part.startswith("g"):
                return "main"
            # Otherwise it's a branch name - restore slashes
            return branch_part.replace("_", "/")

        # Development version without local part -> main branch
        if "dev" in version or version == "dev":
            return "main"

        # Release version -> tag
        return version if version.startswith("v") else f"v{version}"

    _version = _parse_version_to_branch(__version__)

    ONTOLOGY_POOCH = pooch.create(
        path=pooch.os_cache("sdrf-pipelines/ontologies"),
        base_url=f"https://raw.githubusercontent.com/bigbio/sdrf-pipelines/{_version}/data/ontologies/",
        registry=ONTOLOGY_REGISTRY,
    )
else:
    ONTOLOGY_POOCH = None


def _concat_str_or_list(input_str: str | list[str]) -> str:
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
    def __init__(self, iri: str | None = None, term: str | None = None, ontology: str | None = None) -> None:
        self._iri = iri
        self._term = term
        self._ontology = ontology

    def __str__(self) -> str:
        return f"{self._term} -- {self._ontology} -- {self._iri}"


def download_ontology_cache(
    ontologies: list[str] | None = None,
    cache_dir: str | None = None,
    force: bool = False,
) -> list[str]:
    """
    Download ontology parquet files from GitHub using pooch.

    Parameters:
        ontologies (list): List of ontology names to download (e.g., ['efo', 'cl']).
                          If None, downloads all available ontologies.
        cache_dir (str): Override default cache directory location
        force (bool): Force re-download even if files exist in cache

    Returns:
        list: List of paths to downloaded parquet files

    Raises:
        ImportError: If optional OLS dependencies are not installed
        Exception: If download fails
    """
    if not OLS_AVAILABLE:
        raise ImportError(
            "Optional OLS dependencies (pooch, rdflib, requests) are required for download_ontology_cache(). "
            "Install them with: pip install 'sdrf-pipelines[ontology]'"
        )
    # Determine which files to download
    if ontologies is None:
        files_to_download = ONTOLOGY_FILES
    else:
        files_to_download = [f"{ont.lower()}.parquet" for ont in ontologies]
        # Validate that requested ontologies exist in registry
        invalid = [f for f in files_to_download if f not in ONTOLOGY_REGISTRY]
        if invalid:
            available = [f.replace(".parquet", "") for f in ONTOLOGY_FILES]
            raise ValueError(f"Unknown ontologies: {invalid}. Available: {', '.join(available)}")

    # Create custom pooch instance if cache_dir is specified
    if cache_dir:
        _version = _parse_version_to_branch(__version__)

        downloader = pooch.create(
            path=cache_dir,
            base_url=f"https://raw.githubusercontent.com/bigbio/sdrf-pipelines/{_version}/data/ontologies/",
            registry=ONTOLOGY_REGISTRY,
        )
    else:
        downloader = ONTOLOGY_POOCH

    # Download files
    downloaded_files = []
    for filename in files_to_download:
        try:
            logger.info(f"Downloading ontology file: {filename}")
            # Use progressbar=False to avoid cluttering output
            # If force=True, delete the file first to trigger re-download
            if force:
                cached_path = downloader.path / filename
                if cached_path.exists():
                    cached_path.unlink()
            file_path = downloader.fetch(filename, progressbar=False)
            downloaded_files.append(file_path)
            logger.info(f"Successfully cached: {file_path}")
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            raise RuntimeError(
                f"Failed to download ontology file {filename} from GitHub. "
                f"Check your internet connection and verify the file exists at: "
                f"{downloader.base_url}{filename}"
            ) from e

    return downloaded_files


def get_cache_parquet_files() -> tuple[list[str], list[str]] | None:
    """
    Get cached ontology parquet files from pooch cache or local development directory.
    If no cache exists, attempts to download files from GitHub using pooch.

    Returns:
        tuple: A tuple containing (list of parquet file paths, list of unique ontology names),
               or None if cache cannot be obtained and download fails
    """
    parquet_files = []

    # 1. Try pooch cache directory
    if ONTOLOGY_POOCH is not None:
        cache_path = Path(ONTOLOGY_POOCH.path)
        if cache_path.exists():
            parquet_files = [str(f) for f in cache_path.glob("*.parquet")]
            if parquet_files:
                logger.info(f"Using cached ontology files from pooch cache ({len(parquet_files)} files)")

    # 2. Try local development directory
    if not parquet_files:
        local_dev_dir = Path(__file__).parent.parent.parent.parent / "data" / "ontologies"
        if local_dev_dir.exists():
            parquet_files = [str(f) for f in local_dev_dir.glob("*.parquet")]
            if parquet_files:
                logger.info(f"Using ontology files from local development directory ({len(parquet_files)} files)")

    # 3. Download from GitHub if no local cache exists
    if not parquet_files:
        logger.info("No cached ontology files found. Downloading from GitHub...")
        try:
            parquet_files = download_ontology_cache()
            logger.info(f"Successfully downloaded {len(parquet_files)} ontology files")
        except Exception as e:
            logger.error(f"Failed to download ontology cache: {e}")
            raise RuntimeError(
                "No ontology cache files found locally and download from GitHub failed. "
                "Ensure you have internet connectivity."
            ) from e

    # Load all parquets and extract unique ontology names
    try:
        df = pd.concat([pd.read_parquet(f, engine="fastparquet") for f in parquet_files], ignore_index=True)
    except Exception as e:
        logger.error(f"Failed to read parquet files: {e}")
        raise RuntimeError(f"Failed to read cached ontology files: {e}") from e

    if df is None or df.empty:
        logger.warning("The parquet files found do not contain valid ontologies.")
        return None

    ontologies = df.ontology.unique().tolist()
    return parquet_files, ontologies


def get_obo_accession(uri: str) -> str | None:
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


def read_owl_file(ontology_file: str, ontology_name=None) -> list[dict[str, str]]:
    """
    Reads an OWL file and returns a list of OlsTerms

    Parameters:
        ontology_file (str): The name of the ontology file
        ontology_name (str): The name of the ontology

    Returns:
        list: A list of dictionaries containing the ontology terms

    Raises:
        ImportError: If optional OLS dependencies are not installed
    """
    if not OLS_AVAILABLE:
        raise ImportError(
            "Optional OLS dependencies (pooch, rdflib, requests) are required for read_owl_file(). "
            "Install them with: pip install 'sdrf-pipelines[ontology]'"
        )
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
    terms_info = [term for term in terms_info if "label" in term and "accession" in term]
    return terms_info


def read_obo_file(ontology_file: str, ontology_name=None) -> list[dict[str, str]]:
    """
    Reads an OBO file and returns a list of OlsTerms

    Parameters:
        ontology_file (str): The name of the ontology file
        ontology_name (str): The name of the ontology

    Returns:
        list: A list of dictionaries containing the ontology terms
    """

    def split_terms(content_str: str) -> list[str]:
        return content_str.split("[Term]")[1:]  # Skip the header and split by [Term]

    def get_ontology_name(content_str: str) -> str | None:
        lines = content_str.split("\n")
        for line in lines:
            if line.startswith("ontology:"):
                return line.split("ontology:")[1].strip()
        return None

    def parse_term(term: str, ontology_name_param: str) -> dict[str, str]:
        term_info = {}
        lines = term.strip().split("\n")
        for line in lines:
            if line.strip().startswith("id:"):
                term_info["accession"] = line.split("id:")[1].strip()
                term_info["ontology"] = ontology_name_param
            elif line.strip().startswith("name:"):
                term_info["label"] = line.split("name:")[1].strip()
        return term_info

    with open(ontology_file, encoding="utf-8") as file:
        content = file.read()

    terms = split_terms(content)
    ontology_name = get_ontology_name(content) if ontology_name is None else ontology_name
    terms_info = [parse_term(term, ontology_name) for term in terms]

    return terms_info


class OlsClient:
    def __init__(
        self,
        ols_base: str | None = None,
        ontology: str | None = None,
        field_list: list[str] | None = None,
        query_fields: list[str] | None = None,
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

        Raises:
            ImportError: If optional OLS dependencies are not installed
        """
        if not OLS_AVAILABLE:
            raise ImportError(
                "Optional OLS dependencies (pooch, rdflib, requests) are required for OlsClient. "
                "Install them with: pip install 'sdrf-pipelines[ontology]'"
            )
        self.base = (ols_base if ols_base else OLS).rstrip("/")
        self.session = requests.Session()
        self.use_cache = use_cache

        self.ontology = ontology if ontology else None
        self.field_list = field_list if field_list else None
        self.query_fields = query_fields if query_fields else None

        self.ontology_suggest = self.base + API_SUGGEST
        self.ontology_select = self.base + API_SELECT
        self.ontology_search = self.base + API_SEARCH
        self.ontology_term = self.base + API_TERM
        self.ontology_ancestors = self.base + API_ANCESTORS

        # Initialize cache variables
        self._cached_df: pd.DataFrame | None = None
        self._ontology_cache: dict[str, pd.DataFrame] = {}

        if use_cache:
            cache_result = get_cache_parquet_files()
            if cache_result is None:
                logger.info("No cached ontology files found. Falling back to OLS API.")
                self.use_cache = False
            else:
                self.parquet_files, self.ontologies = cache_result

    @staticmethod
    def build_ontology_index(ontology_file: str, output_file: str | None = None, ontology_name: str | None = None):
        """
        Builds an index from an ontology file in the OBO format.
        The output file is a parquet file containing only three columns:
        - the accession of the term in the form of ONTOLOGY:NUMBER (e.g. GO:0000001) its name and number.
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

        # Use pandas with fastparquet to write parquet file
        df.to_parquet(output_file, engine="fastparquet", compression="gzip", index=False)
        logger.info("Index has finished, output file: %s", output_file)

    def besthit(self, name, **kwargs) -> dict[str, str] | None:
        """
        select a first element of the /search API response
        """
        search_resp = self.search(name, **kwargs)
        if search_resp:
            return search_resp[0]

        return None

    def get_term(self, ontology: str, iri: str) -> dict[str, str]:
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

    def search(self, term: str, ontology: str | None = None, exact=True, use_ols_cache_only: bool = False, **kwargs):
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
            try:
                terms = self.ols_search(term, ontology=ontology, exact=exact, **kwargs)
                if terms is None and self.use_cache:
                    terms = self.cache_search(term, ontology)
            except requests.exceptions.ConnectionError as e:
                logger.warning("Connection error during OLS search: %s", e)
                if self.use_cache:
                    logger.info("Falling back to cache search due to connection error")
                    terms = self.cache_search(term, ontology)
                else:
                    logger.error("Cache is not enabled, cannot fall back to cache search")
                    raise
            except Exception as e:
                logger.error("Error during OLS search: %s", e)
                if self.use_cache:
                    logger.info("Falling back to cache search due to error")
                    terms = self.cache_search(term, ontology)
                else:
                    raise
        return terms

    def _perform_ols_search(
        self, params: dict[str, Any], name: str, exact: bool, retry_num: int = 0
    ) -> list[dict[str, str]]:
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
                logger.error("OLS search term %s error, retry number %s", name, retry_num)
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
        except requests.exceptions.ConnectionError as e:
            logger.exception("Connection error during OLS search: %s", e)
            if retry_num < 10:
                logger.info("Retrying OLS search (attempt %s/10)...", retry_num + 1)
                return self._perform_ols_search(params, name, exact, retry_num + 1)
            else:
                logger.error("Max retry attempts reached.  OLS search failed.")
                raise
        except Exception as ex:
            logger.exception("OLS error searching term %s. Error: %s", name, ex)
        return []

    def ols_search(
        self,
        name: str,
        query_fields=None,
        ontology: str | None = None,
        field_list=None,
        children_of=None,
        exact: bool = False,
        bytype: str = "class",
        rows: int = 10,
        num_retries: int = 10,
        start: int = 0,
    ) -> list[dict[str, str]]:
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
        else:
            params["exact"] = "off"

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

    def cache_search(self, term: str, ontology: str | None, full_search: bool = False) -> list[dict[str, str]]:
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

        # Build cached DataFrame on first use
        if self._cached_df is None:
            self._cached_df = pd.concat(
                [pd.read_parquet(f, engine="fastparquet") for f in self.parquet_files], ignore_index=True
            )
            # Ensure all fields are strings
            self._cached_df["accession"] = self._cached_df["accession"].astype(str)
            self._cached_df["label"] = self._cached_df["label"].astype(str)
            self._cached_df["ontology"] = self._cached_df["ontology"].astype(str)

        # Get or create per-ontology filtered cache
        if ontology is not None:
            ontology_key = ontology.lower()
            if ontology_key not in self._ontology_cache:
                self._ontology_cache[ontology_key] = self._cached_df[
                    self._cached_df["ontology"].str.lower() == ontology_key
                ]
            df = self._ontology_cache[ontology_key]
        else:
            df = self._cached_df

        # Filter for case-insensitive search
        df = df[df["label"].str.lower() == term.lower()]

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

    def clear_cache(self) -> None:
        """
        Clear the cached DataFrames to free memory or force reload.
        """
        self._cached_df = None
        self._ontology_cache.clear()
