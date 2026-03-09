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
import re
import urllib.parse
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

# Characters that may break OLS exact search (known bug with terms like "SILAC heavy L:13C(6)")
_SPECIAL_CHARS_PATTERN = re.compile(r"[():]")

# Try to import OLS dependencies - these are optional and only needed for ontology validation
try:
    import pooch
    import rdflib
    import requests

    OLS_AVAILABLE = True
except ImportError:
    OLS_AVAILABLE = False

try:
    from sdrf_pipelines import __version__
except ImportError:
    __version__ = "dev"

OLS = "https://www.ebi.ac.uk/ols4"

__all__ = ["OlsClient", "OLS_AVAILABLE"]

logger = logging.getLogger(__name__)


class CacheBackend(ABC):
    """Abstract base class for ontology cache backends."""

    @abstractmethod
    def search(self, term: str, ontology: str | None, full_search: bool = False) -> list[dict[str, str]]:
        """Search for a term in the cache."""
        ...

    @abstractmethod
    def get_ontologies(self) -> list[str]:
        """Return list of available ontology names."""
        ...

    @abstractmethod
    def load_ontology(self, name: str, path: str) -> None:
        """Load a single ontology from a parquet file."""
        ...

    @abstractmethod
    def is_ontology_loaded(self, name: str) -> bool:
        """Check if an ontology has been loaded."""
        ...


class DictBackend(CacheBackend):
    """Dict-based cache backend. No extra dependencies. 0.01ms/query after build."""

    def __init__(self) -> None:
        # {ontology_lower: {label_lower: [{"ontology_name": str, "label": str, "obo_id": str}]}}
        self._index: dict[str, dict[str, list[dict[str, str]]]] = {}
        self._ontology_names: list[str] = []

    def load_ontology(self, name: str, path: str) -> None:
        name_lower = name.lower()
        if name_lower in self._index:
            return
        df = pd.read_parquet(path, engine="fastparquet")
        index: dict[str, list[dict[str, str]]] = {}
        for row in df.itertuples(index=False):
            label_lower = str(row.label).lower()
            entry = {"ontology_name": str(row.ontology), "label": str(row.label), "obo_id": str(row.accession)}
            if label_lower in index:
                index[label_lower].append(entry)
            else:
                index[label_lower] = [entry]
        self._index[name_lower] = index
        if name_lower not in self._ontology_names:
            self._ontology_names.append(name_lower)
        logger.info("DictBackend: loaded %s (%d terms)", name, len(df))

    def is_ontology_loaded(self, name: str) -> bool:
        return name.lower() in self._index

    def get_ontologies(self) -> list[str]:
        return list(self._ontology_names)

    def search(self, term: str, ontology: str | None, full_search: bool = False) -> list[dict[str, str]]:
        term_lower = term.lower()
        if ontology is not None:
            ont_index = self._index.get(ontology.lower())
            if ont_index is None:
                return []
            return list(ont_index.get(term_lower, []))
        if not full_search:
            return []
        # full_search: search across all loaded ontologies
        results = []
        for ont_index in self._index.values():
            results.extend(ont_index.get(term_lower, []))
        return results


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
    "doid.parquet",
    "efo.parquet",
    "envo.parquet",
    "gaz.parquet",
    "hancestro.parquet",
    "mod.parquet",
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
    "bto.parquet": "sha256:7acb89a4347534482f342bf01d31447d1c5f3a49b9def0a9cf1756691a718f98",
    "chebi.parquet": "sha256:868519678979c2ac7f9e6c46844188fb964270d061c786ba82920ad56c29dcfb",
    "cl.parquet": "sha256:e44d510e1c3f51e71b0dfd4ca9f1f61e329a29be028f6441fcb8a01db1cb473a",
    "clo.parquet": "sha256:dc85a1ef7544b43ae71e703e1cedb776f44f93a925ba1af2d1affbc90acbd745",
    "doid.parquet": "sha256:d56bdbe3eda288b9cea6292737d5a76f8668ac9488b59b2c7fe3530720f2a112",
    "efo.parquet": "sha256:cd21bb394f1a370c73c92d3b483fe2a9be7ef5ee5ebeec67210072780c9c20cd",
    "envo.parquet": "sha256:b432b44ca0a080d4d4b96040a1e2d14085e2d26d8397dd0715a0ffa60a835299",
    "gaz.parquet": "sha256:80304c7a05cccc1eeb1dd9a30c1b5c13429fb2926dfa6aa30e1716f1fc52c618",
    "hancestro.parquet": "sha256:b04fb43c0c9642a8df40d3a02cbefa6e7d45c099b0135b540510c80c8fd26129",
    "mod.parquet": "sha256:7a03296251013d7494d0bcf8847f7ef6618c13e65594f0a9df4ef48da4548b12",
    "mondo.parquet": "sha256:735005008a635f7774e61fff0c3775df826d3204ccd0c4048f3ea533681fda56",
    "ncbitaxon.parquet": "sha256:598aabc6e65f38d4f1274c55816f03901b00c206bce32181e641dfeb72da55b1",
    "ncit.parquet": "sha256:3e3d159bcf0ab2f873ece56d2d1569c504eb1b48305ec38e8be56edacd468b2c",
    "pato.parquet": "sha256:979de9c24e5eb484922c04735466e0c400d1d96015a90efb2d67635c33d9a27b",
    "pride.parquet": "sha256:23bd9a67f5b0c58a6a2fc27a0b7ca3776f4cc2aaec38bc69d291d47012590cb3",
    "psi-ms.parquet": "sha256:5bfeb07884032a82598459430c33b28ed23c14b026fa6d2a9721425e4139ab02",
    "uberon.parquet": "sha256:dec2be2f92e626ab79e0c0a34cdb31222f073a87c0504406df2a8e35ca1c728b",
    "unimod.parquet": "sha256:43f95d2b9b0bee4842ac6cf8dcc9f94fba4fbcd784634c2cd7c5e789e752da5c",
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


# Module-level shared state for backend reuse across OlsClient instances
_shared_ontology_map: dict[str, str] | None = None
_shared_dict_backend: DictBackend | None = None


def get_cache_parquet_files() -> dict[str, str] | None:
    """
    Get cached ontology parquet files from pooch cache or local development directory.
    If no cache exists, attempts to download files from GitHub using pooch.

    Returns a mapping of {ontology_name: parquet_path} WITHOUT loading any data.
    Ontology names are derived from filenames (e.g., 'efo.parquet' -> 'efo').

    Returns:
        dict: A mapping of ontology names to parquet file paths,
              or None if no cache files can be found
    """
    parquet_files: list[str] = []

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

    if not parquet_files:
        return None

    # Build {ontology_name: path} mapping from filenames — no data loading
    ontology_map: dict[str, str] = {}
    for fp in parquet_files:
        name = Path(fp).stem.lower()  # e.g., "efo.parquet" -> "efo"
        ontology_map[name] = fp

    logger.info(f"Found {len(ontology_map)} ontology cache files: {', '.join(sorted(ontology_map.keys()))}")
    return ontology_map


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

        # Initialize cache backend (shared across all OlsClient instances)
        self._backend: CacheBackend | None = None
        self._ontology_map: dict[str, str] = {}  # {ontology_name: parquet_path}

        if use_cache:
            global _shared_ontology_map, _shared_dict_backend

            # Reuse shared ontology map if available
            if _shared_ontology_map is not None:
                ontology_map = _shared_ontology_map
            else:
                ontology_map = get_cache_parquet_files()
                _shared_ontology_map = ontology_map

            if ontology_map is None:
                logger.info("No cached ontology files found. Falling back to OLS API.")
                self.use_cache = False
            else:
                self._ontology_map = ontology_map
                # Reuse shared backend
                if _shared_dict_backend is None:
                    _shared_dict_backend = DictBackend()
                    logger.info("Using Dict cache backend")
                self._backend = _shared_dict_backend

    # Mapping from ontology names used in templates to parquet filenames
    # when they differ (e.g., validators use "ms" but the file is "psi-ms.parquet")
    _ONTOLOGY_ALIASES: dict[str, str] = {
        "ms": "psi-ms",
    }

    @property
    def ontologies(self) -> list[str]:
        """Return list of available ontology names from cache files.

        Includes both filename-derived names and known aliases.
        """
        names = list(self._ontology_map.keys())
        # Add alias names (e.g., "ms" for "psi-ms")
        for alias, filename in self._ONTOLOGY_ALIASES.items():
            if filename in self._ontology_map and alias not in names:
                names.append(alias)
        return names

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

    def _normalize_term_for_fuzzy_query(self, term: str) -> str:
        """Replace special characters that break OLS exact search with spaces, collapse spaces."""
        normalized = _SPECIAL_CHARS_PATTERN.sub(" ", term)
        return re.sub(r"\s+", " ", normalized).strip()

    def search(self, term: str, ontology: str | None = None, exact=True, use_ols_cache_only: bool = False, **kwargs):
        """
        Search a term in the OLS.

        Known bug: OLS API exact search returns no results for some valid PRIDE terms
        (e.g. "SILAC heavy L:13C(6)", "SILAC light L:12C(6)") that contain special
        characters like : ( ). When exact search fails and the term has these chars,
        we retry with a normalized query (fuzzy) and filter results by Python exact
        match on the label. If still no match, we fall back to the local parquet cache.

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
                # 1. First try normal exact query (hoping OLS will fix the bug someday)
                terms = self.ols_search(term, ontology=ontology, exact=exact, **kwargs)

                # 2. If empty and term has special chars, retry with normalized fuzzy query
                if (terms is None or len(terms) == 0) and _SPECIAL_CHARS_PATTERN.search(term):
                    normalized = self._normalize_term_for_fuzzy_query(term)
                    fuzzy_results = self.ols_search(normalized, ontology=ontology, exact=False, **kwargs)
                    if fuzzy_results:
                        term_lower = term.lower()
                        exact_matches = [r for r in fuzzy_results if r.get("label", "").lower() == term_lower]
                        if exact_matches:
                            terms = exact_matches
                            logger.debug(
                                "Found term via normalized fuzzy query + exact filter: %s",
                                term,
                            )

                # 3. Fall back to cache when OLS still returns no results
                if (terms is None or len(terms) == 0) and self.use_cache:
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

    def _resolve_parquet_path(self, ontology: str) -> str | None:
        """Resolve an ontology name to its parquet file path, handling aliases."""
        ont_lower = ontology.lower()
        # Direct match (e.g., "efo" -> "efo.parquet")
        path = self._ontology_map.get(ont_lower)
        if path is not None:
            return path
        # Check aliases (e.g., "ms" -> "psi-ms.parquet")
        alias_filename = self._ONTOLOGY_ALIASES.get(ont_lower)
        if alias_filename is not None:
            return self._ontology_map.get(alias_filename)
        return None

    def _ensure_ontology_loaded(self, ontology: str) -> bool:
        """Lazily load an ontology into the backend if not already loaded.

        Returns True if the ontology is available, False otherwise.
        """
        if self._backend is None:
            return False
        ont_lower = ontology.lower()
        if self._backend.is_ontology_loaded(ont_lower):
            return True
        parquet_path = self._resolve_parquet_path(ont_lower)
        if parquet_path is None:
            return False
        self._backend.load_ontology(ont_lower, parquet_path)
        return True

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
        if self._backend is None:
            return []

        if ontology is not None:
            if self._resolve_parquet_path(ontology) is None and not full_search:
                return []
            self._ensure_ontology_loaded(ontology)
        elif full_search:
            # Load all ontologies for full search
            for ont_name in self._ontology_map:
                self._ensure_ontology_loaded(ont_name)

        return self._backend.search(term, ontology, full_search)

    def clear_cache(self) -> None:
        """
        Clear the cache backend and force reload.
        """
        if isinstance(self._backend, DictBackend):
            self._backend = DictBackend()
        else:
            self._backend = None
