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
import urllib.parse

import requests

OLS = "https://www.ebi.ac.uk/ols4"

__all__ = ["OlsClient"]

logger = logging.getLogger(__name__)

API_SUGGEST = "/api/suggest"
API_SEARCH = "/api/search"
API_SELECT = "/api/select"
API_TERM = "/api/ontologies/{ontology}/terms/{iri}"
API_ANCESTORS = "/api/ontologies/{ontology}/terms/{iri}/ancestors"


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


class OlsClient:
    def __init__(self, ols_base=None, ontology=None, field_list=None, query_fields=None):
        """
        :param ols_base: An optional, custom URL for the OLS RESTful API.
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
                                start=(rows + (start)),
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
