import requests
from requests import HTTPError

from sdrf_pipelines.zooma.ols import OlsClient


class OlsTerm:
    def __init__(self, iri: str = None, term: str = None, ontology: str = None) -> None:
        self._iri = iri
        self._term = term
        self._ontology = ontology

    def __str__(self) -> str:
        return f"{self._term} -- {self._ontology} -- {self._iri}"


class SlimOlsClient:
    def __init__(self) -> None:
        super().__init__()
        self._ols_client = OlsClient()

    @staticmethod
    def get_term_from_url(url, page_size: int = 100, ontology: str = None):
        """
        Return a list of terms by ontology
        :param url:
        :param page_size:
        :param ontology:
        :return:
        """
        url += "&" + "size=" + str(page_size)
        response = requests.get(url)
        if response.status_code == 414:
            raise HTTPError("URL do not exist in OLS")
        json_response = response.json()
        old_terms = json_response["_embedded"]["terms"]
        old_terms = list(filter(lambda k: ontology in k["ontology_name"], old_terms))
        return [OlsTerm(x["iri"], x["label"], x["ontology_name"]) for x in old_terms]


class Zooma:
    """
    A Python binding of the Zooma REST API
    (http://data.bioontology.org/documentation)
    """

    BASE_URL = "https://www.ebi.ac.uk/spot/zooma/v2/api/services"

    @staticmethod
    def process_zooma_results(results):
        """
        Get a list of results from a query to Zooma and return a list
        of dictionaries containing the queryValue, confidence and ols_url
        :param results: List of query terms
        :return:
        """

        ontology_terms = []
        for result in results:
            ols_term = {
                "queryValue": result["annotatedProperty"]["propertyValue"],
                "confidence": result["confidence"],
                "ols_url": result["_links"]["olslinks"][0]["href"],
            }
            ontology_terms.append(ols_term)
        return ontology_terms

    def recommender(self, text_or_keywords, **kwargs):
        """
        # https://www.ebi.ac.uk/spot/zooma/docs/api.html

        Recommender provides a set of ontology terms that match the provided text.
        :param text_or_keywords: keyword to search
        :param kwargs: filters for ontologies
        :return:
        """
        endpoint = "/annotate"
        full_url = Zooma.BASE_URL + endpoint
        payload = kwargs
        payload["propertyValue"] = text_or_keywords
        return self._zooma_api_request(full_url, "get", payload)

    def _zooma_api_request(self, url, method, payload=None):
        if payload is None:
            payload = {}

        global r, error_message
        processed_payload = self._process_payload(payload)
        if method == "get":
            r = requests.get(url, params=processed_payload)
        elif method == "post":
            r = requests.post(url, data=processed_payload)
        if r.status_code == 414:
            raise HTTPError("Text is too long.")

        json_response = r.json()

        try:
            # This will raise an HTTPError if the HTTP request returned an
            # unsuccessful status code.
            r.raise_for_status()
        except HTTPError:
            if "errors" in json_response.keys():
                error_messages = json_response["errors"]
                error_message = "\n".join(error_messages)
            elif "error" in json_response.keys():
                error_message = json_response["error"]

            raise HTTPError(error_message)

        return json_response

    @staticmethod
    def process_value(value):
        if type(value) is bool:
            return str(value).lower()
        return value

    def _process_payload(self, payload):
        """
        Turn boolean True to str 'true' and False to str 'false'. Otherwise,
        server will ignore argument with boolean value.
        :param payload:
        :return:
        """
        return {key: self.process_value(value) for key, value in payload.items()}
