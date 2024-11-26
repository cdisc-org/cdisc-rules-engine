from urllib.parse import quote
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
import requests
from cdisc_rules_engine.interfaces import (
    TermsFactoryInterface,
)
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.snomed.term import SNOMEDTerm


class SNOMEDTermsFactory(TermsFactoryInterface):
    def __init__(self, data_service, **kwargs):
        self.edition = kwargs.get("edition")
        self.version = kwargs.get("version")
        self.term_limit = 100
        self.base_url = (
            kwargs.get("base_url")
            or "https://snowstorm.snomedtools.org/snowstorm/snomed-ct/"
        )

    def install_terms(self, directory_path: str, **kwargs) -> ExternalDictionary:
        if "concepts" not in kwargs:
            return ExternalDictionary({}, "")

        concepts = kwargs.get("concepts", [])

        # Batch concepts by groups of 100 and get concepts
        batches = self._chunk_concept_list(concepts, 100)
        term_lists = [self._get_concepts(concept_ids=batch) for batch in batches]

        # Join lists of terms here
        terms = {}
        for batch in term_lists:
            batch_terms = {term.concept_id: term for term in batch}
            terms.update(batch_terms)

        return ExternalDictionary(terms, self.get_version())

    def get_version(self, directory_path=""):
        return f"MAIN/{self.edition}/{self.version}"

    def _handle_api_error(self, response):
        error_msg = response.json().get("message")
        raise MissingDataError(error_msg)

    def _get_concepts(
        self, published: bool = True, concept_ids: list[str] = []
    ) -> list[SNOMEDTerm]:
        branch_name = quote(self.get_version(), safe="")
        is_published_flag = "true" if published else "false"
        concepts_string = "&conceptIds=".join(concept_ids)
        url = f"{self.base_url}{branch_name}/concepts?isPublished={is_published_flag}"
        url = f"{url}&includeLeafFlag=false&form=inferred&limit={self.term_limit}&conceptIds={concepts_string}"
        response = self._request_concepts(url)
        return [SNOMEDTerm.from_json(term) for term in response.get("items", [])]

    def _request_concepts(self, url):
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "CDISC CORE Engine",
        }
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            self._handle_api_error(response)
            return {}

        return response.json()

    def _chunk_concept_list(self, concepts, terms_per_chunk):
        for i in range(0, len(concepts), terms_per_chunk):
            yield concepts[i : i + terms_per_chunk]
