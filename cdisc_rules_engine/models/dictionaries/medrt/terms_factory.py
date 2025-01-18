from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.interfaces import (
    TermsFactoryInterface,
    DataServiceInterface,
)
from xml.etree import ElementTree

from cdisc_rules_engine.models.dictionaries.medrt.term import MEDRTConcept, MEDRTTerm
from cdisc_rules_engine.utilities.utils import get_dictionary_path


class MEDRTTermsFactory(TermsFactoryInterface):
    """
    This class is a factory that accepts file name
    and contents and creates a term record for each line.
    """

    def __init__(self, data_service: DataServiceInterface, **kwargs):
        self.data_service = data_service
        self.term_file_regex = "Core_MEDRT_.*_DTS.xml"

    def install_terms(
        self,
        directory_path: str,
    ) -> ExternalDictionary:
        """
        Create MEDRT dictionary terms from files in directory.
        """
        data = {}
        version = ""
        file_path = self._get_dictionary_path(directory_path)
        root = self._get_root(file_path)
        version = self._read_version(root)
        # Parse all terms
        for term in root.iter("term"):
            term_obj = self._parse_term(term)
            data[term_obj.code] = term_obj

        # Parse all concepts
        for concept in root.iter("concept"):
            concept_obj = self._parse_concept(concept)
            data[concept_obj.code] = concept_obj

        return ExternalDictionary(data, version=version)

    def _parse_term(self, term_xml: ElementTree) -> MEDRTTerm:
        code = term_xml.find("code").text
        id = term_xml.find("id").text
        return MEDRTTerm(
            code=code,
            id=id,
            status=self._get_element_attribute(term_xml, "status"),
            name=self._get_element_attribute(term_xml, "name"),
        )

    def _parse_concept(self, concept_xml: ElementTree) -> MEDRTConcept:
        params = {
            "name": self._get_element_attribute(concept_xml, "name"),
            "code": self._get_element_attribute(concept_xml, "code"),
            "status": self._get_element_attribute(concept_xml, "status"),
        }

        synonyms = []
        for synonym in concept_xml.iter("synonym"):
            term_code = self._get_element_attribute(synonym, "to_code")
            if term_code:
                synonyms.append(term_code)
        params["synonyms"] = synonyms
        return MEDRTConcept(**params)

    def _get_element_attribute(self, element: ElementTree, attribute_name: str) -> str:
        attribute = element.find(attribute_name)
        if attribute is not None:
            return attribute.text
        return None

    def get_version(self, directory_path) -> str:
        file_path = self._get_dictionary_path(directory_path)
        root = self._get_root(file_path)
        return self._read_version(root)

    def _get_root(self, file_path: str) -> ElementTree:
        with self.data_service.read_data(file_path) as file:
            root = ElementTree.fromstring(file.read())
        return root

    def _read_version(self, root: ElementTree) -> str:
        namespace = next(root.iter("namespace"))
        version_tag = namespace.find("version")
        if version_tag is not None:
            return version_tag.text
        return ""

    def _get_dictionary_path(self, directory_path: str) -> str:
        file_path = self.data_service.get_file_matching_pattern(
            directory_path, self.term_file_regex
        )
        if not file_path:
            raise MissingDataError(
                message="MEDRT dictionary install missing required core xml file."
            )

        return get_dictionary_path(directory_path, file_path)
