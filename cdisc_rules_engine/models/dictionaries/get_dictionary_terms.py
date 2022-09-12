from cdisc_rules_engine.interfaces import DataServiceInterface, TermsFactoryInterface

from . import DictionaryTypes
from .abstract_factory import AbstractTermsFactory


def extract_dictionary_terms(
    data_service: DataServiceInterface,
    dictionary_type: DictionaryTypes,
    dictionaries_directory: str,
) -> dict:
    """Extract dictionary terms from provided directory"""
    factory: TermsFactoryInterface = AbstractTermsFactory(data_service).get_service(
        dictionary_type.value
    )
    terms: dict = factory.install_terms(dictionaries_directory)
    return terms
