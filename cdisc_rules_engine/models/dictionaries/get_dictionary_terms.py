from . import DictionaryTypes, TermsFactoryInterface
from .abstract_factory import AbstractTermsFactory
from cdisc_rules_engine.services.data_services import BaseDataService


def extract_dictionary_terms(
    data_service: BaseDataService,
    dictionary_type: DictionaryTypes,
    dictionaries_directory: str,
) -> dict:
    """Extract dictionary terms from provided directory"""
    factory: TermsFactoryInterface = AbstractTermsFactory(data_service).get_service(
        dictionary_type.value
    )
    terms: dict = factory.install_terms(dictionaries_directory)
    return terms
