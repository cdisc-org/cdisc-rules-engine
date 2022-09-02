from . import DictionaryTypes, TermsFactoryInterface
from .abstract_factory import AbstractTermsFactory
from cdisc_rules_engine.services.data_services import BaseDataService


def get_dictionary_terms_from_folder(
    data_service: BaseDataService,
    dictionary_type: DictionaryTypes,
    dictionaries_directory: str,
):

    factory: TermsFactoryInterface = AbstractTermsFactory(data_service).get_service(
        dictionary_type.value
    )
    terms: dict = factory.install_terms(dictionaries_directory)
    return terms
