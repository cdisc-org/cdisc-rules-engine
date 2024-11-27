from collections import defaultdict

from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.interfaces import (
    TermsFactoryInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.utils import get_dictionary_path, decode_line
from .atc_classification import AtcClassification
from .atc_text import AtcText
from .base_whodrug_term import BaseWhoDrugTerm
from .drug_dict import DrugDictionary
from .whodrug_file_names import WhodrugFileNames


class WhoDrugTermsFactory(TermsFactoryInterface):
    """
    This class is a factory that accepts file name
    and contents and creates a term record for each line.
    """

    def __init__(self, data_service: DataServiceInterface, **kwargs):
        self.__data_service = data_service
        self.__file_name_model_map: dict = {
            WhodrugFileNames.DD_FILE_NAME.value: DrugDictionary,
            WhodrugFileNames.DDA_FILE_NAME.value: AtcClassification,
            WhodrugFileNames.INA_FILE_NAME.value: AtcText,
        }

    def get_version(self, directory_path: str) -> str:
        if not self.__data_service.has_all_files(
            directory_path, [WhodrugFileNames.VERSION.value]
        ):
            raise MissingDataError(message="WhoDrug version file missing")
        file_path = get_dictionary_path(directory_path, WhodrugFileNames.VERSION.value)
        with self.__data_service.read_data(file_path) as file_data:
            for bytes_line in file_data:
                value = decode_line(bytes_line)
                month = value[-5:-2]
                year = value[-2:]

                return f"{month.upper()}_20{year}"
        return ""

    def install_terms(self, directory_path: str) -> ExternalDictionary:
        """
        Accepts directory path and creates
        term records for each line.

        Returns an ExternalDictionary containing a version string and
        a mapping of terms witht he following structure:
        {
            “entity_type_1”: [<term obj>, <term obj>, ...],
            “entity_type_2”: [<term obj>, <term obj>, ...],
            ...
        }
        """
        logger.info(f"Installing WHODD terms from directory {directory_path}")

        files_required = list(self.__file_name_model_map.keys())
        if not self.__data_service.has_all_files(directory_path, files_required):
            raise ValueError(
                f"Insufficient files in directory {directory_path}."
                f"Check that all of ({files_required}) exist"
            )

        try:
            version = self.get_version(directory_path)
        except MissingDataError:
            version = ""

        code_to_term_map = defaultdict(dict)
        for dictionary_filename in self.__file_name_model_map:
            file_path: str = get_dictionary_path(directory_path, dictionary_filename)
            self.__create_term_objects_from_file(
                code_to_term_map, dictionary_filename, file_path
            )
        return ExternalDictionary(terms=code_to_term_map, version=version)

    def __create_term_objects_from_file(
        self, code_to_term_map: defaultdict, dictionary_filename: str, file_path: str
    ):
        """
        Creates a list of term objects for each line of the file.
        code_to_term_map is changed by reference.
        """
        model_class: BaseWhoDrugTerm = self.__file_name_model_map[dictionary_filename]

        with self.__data_service.read_data(file_path) as file:
            # create a term object for each line and append it to the mapping
            for bytes_line in file:
                term_obj: BaseWhoDrugTerm = model_class.from_txt_line(
                    decode_line(bytes_line)
                )
                code_to_term_map[term_obj.type][term_obj.get_identifier()] = term_obj
