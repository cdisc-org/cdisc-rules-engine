import os
from collections import defaultdict
from io import BytesIO

from cdisc_rules_engine.models.dictionaries import TermsFactoryInterface
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.data_services import BaseDataService
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

    def __init__(self, data_service: BaseDataService):
        self.__data_service = data_service
        self.__file_name_model_map: dict = {
            WhodrugFileNames.DD_FILE_NAME.value: DrugDictionary,
            WhodrugFileNames.DDA_FILE_NAME.value: AtcClassification,
            WhodrugFileNames.INA_FILE_NAME.value: AtcText,
        }

    def install_terms(
        self,
        directory_path: str,
    ) -> dict:
        """
        Accepts directory path and creates
        term records for each line.

        Returns a mapping like:
        {
            “entity_type_1”: [<term obj>, <term obj>, ...],
            “entity_type_2”: [<term obj>, <term obj>, ...],
            ...
        }
        """
        logger.info(f"Installing WHODD terms from directory {directory_path}")

        code_to_term_map = defaultdict(list)

        # for each whodrug file in the directory:
        for file_name in self.__file_name_model_map:
            # check if the file exists
            file_path: str = get_dictionary_path(directory_path, file_name)
            if not os.path.exists(file_path):
                logger.warning(
                    f"File {file_name} does not exist in directory {directory_path}"
                )
                continue

            # create term objects
            file_data = self.__data_service.read_data(file_path, read_mode="rb").read()
            res = self.parse_terms_dictionary(file_name, file_data)
            code_to_term_map.update(res)

        return code_to_term_map

    def parse_terms_dictionary(
        self, file_name: str, file_contents: bytes, **kwargs
    ) -> dict:
        """Create terms from single file data"""
        model_class: BaseWhoDrugTerm = self.__file_name_model_map[file_name]
        io_data = BytesIO(file_contents)
        terms = self.__create_term_objects(model_class, io_data)
        return terms

    @staticmethod
    def __create_term_objects(model_class: BaseWhoDrugTerm, file_data: BytesIO):
        code_to_term_map = defaultdict(list)
        for line in file_data:
            term_obj: BaseWhoDrugTerm = model_class.from_txt_line(decode_line(line))
            code_to_term_map[term_obj.type].append(term_obj)
        return code_to_term_map
