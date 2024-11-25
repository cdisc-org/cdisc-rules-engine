from io import StringIO
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.unii.term import UNIITerm
from cdisc_rules_engine.interfaces import (
    TermsFactoryInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.utilities.utils import get_dictionary_path, decode_line
import csv


class UNIITermsFactory(TermsFactoryInterface):
    """
    This class is a factory that accepts file name
    and contents and creates a term record for each line.
    """

    def __init__(self, data_service: DataServiceInterface, **kwargs):
        self.data_service = data_service
        self.term_file_path_pattern = "UNII_Records_*"

    def install_terms(
        self,
        directory_path: str,
    ) -> ExternalDictionary:
        """
        Create LOINC dictionary terms from files in directory.
        """
        file_path = self.data_service.get_file_matching_pattern(
            directory_path, self.term_file_path_pattern
        )
        if not file_path:
            raise MissingDataError(
                message=f"UNII dictionary install missing file matching pattern {self.term_file_path_pattern}"
            )
        current_version = file_path.split("_")[-1].split(".")[0]
        file_path = get_dictionary_path(directory_path, file_path)
        data = {}
        with self.data_service.read_data(file_path) as file:
            headers_read = False
            for bytes_line in file:
                if headers_read:
                    text_line = decode_line(bytes_line)
                    values = next(csv.reader(StringIO(text_line), delimiter="\t"))
                    term = UNIITerm(values[0], values[1])
                    data[term.unii] = term
                headers_read = True
        return ExternalDictionary(data, str(current_version))

    def get_version(self, directory_path) -> str:
        file_path = self.data_service.get_file_matching_pattern(
            directory_path, self.term_file_path_pattern
        )
        if not file_path:
            raise MissingDataError(
                message=f"UNII dictionary install missing file matching pattern {self.term_file_path_pattern}"
            )
        return file_path.split("_")[-1]
