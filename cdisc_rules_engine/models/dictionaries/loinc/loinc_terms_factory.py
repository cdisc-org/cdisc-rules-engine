from io import StringIO
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.loinc.loinc_term import LoincTerm
from cdisc_rules_engine.interfaces import (
    TermsFactoryInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.utilities.utils import get_dictionary_path, decode_line
import csv


class LoincTermsFactory(TermsFactoryInterface):
    """
    This class is a factory that accepts file name
    and contents and creates a term record for each line.
    """

    def __init__(self, data_service: DataServiceInterface, **kwargs):
        self.data_service = data_service
        self.term_file_path = "Loinc.csv"

    def install_terms(
        self,
        directory_path: str,
    ) -> ExternalDictionary:
        """
        Create LOINC dictionary terms from files in directory.
        """
        if not self.data_service.has_all_files(directory_path, [self.term_file_path]):
            raise MissingDataError(
                message="Loinc dictionary install missing required file: Loinc.csv"
            )

        file_path = get_dictionary_path(directory_path, self.term_file_path)
        data = {}
        current_version = 0.0
        with self.data_service.read_data(file_path) as file:
            headers_read = False
            for bytes_line in file:
                if headers_read:
                    text_line = decode_line(bytes_line)
                    values = next(csv.reader(StringIO(text_line)))
                    if len(values) < 9:
                        return MissingDataError(
                            message="Loinc term found without required fields provided"
                        )
                    term = LoincTerm(
                        loinc_num=values[0].strip().strip('"'),
                        component=values[1].strip().strip('"'),
                        property=values[2].strip().strip('"'),
                        time_aspect=values[3].strip().strip('"'),
                        system=values[4].strip().strip('"'),
                        scale_type=values[5].strip().strip('"'),
                        method_type=values[6].strip().strip('"'),
                        term_class=values[7].strip().strip('"'),
                    )
                    version_last_updated = values[8].strip().strip('"')
                    if version_last_updated:
                        try:
                            current_version = max(
                                current_version, float(version_last_updated)
                            )
                        except ValueError:
                            pass
                    data[term.loinc_num] = term
                headers_read = True
        return ExternalDictionary(data, str(current_version))

    def get_version(self, directory_path) -> str:
        dictionary = self.install_terms(directory_path)
        return dictionary.version
