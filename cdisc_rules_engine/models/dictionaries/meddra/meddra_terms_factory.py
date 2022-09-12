from collections import defaultdict
from typing import Dict, List

from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.models.dictionaries.meddra.meddra_file_names import (
    MeddraFileNames,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.interfaces import (
    TermsFactoryInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.utilities.utils import get_dictionary_path


class MedDRATermsFactory(TermsFactoryInterface):
    """
    This class is a factory that accepts file name
    and contents and creates a term record for each line.
    """

    def __init__(self, data_service: DataServiceInterface):
        self.data_service = data_service

    def install_terms(
        self,
        directory_path: str,
    ):
        """
        Create MedDRA dictionary terms from files in directory.
        """
        files = {
            MeddraFileNames.PT.value: TermTypes.PT.value,
            MeddraFileNames.HLT.value: TermTypes.HLT.value,
            MeddraFileNames.LLT.value: TermTypes.LLT.value,
            MeddraFileNames.SOC.value: TermTypes.SOC.value,
            MeddraFileNames.HLGT.value: TermTypes.HLGT.value,
        }

        relationship_files = [
            MeddraFileNames.SOC_HLGT.value,
            MeddraFileNames.HLGT_HLT.value,
            MeddraFileNames.HLT_PT.value,
        ]
        data = {}

        required_files = list(files.keys()) + relationship_files
        if not self.data_service.has_all_files(directory_path, required_files):
            raise MissingDataError(message="Necessary meddra files missing")
        # Load data
        for file_name, data_type in files.items():
            file_path = get_dictionary_path(directory_path, file_name)
            data[data_type] = self.read_data(file_path, data_type)

        # Load relationships
        for file_name in relationship_files:
            data = self.update_relationship_data(directory_path, file_name, data)

        hierarchy = [
            TermTypes.SOC.value,
            TermTypes.HLGT.value,
            TermTypes.HLT.value,
            TermTypes.PT.value,
            TermTypes.LLT.value,
        ]
        for i, term_type in enumerate(hierarchy):
            if i == 0:
                continue
            for term in data[term_type].values():
                parent_type = hierarchy[i - 1]
                if term.parent_code:
                    parent: MedDRATerm = data[parent_type][term.parent_code]
                    term.parent_term = parent.term
                    term.code_hierarchy = f"{parent.code_hierarchy}/{term.code}"
                    term.term_hierarchy = f"{parent.term_hierarchy}/{term.term}"

        return self._flatten_data(data)

    @staticmethod
    def _flatten_data(
        terms: Dict[str, Dict[str, MedDRATerm]]
    ) -> Dict[str, List[MedDRATerm]]:
        """
        Used to convert hierarchical MedDRA terms to uniform
        interface format.
        """
        return_dict = defaultdict(list)
        for group, terms_values in terms.items():
            return_dict[group].extend(terms_values.values())
        return return_dict

    def read_data(self, file_path, data_type: str) -> dict:
        """
        Parse file and generate appropriate MedDRATerms
        """
        parser_map: dict = {
            TermTypes.PT.value: self._parse_pt_item,
            TermTypes.HLT.value: self._parse_hlt_item,
            TermTypes.LLT.value: self._parse_llt_item,
            TermTypes.SOC.value: self._parse_soc_item,
            TermTypes.HLGT.value: self._parse_hlgt_item,
        }
        parser = parser_map[data_type]
        data = {}
        with self.data_service.read_data(file_path) as file_data:
            for line in file_data:
                value = parser(line)
                data[value.code] = value
        return data

    def update_relationship_data(
        self, directory_path: str, file_name: str, data: dict
    ) -> dict:
        """
        Iterates over lines in a relationship file, and sets the
        parent relationship on the appropriate term
        """
        origin_type, target_type = file_name.split("_")
        target_type = target_type.split(".")[0]
        file_path = get_dictionary_path(directory_path, file_name)
        with self.data_service.read_data(file_path) as file_data:
            for line in file_data:
                origin_code, target_code = line.split("$")[:2]
                origin_item: MedDRATerm = data[origin_type][origin_code]
                target_item: MedDRATerm = data[target_type][target_code]
                target_item.set_parent(origin_item)

        return data

    def _parse_pt_item(self, item: str) -> MedDRATerm:
        """
        Parses a row from pt.asc and creates a MedDRATerm
        """
        item = item.strip("$")
        values = item.split("$")
        return MedDRATerm(
            {
                "code": values[0],
                "term": values[1],
                "parentCode": values[3],
                "type": TermTypes.PT.value,
            }
        )

    def _parse_hlt_item(self, item: str) -> MedDRATerm:
        """
        Parses a row from hlt.asc and creates a MedDRATerm
        """
        item = item.strip("$")
        values = item.split("$")
        return MedDRATerm(
            {
                "code": values[0],
                "term": values[1],
                "type": TermTypes.HLT.value,
            }
        )

    def _parse_llt_item(self, item: str) -> MedDRATerm:
        """
        Parses a row from llt.asc and creates a MedDRATerm
        """
        item = item.strip("$")
        values = item.split("$")
        return MedDRATerm(
            {
                "code": values[0],
                "term": values[1],
                "type": TermTypes.LLT.value,
                "parentCode": values[2],
            }
        )

    def _parse_hlgt_item(self, item: str) -> MedDRATerm:
        """
        Parses a row from hlgt.asc and creates a MedDRATerm
        """
        item = item.strip("$")
        values = item.split("$")
        return MedDRATerm(
            {
                "code": values[0],
                "term": values[1],
                "type": TermTypes.HLGT.value,
            }
        )

    def _parse_soc_item(self, item: str) -> MedDRATerm:
        """
        Parses a row from soc.asc and creates a MedDRATerm
        """
        item = item.strip("$")
        values = item.split("$")
        return MedDRATerm(
            {
                "code": values[0],
                "term": values[1],
                "type": TermTypes.SOC.value,
                "abbreviation": values[2],
                "codeHierarchy": values[0],
                "termHierarchy": values[1],
            }
        )
