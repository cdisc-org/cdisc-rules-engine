import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.services import logger


class CodelistTerms(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a list of codelists
        Both the level of the codelist check (codelist or term level) and
        the type of check (code or value) must be specified.
        A list of appropriate submission values or codes is generated
        using the list from comparator and the codelist map.
        Returns a Series of booleans indicating whether each value is valid.
        """
        codelists = self.params.codelists
        codelist_level = self.params.level
        check = self.params.returntype
        codes = []
        try:
            ct_packages = self.library_metadata._ct_package_metadata
            if "define_XML_merged_CT" in ct_packages:
                ct_package_data = ct_packages["define_XML_merged_CT"]
            else:
                ct_package_data = next(
                    (pkg for name, pkg in ct_packages.items() if name != "extensible")
                )
        except (AttributeError) as e:
            logger.warning(
                "CT package data is not populated: %s "
                "-- a valid define.xml file or -ct command is required to execute",
                e,
            )
        submission_lookup = ct_package_data["submission_lookup"]
        lookup_map = {k.lower(): k for k in submission_lookup.keys()}
        for codelist in codelists:
            original_key = lookup_map.get(codelist.lower())
            if original_key is None:
                raise MissingDataError(f"Codelist '{codelist}' not found in metadata")
            code_obj = submission_lookup[original_key]
            codes.append(code_obj)
        values = []

        for code_obj in codes:
            values.extend(
                self._get_codelist_values(
                    code_obj, ct_package_data, codelist_level, check
                )
            )
        return values

    def _get_codelist_values(
        self, code_obj: dict, ct_package_data: dict, codelist_level: str, check: str
    ) -> list:
        """Extract values from a codelist based on level and check type."""
        values = []
        codelist_id = code_obj.get("codelist")
        if codelist_id in ct_package_data:
            codelist_info = ct_package_data[codelist_id]
            if codelist_level == "codelist":
                if code_obj.get("term") == "N/A":
                    if check == "code":
                        values.append(codelist_id)
                    else:
                        values.append(codelist_info["submissionValue"])
            elif codelist_level == "term":
                terms = codelist_info.get("terms", [])
                for term in terms:
                    if check == "value":
                        values.append(term["submissionValue"])
                    else:
                        values.append(term["conceptId"])
        return values
