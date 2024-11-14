import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


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
        # TODO uses 1st map only; may have multiple specified by defineXML, could merge them to keep current logic
        codelists = self.params.codelists
        codelist_level = self.params.level
        check = self.params.returntype
        codes = []
        ct_package_data = next(
            iter(self.library_metadata._ct_package_metadata.values())
        )
        for codelist in codelists:
            codes.append(ct_package_data["submission_lookup"].get(codelist, []))
        values = []

        for code_obj in codes:
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
