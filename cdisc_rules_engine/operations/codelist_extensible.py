import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class CodelistExtensible(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a Series containing a boolean indicating if the specified codelist is extensible.
        """
        codelist = self.params.codelist
        ct_package_data = next(
            iter(self.library_metadata._ct_package_metadata.values())
        )
        code_obj = ct_package_data["submission_lookup"].get(codelist, None)
        codelist_id = code_obj.get("codelist")
        is_extensible = False
        if codelist_id in ct_package_data:
            codelist_info = ct_package_data[codelist_id]
            is_extensible = codelist_info.get("extensible")
        return is_extensible
