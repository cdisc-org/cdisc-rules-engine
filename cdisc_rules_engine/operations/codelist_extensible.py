import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


class CodelistExtensible(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a Series containing a boolean indicating if the specified codelist is extensible.
        """
        codelist = self.params.codelist
        ct_packages = self.library_metadata._ct_package_metadata
        if "define_XML_merged_CT" in ct_packages:
            ct_package_data = ct_packages["define_XML_merged_CT"]
        else:
            ct_package_data = next(
                (pkg for name, pkg in ct_packages.items() if name != "extensible")
            )
        code_obj = ct_package_data["submission_lookup"].get(codelist, None)
        if code_obj is None:
            raise MissingDataError(f"Codelist '{codelist}' not found in metadata")
        codelist_id = code_obj.get("codelist")
        is_extensible = False
        if codelist_id in ct_package_data:
            codelist_info = ct_package_data[codelist_id]
            is_extensible = codelist_info.get("extensible")
        return is_extensible
