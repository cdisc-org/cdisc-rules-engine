import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


class DefineCodelists(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a list of codelist values from the define.xml file.
        fxn to be be used when a codelist is extensible to acquire the additional values
        """
        if not self.params.codelists:
            raise MissingDataError("Codelists operation parameter not provided")
        codelists = self.params.codelists
        values = []
        ct_package_data = self.library_metadata._ct_package_metadata.get("extensible")
        if ct_package_data is None:
            raise MissingDataError(
                "Parsed Extensible terms not found in library CT metadata"
            )
        if len(codelists) == 1 and codelists[0] == "ALL":
            return [
                value
                for data in ct_package_data.values()
                for value in data["extended_values"]
            ]

        lookup_map = {name.lower(): name for name in ct_package_data.keys()}
        for codelist in codelists:
            original_key = lookup_map.get(codelist.lower())
            if original_key is None:
                raise MissingDataError(f"Codelist '{codelist}' not found in metadata")
            codelist_data = ct_package_data[original_key]
            values.extend(codelist_data["extended_values"])
        return values
