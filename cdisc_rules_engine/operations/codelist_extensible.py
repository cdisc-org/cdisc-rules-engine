from numpy import nan
import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


class CodelistExtensible(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a Series containing a boolean indicating if the specified codelist is extensible.
        """
        if (
            self.params.ct_package_type
            and self.params.ct_version
            and self.params.codelist_code
            and self.params.ct_version in self.evaluation_dataset
        ):
            return self._handle_multiple_versions()
        elif self.params.codelist:
            return self._handle_single_version()

    def _handle_multiple_versions(self) -> pd.Series:
        ct_versions = self.evaluation_dataset[self.params.ct_version]
        unique_ct_versions = ct_versions.unique()
        ct_data = self.library_metadata.build_ct_lists(
            self.params.ct_package_type, unique_ct_versions
        )
        ct_df = self.evaluation_dataset.__class__.from_dict(ct_data)
        if self.params.codelist_code in self.evaluation_dataset.columns:
            is_extensible = self.evaluation_dataset.merge(
                ct_df.data,
                left_on=(self.params.ct_version, self.params.codelist_code),
                right_on=("version", "codelist_code"),
                how="left",
            ).replace(nan, None)
        else:
            codelist = ct_df[ct_df["codelist_code"] == self.params.codelist_code]
            is_extensible = self.evaluation_dataset.merge(
                codelist,
                left_on=(self.params.ct_version),
                right_on=("version"),
                how="left",
            ).replace(nan, None)
        return is_extensible["extensible"]

    def _handle_single_version(self) -> pd.Series:
        codelist_name = self.params.codelist
        ct_packages = self.library_metadata._ct_package_metadata
        if "define_XML_merged_CT" in ct_packages:
            ct_package_data = ct_packages["define_XML_merged_CT"]
        else:
            ct_package_data = next(
                (pkg for name, pkg in ct_packages.items() if name != "extensible")
            )
        try:
            codelist = next(
                iter(
                    [
                        codelist
                        for codelist in ct_package_data.get("codelists", [])
                        if codelist.get("submissionValue") == codelist_name
                    ]
                )
            )
        except StopIteration:
            raise MissingDataError(f"Codelist '{codelist_name}' not found in metadata")
        is_extensible = codelist.get("extensible")
        return is_extensible
