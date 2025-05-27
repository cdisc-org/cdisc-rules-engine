from os.path import join
from pickle import load
import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


class CodelistExtensible(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a Series containing a boolean indicating if the specified codelist is extensible.
        """
        if (
            self.params.package
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
        ct_data = None
        for ct_version in unique_ct_versions:
            ct_package_version = f"{self.params.package}-{ct_version}"
            ct_package_data = self.library_metadata.get_ct_package_metadata(
                ct_package_version
            )
            if ct_package_data is None:
                file_name = f"{ct_package_version}.pkl"
                with open(join(self.cache_path, file_name), "rb") as f:
                    ct_package_data = load(f)
                    self.library_metadata.set_ct_package_metadata(
                        ct_package_version, ct_package_data
                    )
            ct_lists = [
                {
                    "package": self.params.package,
                    "version": ct_version,
                    "codelist_code": key,
                    "extensible": value["extensible"],
                }
                for key, value in ct_package_data.items()
                if "extensible" in value
            ]
            ct_df = self.evaluation_dataset.__class__.from_records(ct_lists)
            ct_data = ct_data.concat(ct_df) if ct_data else ct_df
        is_extensible = self.evaluation_dataset.merge(
            ct_data.data,
            left_on=(self.params.ct_version, self.params.codelist_code),
            right_on=("version", "codelist_code"),
            how="left",
        )
        return is_extensible["extensible"]

    def _handle_single_version(self) -> pd.Series:
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
