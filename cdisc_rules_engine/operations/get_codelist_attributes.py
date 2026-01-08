import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dataset import DaskDataset
from jsonpath_ng.ext import parse


def _get_ct_package_dask(row, ct_target, ct_version, standard, substandard):
    if pd.isna(row[ct_version]) or str(row[ct_version]).strip() == "":
        return ""
    target_val = str(row[ct_target]).strip() if pd.notna(row[ct_target]) else ""
    if target_val in ("CDISC", "CDISC CT"):
        std = standard.lower()
        if "tig" in std:
            std = substandard.lower()
        if "adam" in std:
            prefix = "adamct"
        elif "send" in std:
            prefix = "sendct"
        else:
            prefix = "sdtmct"
        pkg = f"{prefix}-{row[ct_version]}"
    else:
        pkg = f"{target_val}-{row[ct_version]}"
    return pkg


class CodeListAttributes(BaseOperation):
    """
    A class for fetching codelist attributes for a trial summary domain.
    Dynamically loads CT packages based on target and version columns in the data.
    """

    def _execute_operation(self):
        return self._get_codelist_attributes()

    def _get_codelist_attributes(self):
        ct_name = "CT_PACKAGE"
        ct_attribute = self.params.ct_attribute
        ct_target = self.params.target
        ct_version = self.params.ct_version
        df = self.params.dataframe

        def get_ct_package(row):
            if pd.isna(row[ct_version]) or str(row[ct_version]).strip() == "":
                return ""
            target_val = str(row[ct_target]).strip() if pd.notna(row[ct_target]) else ""
            if target_val in ("CDISC", "CDISC CT"):
                standard = self.params.standard.lower()
                if "tig" in standard:
                    standard = self.params.standard_substandard.lower()
                if "adam" in standard:
                    prefix = "adamct"
                elif "send" in standard:
                    prefix = "sendct"
                else:
                    prefix = "sdtmct"
                pkg = f"{prefix}-{row[ct_version]}"
            else:
                pkg = f"{target_val}-{row[ct_version]}"
            return pkg

        if isinstance(df, DaskDataset):
            row_packages = df.data.apply(
                _get_ct_package_dask,
                axis=1,
                meta=(None, "object"),
                args=(
                    ct_target,
                    ct_version,
                    self.params.standard,
                    self.params.standard_substandard,
                ),
            )
        else:
            row_packages = df.data.apply(get_ct_package, axis=1)

        if isinstance(df, DaskDataset):
            unique_packages = set(row_packages.compute().unique())
        else:
            unique_packages = set(row_packages.unique())

        unique_packages.discard("")
        ct_cache = self._get_ct_from_library_metadata(
            ct_key=ct_name, ct_val=ct_attribute, ct_packages=list(unique_packages)
        )
        package_to_codelist = {}
        for _, row in ct_cache.iterrows():
            package_to_codelist[row[ct_name]] = row[ct_attribute]
        result = row_packages.apply(
            lambda pkg: package_to_codelist.get(pkg, set()) if pkg else set()
        )
        return result

    def _get_ct_from_library_metadata(
        self, ct_key: str, ct_val: str, ct_packages: list
    ):
        ct_term_maps = []
        for package in ct_packages:
            parts = package.rsplit("-", 3)
            if len(parts) >= 4:
                ct_package_type = parts[0]
                version = "-".join(parts[1:])
                self.library_metadata._load_ct_package_data(ct_package_type, version)
            ct_term_maps.append(
                self.library_metadata.get_ct_package_metadata(package) or {}
            )

        # Convert codelist to dataframe
        ct_result = {ct_key: [], ct_val: []}
        ct_result = self._add_codelist(ct_key, ct_val, ct_term_maps, ct_result)
        return pd.DataFrame(ct_result)

    def _add_codelist(self, ct_key, ct_val, ct_term_maps, ct_result):
        for item in ct_term_maps:
            ct_result[ct_key].append(item.get("package"))
            codes = self._extract_codes_by_attribute(item, ct_val)
            ct_result[ct_val].append(codes)
        return ct_result

    def _extract_codes_by_attribute(
        self, ct_package_data: dict, ct_attribute: str
    ) -> set:
        attribute_name_map = {
            "Codelist CCODE": "$.codelists[*].conceptId",
            "Codelist Value": "$.codelists[*].submissionValue",
            "Term CCODE": "$.codelists[*].terms[*].conceptId",
            "Term Value": "$.codelists[*].terms[*].submissionValue",
            "Term Submission Value": "$.codelists[*].terms[*].submissionValue",
            "Term Preferred Term": "$.codelists[*].terms[*].preferredTerm",
        }
        if ct_attribute not in attribute_name_map:
            raise ValueError(f"Unsupported ct_attribute: {ct_attribute}")
        attributes = set(
            [
                node.value
                for node in parse(attribute_name_map[ct_attribute]).find(
                    ct_package_data
                )
            ]
        )
        return attributes
