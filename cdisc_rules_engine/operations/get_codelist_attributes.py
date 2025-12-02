import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dataset import DaskDataset
from jsonpath_ng.ext import parse


def _get_ct_package_dask(
    row, ct_target, ct_version, ct_packages, standard, substandard
):
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
    return pkg if pkg in ct_packages else ""


class CodeListAttributes(BaseOperation):
    """
    A class for fetching codelist attributes for a trial summary domain.
    """

    def _execute_operation(self):
        """
        Executes the operation to fetch codelist attributes for a trial
        summary (TS) domain.

        Returns:
            pd.Series: A Series of lists containing codelist, where each list
                represents the codelist package and version.
                The length of the Series is equal to the length of the given
                dataframe.
        """
        return self._get_codelist_attributes()

    def _get_codelist_attributes(self):
        """
        Fetches codelist for a given codelist package and version from the TS
        dataset.
        Returns it as a Series of lists like:
          0    ["STUDYID", "DOMAIN", ...]
          1    ["STUDYID", "DOMAIN", ...]
          2    ["STUDYID", "DOMAIN", ...]
          ...

        pd.Series: A Series of lists containing codelist, where each list
            represents the codelist package and version.
            The length of the Series is equal to the length of the given
            dataframe.
        """

        # 1.0 get input variables
        # -------------------------------------------------------------------
        ct_name = "CT_PACKAGE"  # a column for controlled term package names
        # Get controlled term attribute column name specified in rule
        ct_attribute = self.params.ct_attribute
        ct_target = self.params.target
        ct_version = self.params.ct_version
        ct_packages = self.params.ct_packages
        df = self.params.dataframe
        # 2.0 build codelist from cache
        # -------------------------------------------------------------------
        ct_cache = self._get_ct_from_library_metadata(
            ct_key=ct_name, ct_val=ct_attribute
        )

        def get_ct_package(row):
            if pd.isna(row[ct_version]) or str(row[ct_version]).strip() == "":
                return ""
            target_val = str(row[ct_target]).strip() if pd.notna(row[ct_target]) else ""
            # Handle CDISC CT packages
            if target_val in ("CDISC", "CDISC CT"):
                standard = self.params.standard.lower()
                if "tig" in standard:
                    # use substandard for relevant TIG CT
                    standard = self.params.standard_substandard.lower()
                if "adam" in standard:
                    prefix = "adamct"
                elif "send" in standard:
                    prefix = "sendct"
                else:
                    prefix = "sdtmct"
                pkg = f"{prefix}-{row[ct_version]}"
            else:
                # Handle external codelists
                pkg = f"{target_val}-{row[ct_version]}"
            return pkg if pkg in ct_packages else ""

        if isinstance(df, DaskDataset):
            row_packages = df.data.apply(
                _get_ct_package_dask,
                axis=1,
                meta=(None, "object"),
                args=(
                    ct_target,
                    ct_version,
                    ct_packages,
                    self.params.standard,
                    self.params.standard_substandard,
                ),
            )
        else:
            row_packages = df.data.apply(get_ct_package, axis=1)
        package_to_codelist = {}
        for _, row in ct_cache.iterrows():
            package_to_codelist[row[ct_name]] = row[ct_attribute]
        result = row_packages.apply(
            lambda pkg: package_to_codelist.get(pkg, set()) if pkg else set()
        )
        return result

    def _get_ct_from_library_metadata(self, ct_key: str, ct_val: str):
        """
        Retrieves the codelist information from the cache based on the given
        ct_key and ct_val.

        Args:
            ct_key (str): The key for identifying the codelist.
            ct_val (str): The value associated with the codelist.

        Returns:
            pd.DataFrame: A DataFrame containing the codelist information
            retrieved from the cache.
        """
        ct_packages = self.params.ct_packages
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

        # convert codelist to dataframe
        ct_result = {ct_key: [], ct_val: []}
        ct_result = self._add_codelist(ct_key, ct_val, ct_term_maps, ct_result)
        return pd.DataFrame(ct_result)

    def _get_ct_from_dataset(self, ct_key: str, ct_val: str):
        """
        Retrieves the codelist information from the dataset based on the given
        ct_key and ct_val.

        Args:
            ct_key (str): The key for identifying the codelist.
            ct_val (str): The value associated with the codelist.

        Returns:
            pd.DataFrame: A DataFrame containing the codelist information
            retrieved from the dataset.
        """
        ct_packages = self.params.ct_packages
        # get attribute variable specified in rule
        ct_attribute = self.params.ct_attribute

        ct_target = self.params.target  # target variable specified in rule
        ct_version = self.params.ct_version  # controlled term version
        if ct_attribute == "Term CCODE":
            ct_attribute = "TSVALCD"
        sel_cols = [ct_target, ct_version, ct_attribute, ct_key]

        # get dataframe from dataset records
        df = self.params.dataframe

        # add CT_PACKAGE column
        df[ct_key] = df.data.apply(
            lambda row: (
                "sdtmct-" + row[ct_version]
                if row[ct_target] is not None
                and row[ct_target] in ("CDISC", "CDISC CT")
                else row[ct_target] + "-" + row[ct_version]
            ),
            axis=1,
        )

        # select records
        if isinstance(df, DaskDataset):
            filtered_df = df.filter_by_value(ct_key, ct_packages)
            df_sel = filtered_df.loc[:, sel_cols]
        else:
            df_sel = df[(df[ct_key].isin(ct_packages))].loc[:, sel_cols]
        # group the records
        result = df_sel.groupby(ct_key)[ct_attribute].unique().reset_index()
        result = result.rename(columns={ct_attribute: ct_val})
        return result

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
