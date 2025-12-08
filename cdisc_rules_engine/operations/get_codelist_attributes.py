import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dataset import DaskDataset
from jsonpath_ng.ext import parse


def _get_ct_package_dask(row, ct_target, ct_version, standard, substandard):
    """Helper function to construct CT package name for Dask apply."""
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
        """
        Executes the operation to fetch codelist attributes for a trial
        summary (TS) domain.

        Returns:
            pd.Series: A Series of sets containing codelist attributes, where each set
                represents the attributes for the CT package and version specified in that row.
                The length of the Series is equal to the length of the given dataframe.
        """
        return self._get_codelist_attributes()

    def _get_codelist_attributes(self):
        """
        Fetches codelist attributes for CT packages dynamically determined from the data.

        The operation:
        1. Constructs CT package names from target and version columns for each row
        2. Loads only the unique CT packages referenced in the data
        3. Returns attributes for each row based on its specific CT package

        Returns:
            pd.Series: A Series of sets containing codelist attributes, where each set
                represents the attributes for the CT package and version specified in that row.
        """
        # Get input variables
        ct_name = "CT_PACKAGE"  # column name for CT package identifiers
        ct_attribute = self.params.ct_attribute
        ct_target = self.params.target
        ct_version = self.params.ct_version
        df = self.params.dataframe

        # Construct CT package name for each row
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
            return pkg

        # Get CT package for each row
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

        # Get unique CT packages that are actually referenced in the data
        if isinstance(df, DaskDataset):
            unique_packages = set(row_packages.compute().unique())
        else:
            unique_packages = set(row_packages.unique())

        # Remove empty strings from the set
        unique_packages.discard("")

        # Build cache from only the packages referenced in the data
        ct_cache = self._get_ct_from_library_metadata(
            ct_key=ct_name, ct_val=ct_attribute, ct_packages=list(unique_packages)
        )

        # Build mapping from package to attributes
        package_to_codelist = {}
        for _, row in ct_cache.iterrows():
            package_to_codelist[row[ct_name]] = row[ct_attribute]

        # Map each row's package to its attributes
        result = row_packages.apply(
            lambda pkg: package_to_codelist.get(pkg, set()) if pkg else set()
        )
        return result

    def _get_ct_from_library_metadata(
        self, ct_key: str, ct_val: str, ct_packages: list
    ):
        """
        Retrieves the codelist information from the cache based on the given
        ct_key, ct_val, and list of CT packages to load.

        Args:
            ct_key (str): The key for identifying the codelist (column name).
            ct_val (str): The value associated with the codelist (attribute name).
            ct_packages (list): List of CT package names to load.

        Returns:
            pd.DataFrame: A DataFrame containing the codelist information
                retrieved from the cache.
        """
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
        """Helper to add codelist data to result dictionary."""
        for item in ct_term_maps:
            ct_result[ct_key].append(item.get("package"))
            codes = self._extract_codes_by_attribute(item, ct_val)
            ct_result[ct_val].append(codes)
        return ct_result

    def _extract_codes_by_attribute(
        self, ct_package_data: dict, ct_attribute: str
    ) -> set:
        """
        Extracts codes/values from CT package data based on the specified attribute.

        Args:
            ct_package_data (dict): CT package metadata dictionary.
            ct_attribute (str): Attribute to extract (e.g., "Term CCODE", "Codelist Value").

        Returns:
            set: Set of attribute values extracted from the CT package.
        """
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
