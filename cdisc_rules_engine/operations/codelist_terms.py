from numpy import nan
import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import (
    MissingDataError,
    RuleExecutionError,
)
from cdisc_rules_engine.services import logger


class CodelistTerms(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        if (
            self.params.ct_package_type
            and self.params.ct_version
            and self.params.codelist_code
            and self.params.ct_version in self.evaluation_dataset
            and (self.params.term_code or self.params.term_value)
        ):
            return self._handle_multiple_versions()
        elif self.params.codelists:
            return self._handle_single_version()

    def _handle_multiple_versions(self) -> pd.Series:
        if self.params.term_code and self.params.term_value:
            raise RuleExecutionError(
                "Both term_code and term_value cannot be specified at the same time."
            )
        elif self.params.term_code:
            left_on = self.params.term_code
            right_on = "term_code"
            target = "term_value"
        elif self.params.term_value:
            left_on = self.params.term_value
            right_on = "term_value"
            target = "term_code"

        ct_versions = self.evaluation_dataset[self.params.ct_version]
        unique_ct_versions = ct_versions.unique()
        ct_data = self.library_metadata.build_ct_terms(
            self.params.ct_package_type, unique_ct_versions
        )
        ct_df = self.evaluation_dataset.__class__.from_dict(ct_data)
        if self.params.codelist_code in self.evaluation_dataset.columns:
            result = self.evaluation_dataset.merge(
                ct_df.data,
                left_on=(
                    self.params.ct_version,
                    self.params.codelist_code,
                    self.evaluation_dataset[left_on].astype(str).str.lower(),
                ),
                right_on=(
                    "version",
                    "codelist_code",
                    ct_df[right_on].astype(str).str.lower(),
                ),
                how="left",
            ).replace(nan, None)
        else:
            codelist = ct_df[ct_df["codelist_code"] == self.params.codelist_code]
            result = self.evaluation_dataset.merge(
                codelist,
                left_on=(
                    self.params.ct_version,
                    self.evaluation_dataset[left_on].astype(str).str.lower(),
                ),
                right_on=("version", codelist[right_on].astype(str).str.lower()),
                how="left",
            ).replace(nan, None)
        return result[target]

    def _handle_single_version(self) -> pd.Series:
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
            elif not ct_packages:
                raise MissingDataError(
                    "CT package data is not populated. "
                    "A valid define.xml file or -ct command is required to execute."
                )
            else:
                ct_package_data = next(
                    (
                        pkg
                        for name, pkg in ct_packages.items()
                        if name != "extensible" and not name.startswith("define-xml")
                    )
                )
        except AttributeError as e:
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
