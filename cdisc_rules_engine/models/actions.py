from typing import List, Optional, Set, Hashable

from os import path
import pandas as pd
from business_rules.actions import BaseActions, rule_action
from business_rules.fields import FIELD_TEXT

from cdisc_rules_engine.constants import NULL_FLAVORS
from cdisc_rules_engine.constants.metadata_columns import (
    SOURCE_FILENAME,
    SOURCE_ROW_NUMBER,
)
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import ValidationErrorEntity
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor


class COREActions(BaseActions):
    def __init__(
        self,
        output_container: list,
        variable: DatasetVariable,
        dataset_metadata: SDTMDatasetMetadata,
        rule: dict,
        value_level_metadata: list = None,
    ):
        if value_level_metadata is None:
            value_level_metadata = []

        self.output_container = output_container
        self.variable = variable
        self.dataset_metadata = dataset_metadata
        self.rule = rule
        self.value_level_metadata = value_level_metadata

    @rule_action(params={"message": FIELD_TEXT, "target": FIELD_TEXT})
    def generate_record_message(self, message, target=None):
        full_message = f"Error in row {self.record.get('row')} " + message
        if target:
            full_message = full_message + f" Actual value: {self.record.get(target)}"
        self.output_container.append(full_message)

    @rule_action(params={"message": FIELD_TEXT})
    def generate_dataset_error_objects(self, message: str, results: pd.Series):
        # leave only those columns where errors have been found
        rows_with_error = self.variable.dataset.get_error_rows(results)
        target_names: Set[str] = RuleProcessor.extract_target_names_from_rule(
            self.rule,
            self.dataset_metadata.domain,
            self.variable.dataset.columns.tolist(),
        )
        target_names = self._get_target_names_from_list_values(
            target_names, rows_with_error
        )
        if self.value_level_metadata:
            target_names = self.extract_target_names_from_value_level_metadata()
        error_object = self.generate_targeted_error_object(
            target_names, rows_with_error, message
        )
        self.output_container.append(error_object.to_representation())

    @rule_action(params={"message": FIELD_TEXT})
    def generate_single_error(self, message):
        self.output_container.append(message)

    def _get_target_names_from_list_values(self, target_names, rows_with_error):
        expanded_target_names = set(target_names)
        expanded_target_names.update(
            value
            for target in target_names
            if target in rows_with_error
            for candidate_list in rows_with_error[target]
            if isinstance(candidate_list, list)
            for value in candidate_list
            if value in self.variable.dataset.columns
        )
        return expanded_target_names

    def generate_targeted_error_object(
        self, targets: Set[str], data: pd.DataFrame, message: str
    ) -> ValidationErrorContainer:
        """
        Generates a targeted error object.
        Return example:
        {
            "dataset": "ae.xpt",
            "domain": "AE",
            "variables": ["AESTDY", "DOMAIN"],
            "errors": [
                {
                  "dataset": "ae.xpt",
                  "row": 0,
                  "value": {"STUDYID": "Not in dataset"},
                  "uSubjId": "2",
                  "seq": 1,
                },
                {
                  "dataset": "ae.xpt",
                  "row": 1,
                  "value": {"AESTDY": "test", "DOMAIN": "test"},
                  "uSubjId": 7,
                  "seq": 2,
                },
                {
                  "dataset": "ae.xpt",
                  "row": 9,
                  "value": {"AESTDY": "test", "DOMAIN": "test"},
                  "uSubjId": 12,
                  "seq": 10,
                },
            ],
            "message": "AESTDY and DOMAIN are equal to test",
        }
        """
        df_columns: set = set(data)
        targets_in_dataset = targets.intersection(df_columns)
        targets_not_in_dataset = targets.difference(df_columns)
        all_targets_missing = (
            len(targets_in_dataset) == 0 and len(targets_not_in_dataset) > 0
        )
        if targets_in_dataset:
            errors_df = data[list(targets_in_dataset)]
        else:
            errors_df = data
        if not targets:
            errors_df = data

        if self.rule.get("sensitivity") == Sensitivity.DATASET.value:
            # Only generate one error for rules with dataset sensitivity
            missing_vars = {
                target: "Not in dataset" for target in targets_not_in_dataset
            }

            # Create the initial error
            error_value = (
                dict(errors_df.iloc[0].to_dict()) if not all_targets_missing else {}
            )

            # Add missing variables to the error value
            if missing_vars:
                error_value = {**error_value, **missing_vars}

            errors_list = [
                ValidationErrorEntity(
                    value=error_value,
                    dataset=self._get_dataset_name(data),
                )
            ]
        elif self.rule.get("sensitivity") == Sensitivity.RECORD.value:
            errors_list = self._generate_errors_by_target_presence(
                data, targets_not_in_dataset, all_targets_missing, errors_df
            )
        elif (
            self.rule.get("sensitivity") is not None
        ):  # rule sensitivity is incorrectly defined
            error_entity = ValidationErrorEntity(
                {
                    "dataset": "N/A",
                    "row": 0,
                    "value": {"ERROR": "Invalid or undefined sensitivity in the rule"},
                    "uSubjId": "N/A",
                    "SEQ": 0,
                }
            )
            return ValidationErrorContainer(
                domain=(
                    f"SUPP{self.dataset_metadata.rdomain}"
                    if self.dataset_metadata.is_supp
                    else (self.dataset_metadata.domain or self.dataset_metadata.name)
                ),
                targets=sorted(targets),
                message="Invalid or undefined sensitivity in the rule",
                errors=[error_entity],
            )
        else:  # rule sensitivity is undefined
            errors_list = self._generate_errors_by_target_presence(
                data, targets_not_in_dataset, all_targets_missing, errors_df
            )
        return ValidationErrorContainer(
            **{
                "domain": (
                    f"SUPP{self.dataset_metadata.rdomain}"
                    if self.dataset_metadata.is_supp
                    else (self.dataset_metadata.domain or self.dataset_metadata.name)
                ),
                "dataset": ", ".join(
                    sorted(set(error._dataset or "" for error in errors_list))
                ),
                "targets": sorted(targets),
                "errors": errors_list,
                "message": message.replace("--", self.dataset_metadata.domain or ""),
            }
        )

    def _generate_errors_by_target_presence(
        self,
        data: pd.DataFrame,
        targets_not_in_dataset: Set[str],
        all_targets_missing: bool,
        errors_df: pd.DataFrame,
    ) -> List[ValidationErrorEntity]:
        """
        Generate error list based on presence of target variables in the dataset.
        Handles two cases: (1) when all targets are missing, or (2) when some targets are present.

        Args:
            data: The original dataframe
            targets_not_in_dataset: Set of target variables not found in the dataset
            all_targets_missing: Boolean indicating if all targets are missing
            errors_df: DataFrame subset with only the target variables (if any exist)

        Returns:
            List of ValidationErrorEntity objects
        """
        missing_vars = {target: "Not in dataset" for target in targets_not_in_dataset}

        if all_targets_missing:
            errors_list = []
            for idx, row in data.iterrows():
                error = ValidationErrorEntity(
                    value={
                        target: "Not in dataset" for target in targets_not_in_dataset
                    },
                    dataset=self._get_dataset_name(pd.DataFrame([row])),
                    row=int(row.get(SOURCE_ROW_NUMBER, idx + 1)),
                    usubjid=(
                        str(row.get("USUBJID"))
                        if "USUBJID" in row and not pd.isna(row["USUBJID"])
                        else None
                    ),
                    sequence=(
                        int(row.get(f"{self.dataset_metadata.domain or ''}SEQ"))
                        if f"{self.dataset_metadata.domain or ''}SEQ" in row
                        and self._sequence_exists(
                            pd.Series(
                                {
                                    idx: row.get(
                                        f"{self.dataset_metadata.domain or ''}SEQ"
                                    )
                                }
                            ),
                            idx,
                        )
                        else None
                    ),
                )
                errors_list.append(error)
        else:
            errors_series: pd.Series = errors_df.apply(
                lambda df_row: self._create_error_object(df_row, data), axis=1
            )
            errors_list: List[ValidationErrorEntity] = errors_series.tolist()
            if missing_vars:
                for error in errors_list:
                    error.value = {**error.value, **missing_vars}
        return errors_list

    def _get_dataset_name(self, data: pd.DataFrame) -> str:
        source_pathnames = data.get(SOURCE_FILENAME, [])
        source_filenames = [
            path.basename(source_pathname) for source_pathname in source_pathnames
        ]
        source_filename_str = ", ".join(
            sorted(set(source_filename or "" for source_filename in source_filenames))
        )
        return source_filename_str

    def _create_error_object(
        self, df_row: pd.Series, data: pd.DataFrame
    ) -> ValidationErrorEntity:
        usubjid: Optional[pd.Series] = data.get("USUBJID")
        sequence: Optional[pd.Series] = data.get(
            f"{self.dataset_metadata.domain or ''}SEQ"
        )
        source_row_number: Optional[pd.Series] = data.get(SOURCE_ROW_NUMBER)
        source_filename: Optional[pd.Series] = data.get(SOURCE_FILENAME)
        row_dict = df_row.to_dict()
        filtered_dict = {}
        for key, value in row_dict.items():
            if isinstance(value, list):
                filtered_dict[key] = (
                    "null"
                    if any(val in NULL_FLAVORS for val in value) or pd.isna(value).any()
                    else value
                )
            else:
                filtered_dict[key] = (
                    "null" if (value in NULL_FLAVORS or pd.isna(value)) else value
                )
        error_object = ValidationErrorEntity(
            dataset=(
                path.basename(source_filename[df_row.name])
                if isinstance(source_filename, pd.Series)
                else ""
            ),
            row=(
                int(source_row_number[df_row.name])
                if isinstance(source_row_number, pd.Series)
                else (int(df_row.name) + 1)
            ),  # record number should start at 1, not 0
            value=filtered_dict,
            usubjid=(
                str(usubjid[df_row.name]) if isinstance(usubjid, pd.Series) else None
            ),
            sequence=(
                int(sequence[df_row.name])
                if self._sequence_exists(sequence, df_row.name)
                else None
            ),
        )
        return error_object

    def extract_target_names_from_value_level_metadata(self):
        return set([item["define_variable_name"] for item in self.value_level_metadata])

    @staticmethod
    def _sequence_exists(sequence: pd.Series, row_name: Hashable) -> bool:
        return (
            isinstance(sequence, pd.Series)
            and not pd.isnull(sequence[row_name])
            and not sequence[row_name] == ""
        )
