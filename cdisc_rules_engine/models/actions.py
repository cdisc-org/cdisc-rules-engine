from typing import List, Optional, Set

import pandas as pd
from business_rules.actions import BaseActions, rule_action
from business_rules.fields import FIELD_TEXT

from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidOutputVariables
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
        domain: str,
        rule: dict,
        value_level_metadata: list = None,
    ):
        if value_level_metadata is None:
            value_level_metadata = []

        self.output_container = output_container
        self.variable = variable
        self.domain = domain
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
        self.variable.dataset["results"] = results
        rows_with_error = self.variable.dataset[
            self.variable.dataset["results"].isin([True])
        ]
        target_names: Set[str] = RuleProcessor.extract_target_names_from_rule(
            self.rule, self.domain, self.variable.dataset.columns.tolist()
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

    def generate_targeted_error_object(
        self, targets: Set[str], data: pd.DataFrame, message: str
    ) -> ValidationErrorContainer:
        """
        Generates a targeted error object.
        Return example:
        {
            "domain": "AE",
            "variables": ["AESTDY", "DOMAIN"],
            "errors": [
                {
                  "row": 0,
                  "value": {"STUDYID": "Not in dataset"},
                  "uSubjId": "2",
                  "seq": 1,
                },
                {
                  "row": 1,
                  "value": {"AESTDY": "test", "DOMAIN": "test"},
                  "uSubjId": 7,
                  "seq": 2,
                },
                {
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
        errors_df = data[targets_in_dataset]
        if errors_df.empty:
            raise InvalidOutputVariables(
                f"Output variables: {list(targets)} not found in dataset"
            )
        if self.rule.get("sensitivity") == Sensitivity.DATASET.value:
            # Only generate one error for rules with dataset sensitivity
            errors_list = [
                ValidationErrorEntity(
                    value=dict(errors_df.iloc[0].to_dict()),
                )
            ]
        else:
            # Rule is treated as record level
            errors_series: pd.Series = errors_df.apply(
                lambda df_row: self._create_error_object(df_row, data), axis=1
            )
            errors_list: List[ValidationErrorEntity] = errors_series.tolist()
        missing_vars = {target: "Not in dataset" for target in targets_not_in_dataset}
        if missing_vars:
            for error in errors_list:
                error.value = {**error.value, **missing_vars}
        return ValidationErrorContainer(
            **{
                "domain": self.domain,
                "targets": sorted(targets),
                "errors": errors_list,
                "message": message.replace("--", self.domain),
            }
        )

    def _create_error_object(
        self, df_row: pd.Series, data: pd.DataFrame
    ) -> ValidationErrorEntity:
        usubjid: Optional[pd.Series] = data.get("USUBJID")
        sequence: Optional[pd.Series] = data.get(f"{self.domain}SEQ")
        error_object = ValidationErrorEntity(
            row=int(df_row.name) + 1,  # record number should start at 1, not 0
            value=dict(df_row.to_dict()),
            usubjid=str(usubjid[df_row.name])
            if isinstance(usubjid, pd.Series)
            else None,
            sequence=int(sequence[df_row.name])
            if isinstance(sequence, pd.Series)
            else None,
        )
        return error_object

    def extract_target_names_from_value_level_metadata(self):
        return set([item["define_variable_name"] for item in self.value_level_metadata])
