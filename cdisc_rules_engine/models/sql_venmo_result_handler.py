import re
from typing import List, Optional

import pandas as pd
from business_rules.actions import BaseActions, rule_action
from business_rules.fields import FIELD_TEXT

from cdisc_rules_engine.constants import NULL_FLAVORS
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
    SQLDatasetMetadata,
)
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.interfaces.condition_interface import ConditionInterface
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import ValidationErrorEntity


class SqlVenmoResultHandler(BaseActions):
    """
    This class maps the output of venmo (a truth series) to a list of error objects.
    It uses the rule 'Sensitivity' to determine whether to generate a single dataset-level error
    or multiple record-level errors.

    This is an example error:
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

    def __init__(
        self,
        output_container: list,
        dataset_metadata: SQLDatasetMetadata,
        rule: dict,
        dataset_id: str,
        data_service: PostgresQLDataService,
    ):
        self.output_container = output_container
        self.dataset_metadata = dataset_metadata
        self.rule = rule
        self.dataset_id = dataset_id
        self.data_service = data_service

    @rule_action(params={"message": FIELD_TEXT})
    def generate_dataset_error_objects(self, message: str, results: pd.Series):
        """
        This function maps the truth series from venmo to a list of error objects.
        """
        rows_with_error = self._get_error_rows(results)

        target_columns = SqlVenmoResultHandler._get_target_columns(
            self.rule, self.dataset_metadata, self.data_service.pgi.schema.get_table(self.dataset_id)
        )

        errors_list = self._generate_errors_list(rows_with_error, target_columns)
        error_object = self._bundle_error_object(
            message=message,
            error_rows=errors_list,
        )
        self.output_container.append(error_object.to_representation())

    def _get_error_rows(self, truth_series) -> List[dict]:
        """
        Fetch the rows which returned TRUE
        """
        true_indicies = [str(i + 1) for i, x in enumerate(truth_series) if x]
        self.data_service.pgi.execute_sql(
            f"""SELECT * FROM
                {self.data_service.pgi.schema.get_table_hash(self.dataset_id)}
            WHERE id IN ({', '.join(true_indicies)}) ORDER BY id ASC"""
        )
        results = self.data_service.pgi.fetch_all()
        return list(results)

    def _bundle_error_object(self, message: str, error_rows: List[ValidationErrorEntity]) -> ValidationErrorContainer:
        """
        Bundles the error rows into a ValidationErrorContainer.
        """
        return ValidationErrorContainer(
            domain=(
                f"SUPP{self.dataset_metadata.rdomain}"
                if self.dataset_metadata.is_supp
                else (self.dataset_metadata.domain or self.dataset_metadata.dataset_name)
            ),
            dataset=", ".join(sorted(set(error._dataset or "" for error in error_rows))),
            targets=SqlVenmoResultHandler._get_target_columns(
                self.rule, self.dataset_metadata, self.data_service.pgi.schema.get_table(self.dataset_id)
            ),
            errors=error_rows,
            message=message.replace("--", self.dataset_metadata.domain or ""),
        )

    def _generate_errors_list(self, data: List[dict], target_columns: dict[str, bool]) -> List[ValidationErrorEntity]:
        match self.rule.get("sensitivity"):
            case Sensitivity.DATASET.value:
                return [self._build_dataset_error(data, target_columns)]
            case Sensitivity.RECORD.value | None:
                return self._build_record_error_items(data, target_columns)
            case _:
                raise ValueError(f"Invalid sensitivity value: {self.rule.get('sensitivity')}")

    def _build_dataset_error(self, data: List[dict], target_columns: dict[str, bool]) -> ValidationErrorEntity:
        """Only generate one error for rules with dataset sensitivity"""
        if len(data) == 0:
            value = {}
        else:
            schema = self.data_service.pgi.schema.get_table(self.dataset_id)
            value = self._create_error_for_row(data[0], schema, target_columns).value

        return ValidationErrorEntity(
            value=value,
            dataset=self.dataset_metadata.dataset_name,
        )

    def _build_record_error_items(
        self, data: List[dict], target_columns: dict[str, bool]
    ) -> List[ValidationErrorEntity]:
        """
        Build a list of ValidationErrorEntity objects for each error row in the data.
        """
        schema: SqlTableSchema = self.data_service.pgi.schema.get_table(self.dataset_id)
        return [self._create_error_for_row(row, schema, target_columns) for row in data]

    """def _generate_errors_by_target_presence(
        self,
        data: pd.DataFrame,
        targets_not_in_dataset: Set[str],
        all_targets_missing: bool,
        errors_df: pd.DataFrame,
    ) -> List[ValidationErrorEntity]:"""
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
    """missing_vars = {target: "Not in dataset" for target in targets_not_in_dataset}

    if all_targets_missing:
        errors_list = []
        # for idx, row in data.iterrows():
        #     error = ValidationErrorEntity(
        #         value={target: "Not in dataset" for target in targets_not_in_dataset},
        #         dataset=self._get_dataset_name(pd.DataFrame([row])),
        #         row=int(row.get(SOURCE_ROW_NUMBER, idx + 1)),
        #         usubjid=(str(row.get("USUBJID")) if "USUBJID" in row and not pd.isna(row["USUBJID"]) else None),
        #         sequence=(
        #             int(row.get(f"{self.dataset_metadata.domain or ''}SEQ"))
        #             if f"{self.dataset_metadata.domain or ''}SEQ" in row
        #             and self._sequence_exists(
        #                 pd.Series({idx: row.get(f"{self.dataset_metadata.domain or ''}SEQ")}),
        #                 idx,
        #             )
        #             else None
        #         ),
        #     )
        #     errors_list.append(error)
    else:
        errors_series: pd.Series = errors_df.apply(lambda df_row: self._create_error_object(df_row, data), axis=1)
        errors_list: List[ValidationErrorEntity] = errors_series.tolist()
        if missing_vars:
            for error in errors_list:
                error.value = {**error.value, **missing_vars}
    return errors_list"""

    def _create_error_for_row(
        self, row: dict, schema: SqlTableSchema, target_columns: dict[str, bool]
    ) -> ValidationErrorEntity:
        usubjid = str(row.get(schema.get_column_hash("usubjid")))

        sequence_column = f"{self.dataset_metadata.domain or ''}SEQ"
        sequence_value = row.get(schema.get_column_hash(sequence_column))
        sequence = int(sequence_value) if sequence_value is not None and sequence_value != "" else None

        row_id = row.get("id")

        values = {}
        for column in sorted(target_columns.keys()):
            if not target_columns[column]:
                values[column] = "Not in dataset"
                continue

            value = row.get(schema.get_column_hash(column))
            if value is None or value in NULL_FLAVORS:
                values[column] = None
            else:
                values[column] = value

        return ValidationErrorEntity(
            dataset=self.dataset_metadata.dataset_name,
            row=row_id,
            usubjid=usubjid,
            sequence=sequence,
            value=values,
        )

    @staticmethod
    def _get_target_columns(rule: dict, metadata: SQLDatasetMetadata, schema: SqlTableSchema) -> dict[str, bool]:
        """
        Returns the columns to display in the error object
        """
        target_columns = SqlVenmoResultHandler._extract_target_names_from_rule(rule, metadata, schema)
        target_columns_with_presence = {column: schema.has_column(column) for column in target_columns}
        return target_columns_with_presence

    @staticmethod
    def _extract_target_names_from_rule(rule: dict, metadata: SQLDatasetMetadata, schema: SqlTableSchema) -> List[str]:
        r"""
        Extracts target from each item of condition list.

        Some operators require reporting additional column names when
        extracting target names. An operator has a certain pattern,
        to which these column names have to correspond. So we
        have a mapping like {operator: pattern} to find the
        necessary pattern and extract matching column names.
        Example:
            column: TSVAL
            operator: additional_columns_empty
            pattern: ^TSVAL\d+$ (starts with TSVAL and ends with number)
            additional columns: TSVAL1, TSVAL2, TSVAL3 etc.
        """

        output_variables: List[str] = rule.get("output_variables", [])
        if output_variables:
            target_names: List[str] = [var.replace("--", metadata.domain or "", 1) for var in output_variables]
        else:
            target_names: List[str] = []
            conditions: ConditionInterface = rule["conditions"]
            for condition in conditions.values():
                if condition.get("operator") == "not_exists":
                    continue
                target: str = condition["value"].get("target")
                if target is None:
                    continue
                target = target.replace("--", metadata.domain or "")
                op_related_pattern: str = SqlVenmoResultHandler.get_operator_related_pattern(
                    condition.get("operator"), target
                )
                if op_related_pattern is not None:
                    columns = [col for col, _ in schema.get_columns()]
                    target_names.extend(
                        filter(
                            lambda name: re.match(op_related_pattern, name),
                            columns,
                        )
                    )
                else:
                    target_names.append(target)
        return list(set(target_names))

    @staticmethod
    def get_operator_related_pattern(operator: str, target: str) -> Optional[str]:
        # {operator: pattern} mapping
        operator_related_patterns: dict = {
            "additional_columns_empty": rf"^{target}\d+$",
            "additional_columns_not_empty": rf"^{target}\d+$",
        }
        return operator_related_patterns.get(operator)
