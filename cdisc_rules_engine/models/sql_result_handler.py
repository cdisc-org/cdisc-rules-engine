from typing import Dict, List, Optional

from cdisc_rules_engine.compiler.compiler import CompiledQuery
from cdisc_rules_engine.compiler.targets import get_target_columns
from cdisc_rules_engine.constants import NULL_FLAVORS
from cdisc_rules_engine.constants.metadata_columns import SOURCE_DS, SOURCE_ROW_NUMBER, TOTAL_COUNT_COLUMN
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import ValidationErrorEntity
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata


class SqlResultHandler:
    """
    Turns the rows a compiled rule returned into error objects.
    Sensitivity was already applied in SQL, so whatever comes back gets reported.

    Example output:
    {
        "dataset": "ae.xpt",
        "domain": "AE",
        "variables": ["AESTDY", "DOMAIN"],
        "errors": [
            {
                "dataset": "ae.xpt",
                "row": 0,
                "value": {"STUDYID": "Not in dataset"},
                "USUBJID": "2",
                "SEQ": 1,
            },
            {
                "dataset": "ae.xpt",
                "row": 1,
                "value": {"AESTDY": "test", "DOMAIN": "test"},
                "USUBJID": 7,
                "SEQ": 2,
            },
        ],
        "message": "AESTDY and DOMAIN are equal to test",
    }
    """

    def __init__(
        self,
        output_container: list,
        dataset_metadata: BaseDatasetMetadata,
        rule: dict,
        dataset_id: str,
        data_service: PostgresQLDataService,
        operation_variables: dict = None,
    ):
        self.output_container = output_container
        self.dataset_metadata = dataset_metadata
        self.rule = rule
        self.dataset_id = dataset_id
        self.data_service = data_service
        self.operation_variables = operation_variables or {}

    def report(self, compiled: CompiledQuery, rows: List[dict]) -> None:
        if not rows:
            return
        message = (self.rule.get("actions") or [{}])[0].get("params", {}).get("message") or ""
        errors = self._generate_errors_list(rows, compiled)
        for error_object in self._bundle_error_objects_per_source(message, errors, self._shown(rows, compiled)):
            self.output_container.append(error_object.to_representation())

    def _shown(self, rows: List[dict], compiled: CompiledQuery) -> Dict[Optional[str], str]:
        """
        The "(N of M errors shown)" note for each reported file, where
        the errors were capped.
        A split dataset is capped per source file, and each of its rows
        carries the total of its own file.
        """
        split = bool(getattr(self.dataset_metadata, "split_part_filenames", None))
        counts: Dict[Optional[str], List[int]] = {}
        for row in rows:
            total = row.get(TOTAL_COUNT_COLUMN)
            if total is None:
                continue
            source = self._source_dataset_filename(row, compiled.schema).lower() if split else None
            counts.setdefault(source, [0, total])[0] += 1
        return {
            source: f" ({shown} of {total} errors shown)" for source, (shown, total) in counts.items() if total > shown
        }

    def _bundle_error_objects_per_source(
        self, message: str, errors_list: List[ValidationErrorEntity], shown: Optional[Dict[Optional[str], str]] = None
    ) -> List[ValidationErrorContainer]:
        """
        One container per source file.
        Parts with no errors still get one (with no message).
        """
        split_parts = getattr(self.dataset_metadata, "split_part_filenames", None)
        shown = shown or {}
        if not split_parts:
            return [self._bundle_error_object(message=message, error_rows=errors_list, shown=shown.get(None, ""))]

        containers = []
        for source_dataset in sorted(split_parts):
            error_rows = [error for error in errors_list if (error._dataset or "").lower() == source_dataset.lower()]
            containers.append(
                self._bundle_error_object(
                    message=message if error_rows else None,
                    error_rows=error_rows,
                    dataset=source_dataset,
                    shown=shown.get(source_dataset.lower(), ""),
                )
            )
        return containers

    def _bundle_error_object(
        self,
        message: Optional[str],
        error_rows: List[ValidationErrorEntity],
        dataset: Optional[str] = None,
        shown: str = "",
    ) -> ValidationErrorContainer:
        original_schema = self.data_service.pgi.schema.get_table(self.dataset_metadata.name)

        return ValidationErrorContainer(
            domain=(self.dataset_metadata.domain),
            dataset=dataset or ", ".join(sorted(set(error._dataset or "" for error in error_rows))),
            targets=get_target_columns(self.rule, self.dataset_metadata, original_schema),
            errors=error_rows,
            message=self._resolve_message_domain(message) + shown if message is not None else None,
        )

    def _resolve_message_domain(self, message: str) -> str:
        domain = self.dataset_metadata.domain or ""
        for prefix in ("SUPP", "SQ", "AP"):
            if domain.startswith(prefix):
                message = message.replace(f"{prefix}--", domain)
        return message.replace("--", domain)

    def _generate_errors_list(self, rows: List[dict], compiled: CompiledQuery) -> List[ValidationErrorEntity]:
        match self.rule.get("sensitivity"):
            case Sensitivity.DATASET.value | Sensitivity.STUDY.value:
                return self._build_dataset_errors(rows, compiled)
            case Sensitivity.RECORD.value | Sensitivity.GROUP.value | None:
                return [self._create_error_for_row(row, compiled) for row in rows]
            case _:
                raise ValueError(f"Invalid sensitivity value: {self.rule.get('sensitivity')}")

    def _build_dataset_errors(self, rows: List[dict], compiled: CompiledQuery) -> List[ValidationErrorEntity]:
        """One error for the dataset OR one per part, for a split dataset with errors in several parts."""
        if not getattr(self.dataset_metadata, "split_part_filenames", None):
            return [
                ValidationErrorEntity(
                    value=self._create_error_for_row(rows[0], compiled).value,
                    dataset=self.dataset_metadata.filename,
                )
            ]

        first_row_per_source = {}
        for row in rows:
            first_row_per_source.setdefault(self._source_dataset_filename(row, compiled.schema), row)

        return [
            ValidationErrorEntity(
                value=self._create_error_for_row(row, compiled).value,
                dataset=source_dataset,
            )
            for source_dataset, row in first_row_per_source.items()
        ]

    def _create_error_for_row(self, row: dict, compiled: CompiledQuery) -> ValidationErrorEntity:
        schema = compiled.schema
        usubjid = str(row.get(schema.get_column_hash("usubjid")))

        sequence_column = f"{self.dataset_metadata.domain or ''}SEQ"
        sequence_value = row.get(schema.get_column_hash(sequence_column))
        sequence = int(sequence_value) if sequence_value is not None and sequence_value != "" else None

        values = {}
        for column, present in compiled.target_columns.items():
            if not present:
                values[column] = "Not in dataset"
                continue

            if column in compiled.inline_outputs:
                value = self._inline_output(column, row)
            elif column.startswith("$"):
                value = self._evaluate_operation_variable(column, row, schema)
            else:
                value = row.get(schema.get_column_hash(column))

            if value is None or value in NULL_FLAVORS:
                values[column] = None
            else:
                values[column] = value

        return ValidationErrorEntity(
            dataset=self._source_dataset_filename(row, schema),
            row=int(self._row_id(row, schema)),
            usubjid=usubjid,
            sequence=sequence,
            value=values,
        )

    @staticmethod
    def _row_id(row: dict, schema: SqlTableSchema):
        """
        Data tables report the row in the original file. Derived views report it too when they
        carry it through; otherwise the view's own id is the best we have.
        """
        source_row_hash = schema.get_column_hash(SOURCE_ROW_NUMBER)
        if schema.source == "data":
            # original data tables MUST have source_row_number
            if not source_row_hash or source_row_hash not in row:
                raise ValueError(
                    f"source_row_number not found in row data for table {schema.name}. "
                    f"Data loading issue. All original data tables must have source_row_number."
                )
            return row.get(source_row_hash)
        if schema.source == "derived" and source_row_hash and source_row_hash in row:
            return row.get(source_row_hash)
        return row.get("id")

    def _inline_output(self, name: str, row: dict):
        value = row.get(name)
        result = self.operation_variables[name]
        if result.type != "collection":
            return value
        # ARRAY(...) gives {} rather than NULL when nothing matched.
        # For a per-row collection that's reported as None, same as the per-row fallback does.
        values = value or []
        if result.params and not values:
            # A per-row collection with nothing for this row
            return None
        return [item for item in values if item is not None]

    def _source_dataset_filename(self, row: dict, schema: SqlTableSchema) -> str:
        # source_ds is the part name (no extension), so borrow the extension from the dataset's filename
        source_ds_hash = schema.get_column_hash(SOURCE_DS)
        source_ds = row.get(source_ds_hash) if source_ds_hash else None
        if not source_ds:
            return self.dataset_metadata.filename

        extension = self.dataset_metadata.filename.rsplit(".", 1)
        if len(extension) == 2:
            return f"{str(source_ds).lower()}.{extension[1]}"
        return str(source_ds).lower()

    def _evaluate_operation_variable(self, variable_name: str, row: dict, schema: SqlTableSchema):
        """
        Fallback for $operations the compiled query couldn't select itself.
        Run the operation's query once for this row.
        Slow (one round trip per reported row) but only hit when inlining failed (see SQLRulesEngine._failing_rows).
        """
        result = self.operation_variables.get(variable_name)
        if result is None:
            return "Operation variable not found"
        if result.type not in ("constant", "collection"):
            return "Unsupported operation variable type"

        query = result.query
        if result.params:
            query = self._substitute_parameters(query, result.params, row, schema)
        try:
            self.data_service.pgi.execute_sql(query)
            rows = self.data_service.pgi.fetch_all()
        except Exception as e:
            # reported as the value rather than raised
            # one bad operation doesn't lose the whole report
            return f"Query error: {str(e)}"

        if result.type == "constant":
            # first column of the first row whatever it's called
            return next(iter(rows[0].values()), None) if rows else None
        if not rows:
            # per-row collection with nothing for this row is None
            # an unparameterised one is just empty
            return None if result.params else []
        return [r.get("value") for r in rows if r.get("value") is not None]

    def _substitute_parameters(self, query: str, params: dict, row: dict, schema: SqlTableSchema) -> str:
        for placeholder, column_name in sorted(params.items(), key=lambda item: -len(item[0])):
            if column_name == "id":
                value = row.get("id")
            else:
                value = row.get(schema.get_column_hash(column_name))

            if value is None:
                query = query.replace(placeholder, "NULL")
            elif isinstance(value, str):
                # quoted so it's read as a literal and not a column name.
                escaped = value.replace("'", "''")
                query = query.replace(placeholder, f"'{escaped}'")
            else:
                query = query.replace(placeholder, str(value))
        return query
