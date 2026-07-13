"""
Data Preprocessor for SDTM and ADaM clinical data.
"""

from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from cdisc_rules_engine.data_service.sql_serialiser import SQLSerialiser

if TYPE_CHECKING:
    from cdisc_rules_engine.data_service.postgresql_data_service import (
        PostgresQLDataService,
    )
    from cdisc_rules_engine.standards.base_standards_context import (
        BaseStandardsContext,
    )

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER, SOURCE_DS
from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services import logger


class SqlDataPreprocessor:
    """
    Performs preprocessing operations on clinical data.
    Operations should be performed at data ingestion time.
    """

    def __init__(
        self,
        data_service: "PostgresQLDataService",
        standards_context: "BaseStandardsContext",
    ):
        self.data_service = data_service
        self.standards_context = standards_context

    def _get_table_hash(self, table_name: str) -> str:
        table_hash = self.data_service.pgi.schema.get_table_hash(table_name)
        if table_hash:
            return table_hash
        return table_name.lower()

    def _process_split_datasets(self) -> None:
        """Concatenate split datasets into single logical datasets."""
        logger.info("Processing split datasets")

        dataset_names = [ds.name.lower() for ds in self.data_service.datasets]
        split_groups = self.standards_context.detect_split_datasets(dataset_names)

        if not split_groups:
            logger.info("No split datasets found")
            return

        for unsplit_name, dataset_parts in split_groups.items():
            logger.info(f"Concatenating {len(dataset_parts)} parts for {unsplit_name}: " f"{', '.join(dataset_parts)}")
            self._concatenate_split_parts(unsplit_name, dataset_parts)

    def _concatenate_split_parts(self, unsplit_name: str, dataset_parts: List[str]) -> None:
        """Concatenate multiple dataset parts into a single table."""
        if not dataset_parts:
            logger.warning(f"No parts to concatenate for {unsplit_name}")
            return

        all_columns, part_schemas = self._gather_part_schemas_and_columns(dataset_parts)
        if not all_columns:
            logger.error(f"No columns found across parts for {unsplit_name}")
            return

        unsplit_schema = self._create_unsplit_table(unsplit_name, all_columns)
        target_columns = list(all_columns.values())

        union_parts = self._build_union_query_parts(dataset_parts, part_schemas, target_columns)
        if not union_parts:
            logger.error(f"No valid parts to union for {unsplit_name}")
            return

        self._execute_merge_insert(unsplit_schema, target_columns, union_parts)
        self._create_unsplit_indexes(unsplit_name, unsplit_schema)

        logger.info(f"Concatenated dataset: {unsplit_name}")

    def _gather_part_schemas_and_columns(
        self, dataset_parts: List[str]
    ) -> Tuple[Dict[str, Any], Dict[str, SqlTableSchema]]:
        """Collect schema information from all split parts."""
        all_columns: Dict[str, Any] = {}
        part_schemas = {}

        for part in dataset_parts:
            schema = self.data_service.pgi.schema.get_table(part)
            if not schema:
                logger.error(f"Schema not found for split part: {part}")
                continue

            part_schemas[part] = schema
            for col_name, col_schema in schema.get_columns():
                if col_name.lower() == "id":
                    continue
                if col_name not in all_columns:
                    all_columns[col_name] = col_schema

        return all_columns, part_schemas

    def _create_unsplit_table(self, unsplit_name: str, all_columns: Dict[str, Any]) -> SqlTableSchema:
        """Create the target table in the database."""
        unsplit_schema = SqlTableSchema.derived(unsplit_name, self.data_service.pgi)
        for col_schema in all_columns.values():
            unsplit_schema.add_column(col_schema)
        self.data_service.pgi.create_table(unsplit_schema)
        return unsplit_schema

    def _build_union_query_parts(
        self,
        dataset_parts: List[str],
        part_schemas: Dict[str, SqlTableSchema],
        target_columns: List[SqlColumnSchema],
    ) -> List[str]:
        """Build the SELECT statements for the UNION query."""
        union_parts = []
        for part in dataset_parts:
            if part not in part_schemas:
                continue

            part_schema = part_schemas[part]
            part_hash = self._get_table_hash(part)

            select_items = []
            for target_col in target_columns:
                part_col = part_schema.get_column(target_col.name)
                if part_col:
                    select_items.append(f"{part_col.hash} AS {target_col.hash}")
                else:
                    select_items.append(
                        f"CAST(NULL AS {SQLSerialiser.column_type_to_sql_type(target_col.type)}) AS {target_col.hash}"
                    )

            union_parts.append(f"SELECT {', '.join(select_items)} FROM public.{part_hash}")
        return union_parts

    def _execute_merge_insert(
        self,
        unsplit_schema: SqlTableSchema,
        target_columns: List[SqlColumnSchema],
        union_parts: List[str],
    ) -> None:
        """Execute the INSERT logic to populate the concatenated table."""
        union_query = " UNION ALL ".join(union_parts)
        target_col_hashes = [col.hash for col in target_columns]
        columns_str = ", ".join(target_col_hashes)

        source_ds_hash = unsplit_schema.get_column_hash(SOURCE_DS) or SOURCE_DS
        source_row_hash = unsplit_schema.get_column_hash(SOURCE_ROW_NUMBER) or SOURCE_ROW_NUMBER

        insert_query = f"""
            INSERT INTO public.{unsplit_schema.hash} ({columns_str})
            SELECT {columns_str} FROM (
                {union_query}
            ) AS concatenated
            ORDER BY {source_ds_hash}, {source_row_hash}
        """

        self.data_service.pgi.execute_sql(insert_query)

    def _create_unsplit_indexes(self, unsplit_name: str, unsplit_schema: SqlTableSchema) -> None:
        """Create necessary indexes on the concatenated table."""
        source_ds_hash = unsplit_schema.get_column_hash(SOURCE_DS) or SOURCE_DS
        source_row_hash = unsplit_schema.get_column_hash(SOURCE_ROW_NUMBER) or SOURCE_ROW_NUMBER

        index_queries = [
            f"CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_source_ds "
            f"ON public.{unsplit_schema.hash}({source_ds_hash})",
            f"CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_source_row "
            f"ON public.{unsplit_schema.hash}({source_row_hash})",
        ]

        studyid_hash = unsplit_schema.get_column_hash("studyid")
        usubjid_hash = unsplit_schema.get_column_hash("usubjid")

        if studyid_hash and usubjid_hash:
            index_queries.append(
                f"CREATE INDEX IF NOT EXISTS idx_{unsplit_name}_studyid_usubjid "
                f"ON public.{unsplit_schema.hash}({studyid_hash}, {usubjid_hash})"
            )

        for idx_query in index_queries:
            self.data_service.pgi.execute_sql(idx_query)

    def _create_metadata_from_split_parts(
        self, unsplit_name: str, source_parts: List[str]
    ) -> Optional[DatasetMetadata2]:
        """Create metadata object by merging split part metadata."""
        part_metadata = []
        for part_name in source_parts:
            part_meta = next(
                (ds for ds in self.data_service.datasets if ds.name.lower() == part_name.lower()),
                None,
            )
            if part_meta:
                part_metadata.append(part_meta)

        if not part_metadata:
            return None

        first_part = part_metadata[0]
        merged_variables = []
        seen_vars = set()

        for part in part_metadata:
            for var in part.variables:
                var_name = var.name.upper()
                if var_name not in seen_vars:
                    merged_variables.append(
                        VariableMetadata(
                            name=var_name,
                            label=var.label,
                            type=var.type,
                            length=var.length,
                            format=var.format,
                            order=var.order,
                        )
                    )
                    seen_vars.add(var_name)

        metadata = deepcopy(first_part)
        file_type = first_part.filename.split(".")[-1].lower()
        metadata.filename = f"{unsplit_name}.{file_type}"
        metadata.name = unsplit_name.upper()
        metadata.variables = merged_variables

        return metadata

    def _update_metadata(self, timestamp: datetime) -> None:
        """Update metadata for preprocessed datasets."""
        split_update_query = """
            UPDATE public.data_metadata
            SET
                dataset_preprocessed = %s,
                preprocessing_stage = 'split_processed',
                updated_at = %s
            WHERE dataset_is_split = true
        """
        self.data_service.pgi.execute_sql(split_update_query, (timestamp, timestamp))

    @staticmethod
    def run(
        data_service: "PostgresQLDataService",
        standards_context: "BaseStandardsContext",
    ) -> None:
        preprocessor = SqlDataPreprocessor(data_service, standards_context)
        preprocessor._process_split_datasets()
