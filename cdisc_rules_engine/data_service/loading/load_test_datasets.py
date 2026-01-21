from typing import List

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER, SOURCE_DS
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.models.sql.table_schema import SqlColumnSchema, SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset


class SqlTestDatasetLoader:
    @staticmethod
    def load_test_datasets(pgi: PostgresQLInterface, test_datasets: List[TestDataset]) -> List[DatasetMetadata2]:
        return [SqlTestDatasetLoader.load_test_dataset(pgi, test_dataset) for test_dataset in test_datasets]

    @staticmethod
    def load_test_dataset(pgi: PostgresQLInterface, test_dataset: TestDataset) -> DatasetMetadata2:
        records = test_dataset.records
        # Create schema and table:
        row_dicts = [dict(zip(records, values)) for values in zip(*records.values())]
        # force lower_case throughout
        table_name = test_dataset.name.lower()
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        if row_dicts and SOURCE_ROW_NUMBER in row_dicts[0]:
            raise ValueError(
                f"Test dataset '{table_name}' contains reserved column '{SOURCE_ROW_NUMBER}'. "
                "This column is automatically generated and should not be in test data."
            )

        if row_dicts and SOURCE_DS in row_dicts[0]:
            raise ValueError(
                f"Test dataset '{table_name}' contains reserved column '{SOURCE_DS}'. "
                "This column is automatically generated and should not be in test data."
            )

        for idx, row in enumerate(row_dicts, start=1):
            row[SOURCE_ROW_NUMBER] = idx
            row[SOURCE_DS] = table_name.upper()

        schema = SqlTableSchema.from_metadata(test_dataset, pgi)
        source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
        schema.add_column(source_row_column)

        source_ds_column = SqlColumnSchema(name=SOURCE_DS, hash=SOURCE_DS, type="Char")
        schema.add_column(source_ds_column)

        pgi.create_table(schema)
        pgi.insert_data(table_name=table_name, data=row_dicts)

        # TODO INDEX

        return DatasetMetadata2(**{k: v for k, v in test_dataset.__dict__.items() if k != "records"})
