from typing import List

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset


class SqlTestDatasetLoader:
    @staticmethod
    def load_test_datasets(pgi: PostgresQLInterface, test_datasets: List[TestDataset]) -> List[DatasetMetadata]:
        return [SqlTestDatasetLoader.load_test_dataset(pgi, test_dataset) for test_dataset in test_datasets]

    @staticmethod
    def load_test_dataset(pgi: PostgresQLInterface, test_dataset: TestDataset) -> DatasetMetadata:
        # Create schema and table:
        row_dicts = [dict(zip(test_dataset["records"], values)) for values in zip(*test_dataset["records"].values())]
        # force lower_case throughout
        table_name = test_dataset["name"].lower()
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        schema = SqlTableSchema.from_metadata(test_dataset)
        pgi.create_table(schema)
        pgi.insert_data(table_name=table_name, data=row_dicts)

        # TODO INDEX

        return SDTMDatasetMetadata(
            file_size=0,
            filename=test_dataset["filename"],
            full_path=test_dataset["filepath"],
            label=test_dataset["label"],
            name=test_dataset["name"],
            record_count=len(row_dicts),
            modification_date=None,
            original_path=None,
            first_record={k.upper(): v for k, v in row_dicts[0].items()},
        )
