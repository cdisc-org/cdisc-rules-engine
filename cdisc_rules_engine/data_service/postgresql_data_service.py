from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.data_service.loading.load_test_datasets import (
    SqlTestDatasetLoader,
)
from cdisc_rules_engine.data_service.merges.child import SqlChildMerge
from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.merges.relationship import SqlRelationshipMerge
from cdisc_rules_engine.data_service.merges.relrec import SqlRelrecMerge
from cdisc_rules_engine.data_service.merges.supp import SqlSuppMerge
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.startup.populate_codelists import (
    populate_codelists,
)
from cdisc_rules_engine.data_service.startup.populate_standards import (
    populate_standards,
)
from cdisc_rules_engine.data_service.startup.populate_terminology import (
    populate_terminology,
)
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema, SqlColumnSchema
from cdisc_rules_engine.models.test_dataset import TestDataset

SCHEMA_PATH = Path(__file__).parent / "schemas"


@dataclass
class SQLDatasetMetadata:
    filename: str
    filepath: str
    dataset_id: str
    table_hash: str
    dataset_name: str
    dataset_label: str
    unsplit_name: str
    domain: str
    is_supp: bool
    rdomain: str
    variables: list[str]
    is_split: bool = False


class PostgresQLDataService:

    def __init__(self, postgres_interface: PostgresQLInterface):
        self.pgi = postgres_interface
        self.datasets: List[DatasetMetadata] = []

    @classmethod
    def instance(cls) -> "PostgresQLDataService":
        """
        Create a PostgresQLDataService instance with an initialized database.
        """
        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        instance = cls(postgres_interface=pgi)
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))
        populate_terminology(pgi)
        populate_codelists(pgi)
        populate_standards(pgi)
        return instance

    @classmethod
    def from_list_of_testdatasets(cls, test_datasets: list[TestDataset]) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        instance = cls.instance()
        instance.datasets += SqlTestDatasetLoader.load_test_datasets(instance.pgi, test_datasets)
        return instance

    @classmethod
    def from_dataset_paths(cls, dataset_paths: List[str]) -> "PostgresQLDataService":
        instance = cls.instance()
        instance.datasets += SqlDatasetLoader.load_datasets(instance.pgi, dataset_paths)
        return instance

    @staticmethod
    def add_test_dataset(
        data_service: "PostgresQLDataService", table_name: str, column_data: dict[str, list[Union[str, int, float]]]
    ):
        # Check all the columns are the same length
        lengths = {len(v) for v in column_data.values()}
        if len(set(lengths)) != 1:
            raise ValueError("All input data columns must have the same length")

        if SOURCE_ROW_NUMBER in [k.lower() for k in column_data.keys()]:
            raise ValueError(
                f"Test dataset '{table_name}' contains reserved column 'source_row_number'. "
                "This column is automatically generated and should not be in test data."
            )

        # Create schema and table:
        schema_row = {
            col.lower(): next((val for val in values if val is not None), "") for col, values in column_data.items()
        }
        row_dicts = [dict(zip(column_data, values)) for values in zip(*column_data.values())]
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        for idx, row in enumerate(row_dicts, start=1):
            row[SOURCE_ROW_NUMBER] = idx

        schema = SqlTableSchema.from_data(table_name, schema_row)
        source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
        schema.add_column(source_row_column)
        data_service.pgi.create_table(schema)

        data_service.pgi.insert_data(table_name=table_name, data=row_dicts)

        data_service.datasets.append(
            SDTMDatasetMetadata(
                file_size=0,
                filename=f"{table_name}.xpt",
                full_path=f"/test/{table_name}.xpt",
                label=f"Test {table_name} Dataset",
                name=table_name,
                record_count=len(row_dicts),
                modification_date=None,
                original_path=None,
                first_record=row_dicts[0],
            )
        )

        return schema

    def get_uploaded_dataset_ids(self) -> list[str]:
        return [dataset.name for dataset in self.datasets]

    def get_dataset_metadata(self, dataset_id: str) -> SQLDatasetMetadata:

        tmp = next((metadata for metadata in self.datasets if metadata.name.lower() == dataset_id.lower()), None)
        if not tmp:
            return None

        # Query data_metadata to get variable names
        query = f"""
            SELECT var_name
            FROM data_metadata
            WHERE dataset_id = '{dataset_id.lower()}'
            ORDER BY id;
        """
        self.pgi.execute_sql(query=query)
        results = self.pgi.fetch_all()
        variables = [res["var_name"] for res in results] if results else []

        return SQLDatasetMetadata(
            filename=tmp.filename,
            filepath=str(tmp.full_path),
            dataset_id=tmp.name,
            table_hash=tmp.name,
            dataset_name=tmp.name,
            dataset_label=tmp.label,
            unsplit_name=tmp.name,
            domain=tmp.domain,
            is_supp=tmp.is_supp,
            rdomain=tmp.rdomain,
            variables=variables,
            is_split=tmp.is_split,
        )

    def get_dataset_for_rule(self, dataset_metadata: SQLDatasetMetadata, rule: dict) -> str:
        """Get or create preprocessed dataset based on rule requirements."""
        datasets = rule.get("datasets", [])
        if not datasets:
            return dataset_metadata.dataset_id

        left_id = dataset_metadata.dataset_id

        for merge_spec in datasets:
            right: str = merge_spec.get("domain_name").lower()
            is_relationship = merge_spec.get("relationship_columns", None) is not None
            is_child = bool(merge_spec.get("child"))

            if is_child:
                # Gate: Only merge if domain_name matches current dataset
                domain_name = merge_spec.get("domain_name")

                is_supp_merge = (
                    (domain_name[:4] == "SUPP" or domain_name[:4] == "SQAP")
                    and dataset_metadata.is_supp
                    and dataset_metadata.rdomain
                    and (domain_name == "SUPP--" or domain_name == dataset_metadata.dataset_name)
                )

                domain_matches = domain_name == dataset_metadata.domain or domain_name == dataset_metadata.dataset_name

                if is_supp_merge or domain_matches:
                    left_id = self._do_child_merge(
                        child=left_id,
                        dataset_metadata=dataset_metadata,
                        merge_spec=merge_spec,
                        rule=rule,
                    )
            elif right == "relrec":
                left_id = self._do_relrec_merge(
                    original=left_id,
                    relrec_dataset=right,
                    dataset_metadata=dataset_metadata,
                    merge_spec=merge_spec,
                    rule=rule,
                )
            elif right == "supp--":
                left_id = self._do_supp_merge(
                    original=left_id, target=right, dataset_metadata=dataset_metadata, merge_spec=merge_spec, rule=rule
                )
            elif is_relationship:
                left_id = self._do_relationship_merge(
                    original=left_id,
                    relationship_dataset=right,
                    dataset_metadata=dataset_metadata,
                    merge_spec=merge_spec,
                    rule=rule,
                )
            else:
                left_id = self._do_join_merge(left=left_id, right=right, merge_spec=merge_spec, rule=rule)

        return left_id

    def _do_join_merge(self, left: str, right: str, merge_spec: dict, rule: dict) -> str:
        """
        Perform a join merge operation on the datasets.
        """
        join_type = merge_spec.get("join_type", "INNER")
        # For now we assume pivot columns are always the same in left and right
        pivot_columns = merge_spec.get("match_key", [])

        joined_schema = SqlJoinMerge.perform_join(
            pgi=self.pgi,
            left=self.pgi.schema.get_table(left),
            right=self.pgi.schema.get_table(right),
            pivot_left=pivot_columns,
            pivot_right=pivot_columns,
            type=join_type.upper(),
        )

        return joined_schema.name

    def _do_supp_merge(
        self, original: str, target: str, dataset_metadata: SQLDatasetMetadata, merge_spec: dict, rule: dict
    ) -> str:
        """
        Find the corresponding SUPP datasets, then perform a SUPP merge operation on the datasets.
        """
        if target != "supp--" and dataset_metadata.domain.lower() not in target:
            raise ValueError(
                f"Tried to SUPP merge {dataset_metadata.domain}, but the target domain {target} does not match."
            )

        supp_dataset = next(
            (
                dataset
                for dataset in self.datasets
                if dataset.is_supp and not dataset.is_split and dataset.rdomain == dataset_metadata.domain
            ),
            None,
        )
        if not supp_dataset:
            raise ValueError(
                f"Tried to SUPP merge {dataset_metadata.domain}, but could not find corresponding SUPP dataset."
            )

        return SqlSuppMerge.perform_join(
            pgi=self.pgi,
            original=self.pgi.schema.get_table(original),
            supp=self.pgi.schema.get_table(supp_dataset.name),
            domain=dataset_metadata.domain,
        ).name

    def _do_relrec_merge(
        self, original: str, relrec_dataset: str, dataset_metadata: SQLDatasetMetadata, merge_spec: dict, rule: dict
    ) -> str:
        """
        Find the corresponding RELREC dataset, then perform a RELREC merge operation on the datasets.
        """
        # Find the RELREC dataset
        relrec_data = next(
            (
                dataset
                for dataset in self.datasets
                if dataset.name.upper() == "RELREC" or (dataset.domain and dataset.domain.upper() == "RELREC")
            ),
            None,
        )
        if not relrec_data:
            raise ValueError("Tried to RELREC merge, but could not find RELREC dataset.")

        wildcard = merge_spec.get("wildcard", "__")

        return SqlRelrecMerge.perform_join(
            pgi=self.pgi,
            original=self.pgi.schema.get_table(original),
            relrec=self.pgi.schema.get_table(relrec_data.name),
            domain=dataset_metadata.domain,
            wildcard=wildcard,
        ).name

    def _do_relationship_merge(
        self,
        original: str,
        relationship_dataset: str,
        dataset_metadata: SQLDatasetMetadata,
        merge_spec: dict,
        rule: dict,
    ) -> str:
        """
        Perform a relationship merge operation on the datasets.

        This handles relationship datasets like RELSUB, CO, SQ, or any dataset with relationship_columns.
        """
        # Find the relationship dataset
        relationship_data = next(
            (
                dataset
                for dataset in self.datasets
                if dataset.name.upper() == relationship_dataset.upper()
                or (dataset.domain and dataset.domain.upper() == relationship_dataset.upper())
            ),
            None,
        )
        if not relationship_data:
            raise ValueError(f"Tried to relationship merge with {relationship_dataset}, but could not find dataset.")

        relationship_columns = merge_spec.get("relationship_columns", {})
        match_keys = merge_spec.get("match_key", {})

        return SqlRelationshipMerge.perform_join(
            pgi=self.pgi,
            original=self.pgi.schema.get_table(original),
            relationship_dataset=self.pgi.schema.get_table(relationship_data.name),
            domain=relationship_dataset.upper(),
            relationship_columns=relationship_columns,
            match_keys=match_keys,
        ).name

    def _do_child_merge(self, child: str, dataset_metadata: SQLDatasetMetadata, merge_spec: dict, rule: dict) -> str:
        """
        Perform child merge: Find parent dataset and LEFT JOIN child with parent.

        Child dataset is on the left, parent on the right.
        Uses SqlChildMerge for the operation.
        """
        result_schema = SqlChildMerge.perform_merge(
            pgi=self.pgi,
            child=self.pgi.schema.get_table(child),
            child_domain=dataset_metadata.domain,
            datasets=self.datasets,
            merge_spec=merge_spec,
        )
        return result_schema.name

    # Temporarily adding this method to get the report to output
    def read_data(self, path: str):
        return None
