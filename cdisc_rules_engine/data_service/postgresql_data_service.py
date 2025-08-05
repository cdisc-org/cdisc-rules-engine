from dataclasses import dataclass
from datetime import datetime
from typing import Union
import pandas as pd
import logging

from pathlib import Path

from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.data_service.db_cache import DBCache
from cdisc_rules_engine.data_service.sql_data_service import SQLDataService
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.readers.data_reader import DataReader
from cdisc_rules_engine.readers.define_xml_reader import XMLReader
from cdisc_rules_engine.readers.codelist_reader import CodelistReader
from cdisc_rules_engine.readers.metadata_standards_reader import MetadataStandardsReader
from cdisc_rules_engine.utilities.ig_specification import IGSpecification

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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


class PostgresQLDataService(SQLDataService):

    def __init__(
        self,
        postgres_interface: PostgresQLInterface,
        cache: DBCache,
        ig_specs: IGSpecification,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        codelists_path: Path = None,
        metadata_standards_path: Path = None,
        terminology_paths: dict = None,
        data_dfs: dict[str, pd.DataFrame] = None,
        pre_processed_dfs: dict[str, pd.DataFrame] = None,
    ):
        super().__init__(
            ig_specs, datasets_path, define_xml_path, codelists_path, metadata_standards_path, terminology_paths
        )
        self.data_dfs = data_dfs
        self.pre_processed_dfs = pre_processed_dfs
        self.pgi = postgres_interface
        self.cache = cache

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        ig_specs: IGSpecification,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        data_dfs = {}
        metadata_rows: list[dict[str, Union[str, int, float]]] = []

        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        # create metadata table in postgres
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))

        # generate timestamp
        timestamp = datetime.now().astimezone()
        for test_dataset in test_datasets:
            # Create schema and table:
            row_dicts = [
                dict(zip(test_dataset["records"], values)) for values in zip(*test_dataset["records"].values())
            ]
            # force lower_case throughout
            table_name = test_dataset["name"].lower()
            row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

            pgi.create_table_from_data(table_name=table_name, data=row_dicts[0])
            pgi.insert_data(table_name=table_name, data=row_dicts)

            ddf = pd.DataFrame.from_records(row_dicts)
            data_dfs[table_name] = ddf

            # Collect variable metadata
            for test_variable in test_dataset["variables"]:
                name = test_dataset["name"]
                domain = ddf["domain"].iloc[0] if "domain" in ddf.columns else None
                is_supp = test_dataset["name"].startswith(SUPPLEMENTARY_DOMAINS)
                rdomain = ddf["rdomain"].iloc[0] if is_supp and "rdomain" in ddf.columns else None
                unsplit_name = PostgresQLDataService._get_unsplit_name(name, domain, rdomain)
                is_split = name != unsplit_name
                metadata_rows.append(
                    {
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "dataset_filename": test_dataset["filename"],
                        "dataset_filepath": test_dataset["filepath"],
                        "dataset_id": name.lower(),
                        "table_hash": name.lower(),
                        "dataset_name": name,
                        "dataset_label": test_dataset["label"],
                        "dataset_domain": domain,
                        "dataset_is_supp": is_supp,
                        "dataset_rdomain": rdomain,
                        "dataset_is_split": is_split,
                        "dataset_unsplit_name": unsplit_name,
                        "dataset_preprocessed": None,
                        "var_name": test_variable["name"].lower(),
                        "var_label": test_variable["label"],
                        "var_type": test_variable["type"],
                        "var_length": test_variable["length"],
                        "var_format": test_variable["format"],
                    }
                )

        # write metadata rows into DB
        pgi.insert_data(table_name="data_metadata", data=metadata_rows)

        # initialize cache
        cache = DBCache.from_metadata_dict(metadata_rows)

        pre_processed_dfs = PostgresQLDataService._pre_process_data_dfs(data_dfs)
        return cls(
            pgi,
            cache,
            ig_specs,
            datasets_path,
            define_xml_path,
            None,
            None,
            terminology_paths,
            data_dfs,
            pre_processed_dfs,
        )

    @classmethod
    def from_column_data(
        cls, table_name: str, column_data: dict[str, list[str, int, float]]
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in column_data and create corresponding SQL tables
        column_data example: {"target": [1, 2, 4], "VAR2": [3, 3, 3]}
        """
        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()
        # Create schema and table:
        row_dicts = [dict(zip(column_data, values)) for values in zip(*column_data.values())]
        table_name = table_name.lower()
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]
        pgi.create_table_from_data(table_name=table_name, data=row_dicts[0])
        pgi.insert_data(table_name=table_name, data=row_dicts)
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))
        metadata = [{"dataset_id": table_name, "var_name": var} for var in row_dicts[0].keys()]
        return cls(postgres_interface=pgi, cache=DBCache.from_metadata_dict(metadata), ig_specs=None)

    def _pre_process_data_dfs(data_dfs: dict[pd.DataFrame]) -> dict[pd.DataFrame]:
        # TODO
        """
        This method will be responsible to doing all pre-processing, like split dataset concatenation
        and relrec / related merges to move this logic out of the rule execution and perform this during
        database initialization.
        Don't forget to add the pre-processed data metadata into the metadata_df
        """
        return data_dfs

    def _create_sql_tables_from_dataset_paths(self) -> None:
        """
        Iterate through dataset files in `self.datasets_path`
        and create corresponding SQL tables.
        """
        if not self.datasets_path or not self.datasets_path.exists():
            logger.info("No datasets path provided or path doesn't exist")
            return

        self.pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))

        timestamp = datetime.now().astimezone()

        for file_path in self.datasets_path.iterdir():
            self._process_dataset_file(file_path, timestamp)

    def _process_dataset_file(self, file_path: Path, timestamp: datetime) -> None:
        """Process a single dataset file."""
        try:
            reader = DataReader(str(file_path))
            metadata_info = reader.read_metadata()

            # force table_name to be lowercase
            table_name = file_path.stem.lower()

            logger.info(f"Loading dataset {file_path.name} into table {table_name}")

            metadata_rows = []
            first_chunk_processed = False

            for chunk_data in reader.read():
                # force lowercase on columns
                chunk_data = [{k.lower(): v for k, v in row} for row in chunk_data.items()]
                if not first_chunk_processed and chunk_data:
                    first_chunk = chunk_data[0]
                    self._create_table_with_indexes(table_name, first_chunk)

                    metadata_rows = self._build_metadata_rows(
                        file_path, table_name, metadata_info, first_chunk, timestamp
                    )
                    first_chunk_processed = True

                if chunk_data:
                    self.pgi.insert_data(table_name, chunk_data)

            if metadata_rows:
                self.pgi.insert_data("data_metadata", metadata_rows)

            logger.info(f"Successfully loaded {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")

    def _create_table_with_indexes(self, table_name: str, first_chunk: dict) -> None:
        """Create table and add indexes for CDISC variables."""
        self.pgi.create_table_from_data(table_name, first_chunk)

        for col in ("usubjid", "studyid", "domain", "seq", "idvar", "idvarval"):
            if col in first_chunk:
                self.pgi.execute_sql(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col.lower()} ON {table_name}({col})"
                )

    def _build_metadata_rows(
        self, file_path: Path, table_name: str, metadata_info: dict, first_chunk: dict, timestamp: datetime
    ) -> list[dict]:
        """Build metadata rows for all variables in the dataset."""

        domain = first_chunk.get("domain", None)
        is_supp = domain.startswith(SUPPLEMENTARY_DOMAINS) if domain is not None else False
        rdomain = first_chunk.get("rdomain", None)
        unsplit_name = PostgresQLDataService._get_unsplit_name(table_name, domain, rdomain)
        is_split = table_name != unsplit_name

        metadata_rows = []
        for var_info in metadata_info["variables"]:
            metadata_rows.append(
                {
                    "created_at": timestamp,
                    "updated_at": timestamp,
                    "dataset_filename": file_path.name,
                    "dataset_filepath": str(file_path),
                    "dataset_id": table_name,
                    "dataset_name": table_name,
                    "dataset_label": metadata_info["metadata"].get("dataset_label", ""),
                    "dataset_domain": domain,
                    "dataset_is_supp": is_supp,
                    "dataset_rdomain": rdomain,
                    "dataset_is_split": is_split,
                    "dataset_unsplit_name": unsplit_name,
                    "dataset_preprocessed": None,
                    "var_name": var_info.get("name", "").lower(),
                    "var_label": var_info.get("label"),
                    "var_type": var_info.get("type") or var_info.get("ctype"),
                    "var_length": var_info.get("length"),
                    "var_format": var_info.get("format"),
                }
            )

        return metadata_rows

    def create_definexml_tables(self):
        """Create tables for Define-XML metadata"""
        if not self.define_xml_path:
            logger.info("No Define-XML path provided.")
            return

        if not self.define_xml_path.exists():
            logger.warning(f"Define-XML path {self.define_xml_path} does not exist.")
            return

        for query in (SCHEMA_PATH / "xml").glob("*.sql"):
            self.pgi.execute_sql_file(str(SCHEMA_PATH / "xml" / query))

        logger.info("Define-XML tables created successfully")

        reader = XMLReader(str(self.define_xml_path))
        xml_data = reader.read()

        try:
            insert_order = [
                "studies",
                "metadata_versions",
                "comments",
                "methods",
                "documents",
                "codelists",
                "codelist_items",
                "variables",
                "datasets",
                "dataset_variables",
                "value_lists",
                "value_list_items",
                "where_clauses",
                "where_clause_conditions",
                "variable_codelist_refs",
                "variable_value_lists",
                "analysis_results",
            ]

            for table_name in insert_order:
                if table_name in xml_data and xml_data[table_name]:
                    records = xml_data[table_name]
                    self.pgi.insert_data(table_name, records)

            logger.info("Define-XML data inserted successfully")

        except Exception as e:
            logger.error(f"Error inserting Define-XML data: {str(e)}")
            raise

    def _create_terminology_tables(self) -> None:
        """
        Iterate through self.terminology_paths dict
        and create corresponding SQL tables if paths exist.
        """
        pass

    def _create_standards_tables(self) -> None:
        """
        Create all necessary SQL tables for IG standards.
        """
        if not self.metadata_standards_path:
            logger.info("No metadata standards path provided, will use cached IG metadata")
            return

        if not self.metadata_standards_path.exists():
            logger.warning(f"Metadata standards path {self.metadata_standards_path} does not exist")
            return

        self.pgi.execute_sql_file(str(SCHEMA_PATH / "ig_datasets.sql"))
        self.pgi.execute_sql_file(str(SCHEMA_PATH / "ig_variables.sql"))

        for file_path in self.metadata_standards_path.iterdir():
            try:
                reader = MetadataStandardsReader(str(file_path))
                ig_data = reader.read()

                if ig_data.get("datasets"):
                    # TODO: lowercase all
                    self.pgi.insert_data("ig_datasets", ig_data["datasets"])

                if ig_data.get("variables"):
                    # TODO: lowercase all
                    self.pgi.insert_data("ig_variables", ig_data["variables"])

                logger.info(f"Loaded IG metadata from {file_path.name}")

            except Exception as e:
                logger.error(f"Failed to load IG metadata {file_path.name}: {e}")
                continue

    def _create_codelist_tables(self) -> None:
        """
        Create all necessary SQL tables for CDISC codelists.
        """
        if not self.codelists_path:
            logger.info("No codelists path provided, will use cached CDISC codelists")
            return

        if not self.codelists_path.exists():
            logger.warning(f"Codelists path {self.codelists_path} does not exist")
            return

        self.pgi.execute_sql_file(str(SCHEMA_PATH / "codelists.sql"))

        for file_path in self.codelists_path.iterdir():
            try:
                reader = CodelistReader(str(file_path))
                codelist_data = reader.read()

                if codelist_data:
                    # TODO: lowercase all
                    self.pgi.insert_data("codelists", codelist_data)
                    logger.info(f"Loaded codelist from {file_path.name}")

            except Exception as e:
                logger.error(f"Failed to load codelist {file_path.name}: {e}")
                continue

    def get_uploaded_dataset_ids(self) -> list[str]:
        query = "SELECT DISTINCT dataset_id FROM data_metadata;"
        self.pgi.execute_sql(query=query)
        results = self.pgi.fetch_all()
        return [res["dataset_id"] for res in results]

    def get_dataset_metadata(self, dataset_id: str) -> SQLDatasetMetadata:
        query = f"""
            SELECT *
            FROM data_metadata
            WHERE dataset_id = '{dataset_id}';
        """
        self.pgi.execute_sql(query=query)
        results = self.pgi.fetch_all()
        return SQLDatasetMetadata(
            filename=results[0].get("dataset_filename"),
            filepath=results[0].get("dataset_filepath"),
            dataset_id=results[0].get("dataset_id"),
            table_hash=results[0].get("table_hash"),
            dataset_name=results[0].get("dataset_name"),
            dataset_label=results[0].get("dataset_label"),
            unsplit_name=results[0].get("dataset_unsplit_name"),
            domain=results[0].get("dataset_domain"),
            is_supp=results[0].get("dataset_is_supp"),
            rdomain=results[0].get("dataset_rdomain"),
            variables=[res["var_name"] for res in results],
        )

    def _get_unsplit_name(
        name: str,
        domain: Union[str, None],
        rdomain: str,
    ) -> str:
        if domain:
            return domain
        if name.startswith("SUPP"):
            return f"SUPP{rdomain}"
        if name.startswith("SQ"):
            return f"SQ{rdomain}"
        return name
