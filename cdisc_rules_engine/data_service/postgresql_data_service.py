from dataclasses import dataclass
from datetime import datetime
from typing import Union
import pandas as pd
import pandasql as ps

from pathlib import Path

from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.data_service.sql_data_service import SQLDataService
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.test_dataset import TestDataset


@dataclass
class SQLDatasetMetadata:
    filename: str
    filepath: str
    dataset_id: str
    dataset_name: str
    dataset_label: str
    domain: str
    is_supp: bool
    rdomain: str
    variables: list[str]


class PostgresQLDataService(SQLDataService):

    def __init__(
        self,
        postgres_interface: PostgresQLInterface,
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
        data_dfs: dict[str, pd.DataFrame] = None,
        pre_processed_dfs: dict[str, pd.DataFrame] = None,
        metadata_df: pd.DataFrame = None,
    ):
        super().__init__(datasets_path, define_xml_path, terminology_paths)
        self.data_dfs = data_dfs
        self.pre_processed_dfs = pre_processed_dfs
        self.metadata_df = metadata_df
        self.psql = ps.PandaSQL()
        self.pgi = postgres_interface

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        datasets_path: Path = None,
        define_xml_path: Path = None,
        terminology_paths: dict = None,
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables, setting path to "memory"
        """
        data_dfs = {}
        metadata_df = pd.DataFrame()
        metadata_rows: list[dict[str, Union[str, int, float]]] = []

        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        # create metadata table in postgres
        pgi.execute_sql_file(str(Path(__file__).parent / "schemas" / "clinical_data_metadata_schema.sql"))

        # generate timestamp
        timestamp = datetime.now().astimezone()
        for test_dataset in test_datasets:
            # Collect content
            ddf = pd.DataFrame.from_records(test_dataset["records"])
            ddf.columns = [col for col in ddf.columns]
            data_dfs[test_dataset["name"]] = ddf

            # Collect variable metadata
            for test_variable in test_dataset["variables"]:
                name = test_dataset["name"]
                domain = ddf["DOMAIN"].iloc[0] if "DOMAIN" in ddf.columns else None
                is_supp = test_dataset["name"].startswith(SUPPLEMENTARY_DOMAINS)
                rdomain = ddf["RDOMAIN"].iloc[0] if is_supp and "RDOMAIN" in ddf.columns else None
                unsplit_name = PostgresQLDataService._get_unsplit_name(name, domain, rdomain)
                is_split = name != unsplit_name
                metadata_rows.append(
                    {
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "dataset_filename": test_dataset["filename"],
                        "dataset_filepath": test_dataset["filepath"],
                        "dataset_id": name,
                        "dataset_name": name,
                        "dataset_label": test_dataset["label"],
                        "dataset_domain": domain,
                        "dataset_is_supp": is_supp,
                        "dataset_rdomain": rdomain,
                        "dataset_is_split": is_split,
                        "dataset_unsplit_name": unsplit_name,
                        "dataset_preprocessed": None,
                        "var_name": test_variable["name"],
                        "var_label": test_variable["label"],
                        "var_type": test_variable["type"],
                        "var_length": test_variable["length"],
                        "var_format": test_variable["format"],
                    }
                )
                new_row = pd.DataFrame(
                    {
                        "filename": [test_dataset["filename"]],
                        "filepath": [test_dataset["filepath"]],
                        "dataset_id": [test_dataset["name"]],
                        "dataset_name": [test_dataset["name"]],
                        "dataset_label": [test_dataset["label"]],
                        "domain": [test_dataset["domain"]],
                        "name": [test_variable["name"]],
                        "label": [test_variable["label"]],
                        "type": [test_variable["type"]],
                        "length": [test_variable["length"]],
                        "format": [test_variable["format"]],
                    }
                )
                metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)

        # write metadata rows into DB
        pgi.insert_data(table_name="data_metadata", data=metadata_rows)

        pre_processed_dfs = PostgresQLDataService._pre_process_data_dfs(data_dfs)
        return cls(pgi, datasets_path, define_xml_path, terminology_paths, data_dfs, pre_processed_dfs, metadata_df)

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
        pass

    def _create_definexml_tables(self) -> None:
        """
        Read the self.define_xml_path and create corresponding SQL tables.
        """
        pass

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
        pass

    def _create_codelist_tables(self) -> None:
        """
        Create all necessary SQL tables for CDISC codelists.
        """
        pass

    def get_uploaded_dataset_ids(self) -> list[str]:
        metadata_df = self.metadata_df
        query = "SELECT DISTINCT dataset_id FROM metadata_df"
        result_df = self._safe_psql(query, {"metadata_df": metadata_df})
        if result_df.empty:
            return False
        domains = result_df["dataset_id"].to_list()
        return domains

    def get_dataset_metadata(self, dataset_id: str) -> SQLDatasetMetadata:
        metadata_df = self.metadata_df
        query = f"""
            SELECT *
            FROM metadata_df
            WHERE dataset_id = '{dataset_id}'
        """
        result_df = self._safe_psql(query, {"metadata_df": metadata_df})
        return SQLDatasetMetadata(
            filename=result_df["filename"].iloc[0],
            filepath=result_df["filepath"].iloc[0],
            dataset_id=result_df["dataset_id"].iloc[0],
            dataset_name=result_df["dataset_name"].iloc[0],
            dataset_label=result_df["dataset_label"].iloc[0],
            domain=result_df["domain"].iloc[0],
            is_supp=str(result_df["domain"].iloc[0]).startswith(SUPPLEMENTARY_DOMAINS),
            rdomain=self.get_rdomain(result_df["dataset_id"].iloc[0]),
            variables=result_df["name"].to_list(),
        )

    def get_rdomain(self, dataset_id: str) -> Union[str, None]:
        """
        Return dataset rdomain based on dataset_id.
        """
        return self._get_first_col_value_from_data(dataset_id, "RDOMAIN")

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

    def _get_first_col_value_from_data(self, dataset_id: str, col: str) -> Union[str, None]:
        dataset = self.data_dfs.get(dataset_id, None)
        if dataset is None:
            return None
        query = f"""
            SELECT {col}
            FROM dataset
            LIMIT 1
        """
        result_df = self._safe_psql(query, {"dataset": dataset})
        if result_df.empty:
            return None
        ret = result_df[col].iat[0]
        return ret

    def _safe_psql(self, query: str, env: dict) -> pd.DataFrame:
        try:
            return self.psql(query, env)
        except ps.PandaSQLException:
            return pd.DataFrame()

    def _get_val_from_var_from_metadata(self, dataset_id: str, col: str) -> Union[str, None]:
        metadata_df = self.metadata_df
        query = f"""
            SELECT {col}
            FROM metadata_df
            WHERE dataset_id = '{dataset_id}'
            LIMIT 1
        """
        result_df = self._safe_psql(query, {"metadata_df": metadata_df})
        if result_df.empty:
            return None
        ret = result_df[col].iat[0]
        return ret
