from __future__ import annotations
from typing import List, Optional, TYPE_CHECKING

from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
import pandas as pd

from cdisc_rules_engine.config import config
from cdisc_rules_engine.enums.join_types import JoinTypes
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.services.data_services import (
    DataServiceFactory,
    DummyDataService,
)
from cdisc_rules_engine.utilities.utils import (
    search_in_list_of_dicts,
)
from cdisc_rules_engine.utilities.sdtm_utilities import add_variable_wildcards

if TYPE_CHECKING:
    from cdisc_rules_engine.utilities.dataset_preprocessor import DatasetPreprocessor


class DataProcessor:
    def __init__(self, data_service=None, cache: CacheServiceInterface = None):
        self.cache = cache or CacheServiceFactory(config).get_cache_service()
        self.data_service = (
            data_service or DataServiceFactory(config, self.cache).get_data_service()
        )
        self.dataset_implementation = self.data_service.dataset_implementation

    @staticmethod
    def convert_float_merge_keys(series: pd.Series) -> pd.Series:
        """
        Converts all values of the given series to float and then to string.
        This is needed to avoid merging errors when merging datasets
        with numeric columns that have different data types.
        For example, merging "2" with 2.0
        """
        return series.astype(float, errors="ignore").astype(str)

    @staticmethod
    def filter_if_present(df: DatasetInterface, col: str, filter_value):
        """
        If a filter value is provided, filter the dataframe on that value.
        Otherwise, return the whole dataframe.
        """
        try:
            filter_value = float(filter_value)
        except TypeError:
            pass
        except ValueError:
            pass
        return (
            df.from_dict(
                df[
                    DataProcessor.convert_float_merge_keys(df[col]) == str(filter_value)
                ].to_dict()
            )
            if filter_value
            else df
        )

    @staticmethod
    def filter_relrec_for_domain(
        domain_name: str,
        relrec_dataset: DatasetInterface,
    ):
        """
        Find all relrec records associated with a given domain
        """
        relrec_left = relrec_dataset[relrec_dataset["RDOMAIN"] == domain_name]
        left_relids_index = pd.MultiIndex.from_arrays(
            [relrec_left[col] for col in ["USUBJID", "RELID"]]
        )
        all_relids_index = pd.MultiIndex.from_arrays(
            [relrec_dataset[col] for col in ["USUBJID", "RELID"]]
        )
        relrec_right = relrec_dataset[all_relids_index.isin(left_relids_index)]
        relrec_right = relrec_right[relrec_right["RDOMAIN"] != domain_name]
        return relrec_left.merge(
            right=relrec_right,
            on=("STUDYID", "USUBJID", "RELID"),
            suffixes=("_LEFT", "_RIGHT"),
        )

    @staticmethod
    def merge_on_relrec_record(
        relrec_row: pd.Series,
        left_dataset: DatasetInterface,
        datasets: List[dict],
        dataset_preprocessor: DatasetPreprocessor,
        wildcard: str,
    ):
        """
        Given a pair of relrec rows and a left dataset, find the right dataset and join
          the left and right on the criteria given in the relrec pair
        """
        model_metadata = (
            dataset_preprocessor._data_service.library_metadata.model_metadata
        )
        file_info: SDTMDatasetMetadata = search_in_list_of_dicts(
            datasets, lambda item: item.domain == relrec_row["RDOMAIN_RIGHT"]
        )
        if not file_info:
            return DatasetInterface()
        right_dataset: DatasetInterface = dataset_preprocessor._download_dataset(
            file_info.filename
        )
        variables_with_wildcards = {
            source: f"RELREC.{target}"
            for (source, target) in add_variable_wildcards(
                model_metadata,
                right_dataset.columns,
                relrec_row["RDOMAIN_RIGHT"],
                wildcard,
            ).items()
        }
        left_subset = left_dataset
        right_subset = right_dataset
        left_subset = DataProcessor.filter_if_present(
            left_subset, "USUBJID", relrec_row["USUBJID"]
        )
        right_subset = DataProcessor.filter_if_present(
            right_subset, "USUBJID", relrec_row["USUBJID"]
        )
        if relrec_row["IDVARVAL_LEFT"] and relrec_row["IDVARVAL_RIGHT"]:
            left_subset = DataProcessor.filter_if_present(
                left_subset, relrec_row["IDVAR_LEFT"], relrec_row["IDVARVAL_LEFT"]
            )
            right_subset = DataProcessor.filter_if_present(
                right_subset, relrec_row["IDVAR_RIGHT"], relrec_row["IDVARVAL_RIGHT"]
            )
            left_on = ["STUDYID", "USUBJID"]
            right_on = [
                variables_with_wildcards["STUDYID"],
                variables_with_wildcards["USUBJID"],
            ]
        else:
            left_on = ["STUDYID", "USUBJID", relrec_row["IDVAR_LEFT"]]
            right_on = [
                variables_with_wildcards["STUDYID"],
                variables_with_wildcards["USUBJID"],
                variables_with_wildcards[relrec_row["IDVAR_RIGHT"]],
            ]
        right_subset = right_subset.rename(columns=variables_with_wildcards)
        result = left_subset.merge(
            other=right_subset.data,
            left_on=left_on,
            right_on=right_on,
        )
        return result

    @staticmethod
    def merge_relrec_datasets(
        left_dataset: DatasetInterface,
        left_dataset_domain_name: str,
        relrec_dataset: DatasetInterface,
        datasets: List[dict],
        dataset_preprocessor: DatasetPreprocessor,
        wildcard: str,
    ) -> DatasetInterface:
        """
        1. Find each record within relrec_dataset where RDOMAIN matches the
          left_dataset_domain_name
        2. Join each of these (left) records with all other (right) records in
          relrec_dataset sharing the same STUDYID, USUBJID, RELID
        3. For each record in this new dataset:
            1. Filter the left and right datasets by the criteria
            2. Rename the right dataset columns with wildcards and a "RELREC." dataset
              specifier
            3. Join the records referenced by the left side with the records referenced
              by the right side
            4. Union the results
        """
        relrec_for_domain = DataProcessor.filter_relrec_for_domain(
            left_dataset_domain_name, relrec_dataset
        )

        # TODO: FIX
        objs = [
            DataProcessor.merge_on_relrec_record(
                relrec_row, left_dataset, datasets, dataset_preprocessor, wildcard
            )
            for _, relrec_row in relrec_for_domain.iterrows()
        ]
        result = objs[0].concat(objs[1:], ignore_index=True)
        return result

    @staticmethod
    def merge_pivot_supp_dataset(
        dataset_implementation: DatasetInterface,
        left_dataset: DatasetInterface,
        right_dataset: DatasetInterface,
    ):

        static_keys = ["STUDYID", "USUBJID", "APID", "POOLID", "SPDEVID"]
        qnam_list = right_dataset["QNAM"].unique()
        unique_idvar_values = right_dataset["IDVAR"].unique()
        if len(unique_idvar_values) == 1:
            right_dataset = DataProcessor.process_supp(right_dataset)
            dynamic_key = right_dataset["IDVAR"].iloc[0]
            is_blank = pd.isna(dynamic_key) or str(dynamic_key).strip() == ""
            # Determine the common keys present in both datasets
            common_keys = [
                key
                for key in static_keys
                if key in left_dataset.columns and key in right_dataset.columns
            ]
            if not is_blank:
                common_keys.append(dynamic_key)
                current_supp = right_dataset.rename(columns={"IDVARVAL": dynamic_key})
                current_supp = current_supp.drop(columns=["IDVAR"])
                left_dataset[dynamic_key] = left_dataset[dynamic_key].astype(str)
                current_supp[dynamic_key] = current_supp[dynamic_key].astype(str)
            else:
                columns_to_drop = [
                    col for col in ["IDVAR", "IDVARVAL"] if col in right_dataset.columns
                ]
                current_supp = right_dataset.drop(columns=columns_to_drop)
            if dataset_implementation == DaskDataset:
                current_supp = DaskDataset(current_supp.data)
                left_dataset = left_dataset.merge(
                    other=current_supp.data,
                    how="left",
                    on=common_keys,
                    suffixes=("", "_supp"),
                )
                DataProcessor._validate_qnam_dask(left_dataset, qnam_list, common_keys)
            else:
                left_dataset = PandasDataset(
                    pd.merge(
                        left_dataset.data,
                        current_supp.data,
                        how="left",
                        on=common_keys,
                        suffixes=("", "_supp"),
                    )
                )
                DataProcessor._validate_qnam(left_dataset.data, qnam_list, common_keys)
        else:
            if dataset_implementation == DaskDataset:
                left_dataset = PandasDataset(left_dataset.data.compute())
                right_pandas = PandasDataset(right_dataset.data.compute())
                left_dataset = DataProcessor._merge_supp_with_multiple_idvars(
                    left_dataset, right_pandas, static_keys, qnam_list
                )
                left_dataset = DaskDataset(left_dataset.data)
            else:
                left_dataset = DataProcessor._merge_supp_with_multiple_idvars(
                    left_dataset, right_dataset, static_keys, qnam_list
                )
        return left_dataset

    @staticmethod
    def _merge_supp_with_multiple_idvars(
        left_dataset: DatasetInterface,
        right_dataset: DatasetInterface,
        static_keys: List[str],
        qnam_list: list,
    ) -> DatasetInterface:
        result_dataset = left_dataset

        # Process each IDVAR group separately
        for idvar_value in right_dataset["IDVAR"].unique():
            group_data = right_dataset.data[
                right_dataset.data["IDVAR"] == idvar_value
            ].copy()
            group_qnam_list = group_data["QNAM"].unique()
            for qnam in group_qnam_list:
                group_data[qnam] = pd.NA
            for index, row in group_data.iterrows():
                group_data.at[index, row["QNAM"]] = row["QVAL"]
            columns_to_drop = [
                col
                for col in ["QNAM", "QVAL", "QLABEL", "IDVAR"]
                if col in group_data.columns
            ]
            group_data = group_data.drop(columns=columns_to_drop)
            group_data = group_data.rename(columns={"IDVARVAL": idvar_value})
            common_keys = [
                key
                for key in static_keys
                if key in result_dataset.columns and key in group_data.columns
            ]
            common_keys.append(idvar_value)

            agg_dict = {
                col: lambda x: x.dropna().iloc[0] if not x.dropna().empty else pd.NA
                for col in group_data.columns
                if col not in common_keys
            }
            group_data = group_data.groupby(
                common_keys, as_index=False, dropna=False
            ).agg(agg_dict)
            cols_to_drop = [
                col
                for col in group_data.columns
                if col in result_dataset.columns and col not in common_keys
            ]
            if cols_to_drop:
                group_data = group_data.drop(columns=cols_to_drop)
            result_dataset[idvar_value] = result_dataset[idvar_value].astype(str)
            group_data[idvar_value] = group_data[idvar_value].astype(str)

            result_dataset = PandasDataset(
                pd.merge(
                    result_dataset.data,
                    group_data,
                    how="left",
                    on=common_keys,
                    suffixes=("", "_supp"),
                )
            )
        for qnam in qnam_list:
            if qnam not in result_dataset.columns:
                continue
            qnam_check = result_dataset.data.dropna(subset=[qnam])
            if len(qnam_check) == 0:
                continue
            idvar_for_qnam = right_dataset.data[right_dataset.data["QNAM"] == qnam][
                "IDVAR"
            ].iloc[0]
            validation_keys = [
                key for key in static_keys if key in result_dataset.columns
            ]
            validation_keys.append(idvar_for_qnam)
            grouped = qnam_check.groupby(validation_keys).size()
            if (grouped > 1).any():
                raise ValueError(
                    f"Multiple records with the same QNAM '{qnam}' match a single parent record"
                )
        return result_dataset

    @staticmethod
    def process_supp(supp_dataset):
        # TODO: QLABEL is not added to the new columns.  This functionality is not supported directly in pandas.
        # initialize new columns for each unique QNAM in the dataset with NaN
        is_dask = isinstance(supp_dataset, DaskDataset)
        if is_dask:
            supp_dataset = PandasDataset(supp_dataset.data.compute())
        for qnam in supp_dataset["QNAM"].unique():
            supp_dataset.data[qnam] = pd.NA
        # Set the value of the new columns only in their respective rows
        for index, row in supp_dataset.iterrows():
            supp_dataset.at[index, row["QNAM"]] = row["QVAL"]
        columns_to_drop = [
            col for col in ["QNAM", "QVAL", "QLABEL"] if col in supp_dataset.columns
        ]
        if columns_to_drop:
            supp_dataset = supp_dataset.drop(labels=columns_to_drop, axis=1)
        return supp_dataset

    @staticmethod
    def _validate_qnam(
        data: pd.DataFrame,
        qnam_list: list,
        common_keys: List[str],
    ):
        for qnam in qnam_list:
            if qnam not in data.columns:
                continue
            qnam_check = data.dropna(subset=[qnam])
            if len(qnam_check) == 0:
                continue
            grouped = qnam_check.groupby(common_keys).size()
            if (grouped > 1).any():
                raise ValueError(
                    f"Multiple records with the same QNAM '{qnam}' match a single parent record"
                )

    @staticmethod
    def _validate_qnam_dask(
        left_dataset: DaskDataset,
        qnam_list: list,
        common_keys: List[str],
    ):
        for qnam in qnam_list:
            if qnam not in left_dataset.columns:
                continue
            qnam_check = left_dataset.data[~left_dataset.data[qnam].isna()]
            if len(qnam_check) == 0:
                continue
            grouped_counts = qnam_check.groupby(common_keys).size()
            problem_groups = grouped_counts[grouped_counts > 1]
            problem_groups_computed = problem_groups.compute()
            if len(problem_groups_computed) > 0:
                raise ValueError(
                    f"Multiple records with the same QNAM '{qnam}' match a single parent record. "
                )

    @staticmethod
    def merge_sdtm_datasets(
        left_dataset: DatasetInterface,
        right_dataset: DatasetInterface,
        left_dataset_match_keys: List[str],
        right_dataset_match_keys: List[str],
        right_dataset_domain_name: str,
        join_type: JoinTypes,
    ) -> DatasetInterface:
        result = left_dataset.merge(
            right_dataset.data,
            how=join_type.value,
            left_on=left_dataset_match_keys,
            right_on=right_dataset_match_keys,
            suffixes=("", f".{right_dataset_domain_name}"),
            indicator=(
                False
                if join_type is JoinTypes.INNER
                else f"_merge_{right_dataset_domain_name}"
            ),
        )
        result["index"] = [i for i in range(len(result))]
        result.data = result.data.set_index("index")
        if join_type is JoinTypes.LEFT:
            if "left_only" in result[f"_merge_{right_dataset_domain_name}"].values:
                DummyDataService._replace_nans_in_numeric_cols_with_none(result)
                result.data.loc[
                    result[f"_merge_{right_dataset_domain_name}"] == "left_only",
                    result.columns.symmetric_difference(
                        left_dataset.columns.union(
                            [f"_merge_{right_dataset_domain_name}"]
                        )
                    ),
                ] = None
        return result

    @staticmethod
    def filter_dataset_columns_by_metadata_and_rule(
        columns: List[str],
        define_metadata: List[dict],
        library_metadata: dict,
        rule: dict,
    ) -> List[str]:
        """
        Leaves only those variables where:
            variable origin type is the same as in rule and
            variable core status is the same as in rule
        """
        targets: List[str] = []
        for column in columns:
            if DataProcessor.column_metadata_equal_to_define_and_library(
                column, define_metadata, library_metadata, rule
            ):
                targets.append(column)
        return targets

    @staticmethod
    def column_metadata_equal_to_define_and_library(
        column: str,
        define_metadata: List[dict],
        library_metadata: dict,
        rule: dict,
    ) -> bool:
        define_variable_metadata: Optional[dict] = search_in_list_of_dicts(
            define_metadata, lambda item: item.get("define_variable_name") == column
        )
        if not define_variable_metadata:
            return False
        equal_origin_type: bool = define_variable_metadata[
            "define_variable_origin_type"
        ] == rule.get("variable_origin_type")
        equal_core_status: bool = library_metadata.get(column, {}).get(
            "core"
        ) == rule.get("variable_core_status")
        return equal_core_status and equal_origin_type

    @staticmethod
    def is_dummy_data(data_service: DataServiceInterface) -> bool:
        return isinstance(data_service, DummyDataService)
