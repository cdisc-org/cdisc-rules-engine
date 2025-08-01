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
    def filter_dataset_by_match_keys_of_other_dataset(
        dataset: DatasetInterface,
        dataset_match_keys: List[str],
        other_dataset: DatasetInterface,
        other_dataset_match_keys: List[str],
    ) -> DatasetInterface:
        """
        Returns a DataFrame where values of match keys of
        dataset are equal to values of match keys of other dataset.
        Example:
            dataset = USUBJID  DOMAIN
                      CDISC001 AE
                      CDISC001 AE
                      CDISC009 AE
            dataset_match_keys = ["USUBJID"]
            other_dataset = USUBJID  DOMAIN
                            CDISC009 AE
            other_dataset_match_keys = ["USUBJID"]

            The result will be: USUBJID  DOMAIN
                                CDISC009 AE
        """
        dataset_w_ind = dataset.set_index(dataset_match_keys)
        other_dataset_w_ind = other_dataset.set_index(other_dataset_match_keys)
        condition = dataset_w_ind.index.isin(other_dataset_w_ind.index)
        return dataset_w_ind[condition].reset_index()

    @staticmethod
    def filter_parent_dataset_by_supp_dataset(
        parent_dataset: DatasetInterface,
        supp_dataset: DatasetInterface,
        column_with_names: str,
        column_with_values: str,
    ) -> DatasetInterface:
        """
        A wrapper function for convenient filtering of parent dataset by supp dataset.
        Does two things:
        1. Filters parent dataset by RDOMAIN column of supp dataset.
        2. Filters parent dataset by columns of supp dataset
           that describe their relation.
        """
        parent_dataset = DataProcessor.filter_parent_dataset_by_supp_dataset_rdomain(
            parent_dataset, supp_dataset
        )
        return DataProcessor.filter_dataset_by_nested_columns_of_other_dataset(
            parent_dataset, supp_dataset, column_with_names, column_with_values
        )

    @staticmethod
    def filter_parent_dataset_by_supp_dataset_rdomain(
        parent_dataset: DatasetInterface, supp_dataset: DatasetInterface
    ) -> DatasetInterface:
        """
        Leaves only those rows in parent dataset
        where DOMAIN is the same as RDOMAIN of supp dataset.
        """
        parent_domain_values: pd.Series = parent_dataset.get("DOMAIN", pd.Series())
        supp_domain_values: pd.Series = supp_dataset.get("RDOMAIN", pd.Series())
        if parent_domain_values.empty or supp_domain_values.empty:
            return parent_dataset

        return parent_dataset[parent_domain_values.isin(supp_domain_values)]

    @staticmethod
    def filter_dataset_by_nested_columns_of_other_dataset(
        dataset: DatasetInterface,
        other_dataset: DatasetInterface,
        column_with_names: str,
        column_with_values: str,
    ) -> DatasetInterface:
        """
        other_dataset has two columns:
            1. list of column names which exist in dataset - column_with_names.
            2. list of values of the aforementioned columns - column_with_values.

        The function filters columns name of dataset by their respective column values.

        Example:
            other_dataset = IDVAR IDVARVAL
                            ECSEQ 100
                            ECSEQ 101
                            ECNUM 105
            column_with_names = "IDVAR"
            column_with_values = "IDVARVAL"

            We need to leave only those rows in dataset
            where dataset["ECSEQ"] is equal to 100 or 101
            AND dataset["ECNUM"] is equal to 105.
        """
        if (
            other_dataset[column_with_names].str.strip().eq("").all()
            and other_dataset[column_with_values].str.strip().eq("").all()
        ):
            return dataset
        grouped = other_dataset.groupby(column_with_names, group_keys=False)

        def filter_dataset_by_group_values(group) -> DatasetInterface:
            decimal_group_values: pd.Series = DataProcessor.convert_float_merge_keys(
                group[column_with_values]
            )
            decimal_dataset_values: pd.Series = DataProcessor.convert_float_merge_keys(
                dataset[group.name]
            )
            condition: pd.Series = decimal_dataset_values.isin(decimal_group_values)
            return dataset[condition]

        result = grouped.apply(lambda group: filter_dataset_by_group_values(group))
        # grouping breaks sorting, need to sort once again
        return dataset.__class__(result.sort_values(list(grouped.groups.keys())))

    @staticmethod
    def merge_datasets_on_relationship_columns(
        left_dataset: DatasetInterface,
        left_dataset_match_keys: List[str],
        right_dataset: DatasetInterface,
        right_dataset_match_keys: List[str],
        right_dataset_domain_name: str,
        column_with_names: str,
        column_with_values: str,
    ) -> DatasetInterface:
        """
        Uses full join to merge given datasets on the
        columns that describe their relation.
        """
        # right dataset holds column names of left dataset.
        # all values in the column are the same
        if (
            right_dataset[column_with_names].str.strip().eq("").all()
            and right_dataset[column_with_values].str.strip().eq("").all()
        ):
            return left_dataset.merge(
                other=right_dataset.data,
                left_on=left_dataset_match_keys,
                right_on=right_dataset_match_keys,
                how="outer",
                suffixes=("", f".{right_dataset_domain_name}"),
            )
        left_ds_col_name: str = right_dataset[column_with_names][0]

        # convert numeric columns to one data type to avoid merging errors
        # there is no point in converting string cols since their data type is the same
        DataProcessor.cast_numeric_cols_to_same_data_type(
            right_dataset, column_with_values, left_dataset, left_ds_col_name
        )
        left_dataset_match_keys.append(left_ds_col_name)
        right_dataset_match_keys.append(column_with_values)

        return left_dataset.merge(
            other=right_dataset.data,
            left_on=left_dataset_match_keys,
            right_on=right_dataset_match_keys,
            how="outer",
            suffixes=("", f".{right_dataset_domain_name}"),
        )

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
    def merge_relationship_datasets(
        left_dataset: DatasetInterface,
        left_dataset_match_keys: List[str],
        right_dataset: DatasetInterface,
        right_dataset_match_keys: List[str],
        right_dataset_domain: dict,
    ) -> DatasetInterface:
        result = DataProcessor.filter_dataset_by_match_keys_of_other_dataset(
            left_dataset,
            left_dataset_match_keys,
            right_dataset,
            right_dataset_match_keys,
        )
        result = DataProcessor.filter_parent_dataset_by_supp_dataset(
            result,
            right_dataset,
            **right_dataset_domain["relationship_columns"],
        )

        # convert result back to DatasetInterface class
        result = left_dataset.__class__(result.reset_index(drop=True))
        result = DataProcessor.merge_datasets_on_relationship_columns(
            left_dataset=result,
            left_dataset_match_keys=left_dataset_match_keys,
            right_dataset=right_dataset,
            right_dataset_match_keys=right_dataset_match_keys,
            right_dataset_domain_name=right_dataset_domain.get("domain"),
            **right_dataset_domain["relationship_columns"],
        )
        return result

    @staticmethod
    def merge_pivot_supp_dataset(
        dataset_implementation: DatasetInterface,
        left_dataset: DatasetInterface,
        right_dataset: DatasetInterface,
    ):

        static_keys = ["STUDYID", "USUBJID", "APID", "POOLID", "SPDEVID"]
        qnam_list = right_dataset["QNAM"].unique()
        right_dataset = DataProcessor.process_supp(right_dataset)
        dynamic_key = right_dataset["IDVAR"].iloc[0]

        # Determine the common keys present in both datasets
        common_keys = [
            key
            for key in static_keys
            if key in left_dataset.columns and key in right_dataset.columns
        ]
        common_keys.append(dynamic_key)
        current_supp = right_dataset.rename(columns={"IDVARVAL": dynamic_key})
        current_supp = current_supp.drop(columns=["IDVAR"])
        left_dataset[dynamic_key] = left_dataset[dynamic_key].astype(str)
        current_supp[dynamic_key] = current_supp[dynamic_key].astype(str)
        left_dataset = PandasDataset(
            pd.merge(
                left_dataset.data,
                current_supp.data,
                how="left",
                on=common_keys,
                suffixes=("", "_supp"),
            )
        )
        for qnam in qnam_list:
            qnam_check = left_dataset.data.dropna(subset=[qnam])
            grouped = qnam_check.groupby(common_keys).size()
            if (grouped > 1).any():
                raise ValueError(
                    f"Multiple records with the same QNAM '{qnam}' match a single parent record"
                )
        if dataset_implementation == DaskDataset:
            left_dataset = DaskDataset(left_dataset.data)
        return left_dataset

    @staticmethod
    def process_supp(supp_dataset):
        # TODO: QLABEL is not added to the new columns.  This functionality is not supported directly in pandas.
        # initialize new columns for each unique QNAM in the dataset with NaN
        for qnam in supp_dataset["QNAM"].unique():
            supp_dataset.data[qnam] = pd.NA
        # Set the value of the new columns only in their respective rows
        for index, row in supp_dataset.iterrows():
            supp_dataset.at[index, row["QNAM"]] = row["QVAL"]
        supp_dataset.drop(labels=["QNAM", "QVAL", "QLABEL"], axis=1)
        return supp_dataset

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
    def cast_numeric_cols_to_same_data_type(
        left_dataset: DatasetInterface,
        left_dataset_column: str,
        right_dataset: DatasetInterface,
        right_dataset_column: str,
    ):
        """
        Casts given columns to one data type (float)
        ONLY if they are numeric.
        Before casting, the method ensures that both of the
        columns hold numeric values and performs conversion
        only if both of them are.

        Example 1:
            left_dataset[left_dataset_column] = [1, 2, 3, 4, ]
            right_dataset[right_dataset_column] = ["1.0", "2.0", "3.0", "4.0", ]

        Example 2:
            left_dataset[left_dataset_column] = ["1", "2", "3", "4", ]
            right_dataset[right_dataset_column] = ["1.0", "2.0", "3.0", "4.0", ]

        Example 3:
            left_dataset[left_dataset_column] = ["1", "2", "3", "4", ]
            right_dataset[right_dataset_column] = [1, 2, 3, 4, ]
        """
        # check if both columns are numeric
        left_is_numeric: bool = DataProcessor.column_contains_numeric(
            left_dataset[left_dataset_column]
        )
        right_is_numeric: bool = DataProcessor.column_contains_numeric(
            right_dataset[right_dataset_column]
        )
        if left_is_numeric and right_is_numeric:
            # convert to float
            right_dataset[right_dataset_column] = right_dataset[
                right_dataset_column
            ].astype(float)
            left_dataset[left_dataset_column] = left_dataset[
                left_dataset_column
            ].astype(float)

    @staticmethod
    def column_contains_numeric(column: pd.Series) -> bool:
        if not pd.api.types.is_numeric_dtype(column):
            return column.str.replace(".", "").str.isdigit().all()
        return True

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
