from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.constants.permissibility import (
    REQUIRED,
    PERMISSIBLE,
    REQUIRED_MODEL_VARIABLES,
    SEQ_VARIABLE,
    PERMISSIBILITY_KEY,
)
from abc import abstractmethod
from typing import List
import pandas as pd

from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
)

import cdisc_rules_engine.utilities.sdtm_utilities as sdtm_utilities
from collections import OrderedDict
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.exceptions.custom_exceptions import (
    EngineError,
    DatasetNotFoundError,
    ReferentialIntegrityError,
    MissingDataError,
    RuleExecutionError,
    RuleFormatError,
    InvalidMatchKeyError,
    VariableMetadataNotFoundError,
    DomainNotFoundInDefineXMLError,
    InvalidDatasetFormat,
    NumberOfAttemptsExceeded,
    InvalidDictionaryVariable,
    UnsupportedDictionaryType,
    FailedSchemaValidation,
)


class BaseOperation:
    def __init__(
        self,
        params: OperationParams,
        original_dataset: DatasetInterface,
        cache_service: CacheServiceInterface,
        data_service: DataServiceInterface,
        library_metadata: LibraryMetadataContainer = LibraryMetadataContainer(),
    ):
        self.params = params
        self.cache = cache_service
        self.data_service = data_service
        self.evaluation_dataset = original_dataset
        self.library_metadata = library_metadata

    @abstractmethod
    def _execute_operation(self):
        """Perform operation calculations."""
        pass

    def execute(self) -> DatasetInterface:
        """
        Execute the operation with error handling.
        Custom exceptions should be allowed to propagate up while other exceptions are logged.
        """
        try:
            logger.info(f"Starting operation {self.params.operation_name}")
            result = self._execute_operation()
            logger.info(f"Operation {self.params.operation_name} completed.")
            return self._handle_operation_result(result)
        except (
            EngineError,
            DatasetNotFoundError,
            ReferentialIntegrityError,
            MissingDataError,
            RuleExecutionError,
            RuleFormatError,
            InvalidMatchKeyError,
            VariableMetadataNotFoundError,
            DomainNotFoundInDefineXMLError,
            InvalidDatasetFormat,
            NumberOfAttemptsExceeded,
            InvalidDictionaryVariable,
            UnsupportedDictionaryType,
            FailedSchemaValidation,
        ) as e:
            logger.debug(f"error in operation {self.params.operation_name}: {str(e)}")
            raise
        except Exception as e:
            error_message = str(e)
            # Log unexpected errors
            logger.error(
                f"error in operation {self.params.operation_name}: {str(e)}",
                exc_info=True,
            )
            if isinstance(e, TypeError) and any(
                phrase in error_message
                for phrase in [
                    "NoneType",
                    "None",
                    "object is None",
                    "'NoneType'",
                    "None has no attribute",
                    "unsupported operand type",
                    "bad operand type",
                    "object is not",
                    "cannot be None",
                ]
            ):
                return None
            raise

    def _handle_operation_result(self, result) -> DatasetInterface:
        if self.evaluation_dataset.is_series(result):
            self.evaluation_dataset[self.params.operation_id] = result
            return self.evaluation_dataset
        elif self.params.grouping:
            return self._handle_grouped_result(result)
        elif isinstance(result, DatasetInterface):
            # Assume that the operation id has been applied and
            # result matches the length of the evaluation dataset.
            return self.evaluation_dataset.concat(result, axis=1)
        elif isinstance(result, dict):
            return self._handle_dictionary_result(result)
        else:
            # Handle single results

            self.evaluation_dataset[self.params.operation_id] = (
                self.evaluation_dataset.get_series_from_value(result)
            )
            return self.evaluation_dataset

    def _handle_grouped_result(self, result):
        # Handle grouped results
        result = result.rename(columns={self.params.target: self.params.operation_id})
        if self.params.grouping_aliases:
            result = self._rename_grouping_columns(result)
        grouping_columns = self._get_grouping_columns()
        target_columns = grouping_columns + [self.params.operation_id]
        result = result.reset_index()
        merged = self.evaluation_dataset.merge(
            result[target_columns], on=grouping_columns, how="left"
        )
        self.data_service._replace_nans_in_specified_cols_with_none(
            merged, [self.params.operation_id]
        )
        return self.evaluation_dataset.__class__(merged.data)

    def _handle_dictionary_result(self, result):
        self.evaluation_dataset[self.params.operation_id] = [result] * len(
            self.evaluation_dataset
        )
        return self.evaluation_dataset

    def _filter_data(self, data):
        # filters inputted dataframe on self.param.filter dictionary
        filtered_df = data
        for variable, value in self.params.filter.items():
            if self._is_wildcard_pattern(value):
                mask = self._apply_wildcard_filter(filtered_df[variable], value)
                filtered_df = filtered_df[mask]
            else:
                filtered_df = filtered_df[filtered_df[variable] == value]
        return self.evaluation_dataset.__class__(filtered_df)

    def _is_wildcard_pattern(self, value: str) -> bool:
        if not isinstance(value, str):
            return False
        return value.endswith("%")

    def _apply_wildcard_filter(self, series: pd.Series, pattern: str) -> pd.Series:
        prefix = pattern.rstrip("%")
        result = series.str.startswith(prefix, na=False)
        return result

    def _rename_grouping_columns(self, data):
        # Renames grouping columns to any corresponding grouping aliases columns
        return data.rename(
            columns={
                v: self.params.grouping_aliases[i]
                for i, v in enumerate(self.params.grouping)
                if 0 <= i < len(self.params.grouping_aliases)
                and self.params.grouping_aliases[i] != v
            }
        )

    def _get_grouping_columns(self) -> List[str]:
        if any(item.startswith("$") for item in self.params.grouping):
            return self._expand_operation_results_in_grouping(self.params.grouping)
        else:
            return (
                self.params.grouping
                if not self.params.grouping_aliases
                else [
                    (
                        self.params.grouping_aliases[i]
                        if 0 <= i < len(self.params.grouping_aliases)
                        else v
                    )
                    for i, v in enumerate(self.params.grouping)
                ]
            )

    def _expand_operation_results_in_grouping(self, grouping_list):
        expanded = []
        for item in grouping_list:
            if item.startswith("$") and item in self.evaluation_dataset.columns:
                operation_col = self.evaluation_dataset[item]
                first_val = operation_col.iloc[0]
                if operation_col.astype(str).nunique() == 1:
                    if isinstance(first_val, (list, tuple)):
                        expanded.extend(first_val)
                    else:
                        expanded.append(item)
                else:
                    expanded.extend(self._collect_values_from_column(operation_col))
            else:
                expanded.append(item)
        return list(dict.fromkeys(expanded))

    def _collect_values_from_column(self, operation_col):
        seen = []
        for val in operation_col:
            if val is not None:
                if isinstance(val, (list, tuple)):
                    for v in val:
                        if v not in seen:
                            seen.append(v)
                else:
                    if val not in seen:
                        seen.append(val)
        return seen

    def _get_variables_metadata_from_standard(self) -> List[dict]:
        # TODO: Update to handle other standard types: adam, cdash, etc.
        target_metadata = None
        for ds in self.params.datasets:
            if ds.unsplit_name == self.params.domain:
                target_metadata = ds
                break
        if (
            target_metadata
            and hasattr(target_metadata, "is_supp")
            and target_metadata.is_supp
        ):
            domain_for_library = "SUPPQUAL"
        elif target_metadata and "rel" in target_metadata.name.lower():
            if target_metadata.name.lower().startswith(
                "ap"
            ) and target_metadata.name.lower()[2:].startswith("rel"):
                domain_for_library = target_metadata.name[2:]
            else:
                domain_for_library = target_metadata.name
        else:
            domain_for_library = self.params.domain
        return sdtm_utilities.get_variables_metadata_from_standard(
            domain_for_library,
            self.library_metadata,
        )

    def get_allowed_variable_permissibility(self, variable_metadata: dict):
        """
        Returns the permissibility value of a variable allowed in the current domain
        """
        variable_name = variable_metadata.get("name")
        if PERMISSIBILITY_KEY in variable_metadata:
            return variable_metadata[PERMISSIBILITY_KEY]
        elif variable_name in REQUIRED_MODEL_VARIABLES:
            return REQUIRED
        elif variable_name.replace("--", self.params.domain) == SEQ_VARIABLE.replace(
            "--", self.params.domain
        ):
            return REQUIRED

        return PERMISSIBLE

    def _get_variable_names_list(self, domain, dataframe):
        # get variables metadata from the standard model
        variables_metadata: List[dict] = (
            self._get_variables_metadata_from_standard_model(domain, dataframe)
        )
        # create a list of variable names in accordance to the "ordinal" key
        variable_names_list = self._replace_variable_wildcards(
            variables_metadata, domain
        )
        return list(OrderedDict.fromkeys(variable_names_list))

    def _get_variables_metadata_from_standard_model(
        self, domain, dataframe
    ) -> List[dict]:
        """
        Gets variables metadata for the given class and domain from cache.
        The cache stores CDISC Library metadata.

        Return example:
        [
            {
               "label":"Study Identifier",
               "name":"STUDYID",
               "ordinal":"1",
               "role":"Identifier",
               ...
            },
            {
               "label":"Domain Abbreviation",
               "name":"DOMAIN",
               "ordinal":"2",
               "role":"Identifier"
            },
            ...
        ]
        """

        # TODO: Update to handle multiple standard types.

        return sdtm_utilities.get_variables_metadata_from_standard_model(
            domain=domain,
            dataframe=dataframe,
            datasets=self.params.datasets,
            dataset_path=self.params.dataset_path,
            data_service=self.data_service,
            library_metadata=self.library_metadata,
        )

    @staticmethod
    def _replace_variable_wildcards(variables_metadata, domain):
        return [var["name"].replace("--", domain) for var in variables_metadata]
