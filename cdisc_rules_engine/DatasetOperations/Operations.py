import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.delayed

from typing import Generator, Counter
from cdisc_rules_engine.DatasetOperations import helpers
import numpy as np
from uuid import uuid4
import os
import asyncio

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.constants.permissibility import (
    REQUIRED,
    PERMISSIBLE,
    REQUIRED_MODEL_VARIABLES,
    SEQ_VARIABLE,
    PERMISSIBILITY_KEY,
    EXPECTED,
)
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_variable_names import (
    WhodrugVariableNames,
)
from typing import List

from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.utilities.utils import get_meddra_code_term_pairs_cache_key
from typing import Optional, Tuple


import cdisc_rules_engine.utilities.sdtm_utilities as sdtm_utilities
from cdisc_rules_engine import config
from collections import OrderedDict
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_record_types import (
    WhodrugRecordTypes,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.utilities.utils import (
    get_corresponding_datasets,
    is_split_dataset,
    search_in_list_of_dicts,
)
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
from cdisc_rules_engine.models.dictionaries.meddra.meddra_validator import (
    MedDRAValidator,
)
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes


class DatasetOperations:
    def __init__(self):
        self._operations_map = {
            "distinct": self.Distinct,
            "dy": self.DayDataValidator,
            "extract_metadata": self.ExtractMetadata,
            "get_column_order_from_dataset": self.DatasetColumnOrder,
            "get_column_order_from_library": self.LibraryColumnOrder,
            "get_codelist_attributes": self.CodeListAttributes,
            "get_model_column_order": self.LibraryModelColumnOrder,
            "get_model_filtered_variables": self.LibraryModelVariablesFilter,
            "get_parent_model_column_order": self.ParentLibraryModelColumnOrder,
            "max": self.Maximum,
            "max_date": self.MaxDate,
            "mean": self.Mean,
            "min": self.Minimum,
            "min_date": self.MinDate,
            "record_count": self.RecordCount,
            "valid_meddra_code_references": self.MedDRACodeReferencesValidator,
            "valid_whodrug_references": self.WhodrugReferencesValidator,
            "whodrug_code_hierarchy": self.WhodrugHierarchyValidator,
            "valid_meddra_term_references": self.MedDRATermReferencesValidator,
            "valid_meddra_code_term_pairs": self.MedDRACodeTermPairsValidator,
            "variable_exists": self.VariableExists,
            "variable_names": self.VariableNames,
            "variable_library_metadata": self.VariableLibraryMetadata,
            "variable_value_count": self.VariableValueCount,
            "variable_count": self.VariableCount,
            "variable_is_null": self.VariableIsNull,
            "domain_is_custom": self.DomainIsCustom,
            "domain_label": self.DomainLabel,
            "required_variables": self.RequiredVariables,
            "expected_variables": self.ExpectedVariables,
            "permissible_variables": self.PermissibleVariables,
            "study_domains": self.StudyDomains,
            "valid_codelist_dates": self.ValidCodelistDates,
            "label_referenced_variable_metadata": self.LabelReferencedVariableMetadata,
            "name_referenced_variable_metadata": self.NameReferencedVariableMetadata,
            "define_variable_metadata": self.DefineVariableMetadata,
            "valid_external_dictionary_value": self.ValidExternalDictionaryValue,
        }
        self.library = None
        self.params: OperationParams = None
        self.original_dataset: pd.DataFrame = None
        self.cache_service: CacheServiceInterface = None
        self.data_service: DataServiceInterface = None
        self.library_metadata: LibraryMetadataContainer = LibraryMetadataContainer()

        self.evaluation_dataset = None

    def _set_library(self, original_dataset):
        if isinstance(original_dataset, pd.DataFrame):
            self.library = "pandas"
        elif isinstance(original_dataset, dd.DataFrame):
            self.library = "dask"
        else:
            raise ValueError(
                "Unsupported library. Currently supported: 'pandas', 'dask'."
            )

    def get_service(
        self,
        name,
        operation_params,
        original_dataset,
        cache,
        data_service,
        library_metadata=None,
    ):
        """Calls the appropriate operation function depending upoin the name"""

        self.params = operation_params
        self.original_dataset = original_dataset
        self.cache_service = cache
        self.data_service = data_service
        self.library_metadata = (
            library_metadata if library_metadata else LibraryMetadataContainer()
        )

        self._set_library(self.original_dataset)
        self.evaluation_dataset = self.original_dataset

        if name in self._operations_map:
            result = self._operations_map.get(name)()
            return self._handle_operation_result(result)
        raise ValueError(
            f"Operation name must be in  {list(self._operations_map.keys())}, "
            f"given operation name is {name}"
        )

    def Distinct(self):
        if not self.params.grouping:
            if self.library == "pandas":
                data = self.params.dataframe[self.params.target].unique()
            elif self.library == "dask":
                data = self.params.dataframe[self.params.target].compute().unique()
            if isinstance(data[0], bytes):
                data = data.astype(str)
            result = set(data)
        else:
            if self.library == "pandas":
                grouped = self.params.dataframe.groupby(
                    self.params.grouping, as_index=False
                )
                result = grouped[self.params.target].agg(
                    lambda x: pd.Series([set(x.unique())])
                )
            elif self.library == "dask":
                grouped = self.params.dataframe.compute().groupby(
                    self.params.grouping, as_index=False
                )
                print(type(grouped))
                result = grouped[self.params.target].agg(
                    lambda x: pd.Series([set(x.unique())])
                )
        return result

    def DayDataValidator(self):
        dtc_value = self.evaluation_dataset[self.params.target].map(
            helpers.parse_timestamp
        )

        # Always get RFSTDTC column from DM dataset.
        dm_datasets = [
            dataset for dataset in self.params.datasets if dataset["domain"] == "DM"
        ]
        if not dm_datasets:
            # Return none for all values if dm is not provided.
            if self.library == "dask":
                return da.full(len(self.evaluation_dataset), 0, dtype=int)
            else:  # Assuming the default is "pandas"
                return np.zeros(len(self.evaluation_dataset), dtype=int)
        if len(dm_datasets) > 1:
            files = [dataset["filename"] for dataset in dm_datasets]
            dm_data = self.data_service.join_split_datasets(files)
        else:
            dm_data = self.data_service.get_dataset(dm_datasets[0]["filename"])

        new_dataset = self.evaluation_dataset.merge(
            dm_data[["USUBJID", "RFSTDTC"]], on="USUBJID", suffixes=("", "_dm")
        )
        rfstdtc_value = "RFSTDTC"
        if "RFSTDTC_dm" in new_dataset:
            rfstdtc_value = "RFSTDTC_dm"

        if self.library == "dask":
            # Convert timestamp columns to Dask delayed objects and calculate delta
            dtc_value = dask.delayed(dtc_value)
            rfstdtc = dask.delayed(
                new_dataset[rfstdtc_value].map(helpers.parse_timestamp)
            )
            delta = (dtc_value - rfstdtc).map(helpers.get_day_difference)

            # Explicitly replace NaN values with an empty string for Dask
            delta = delta.fillna("")

            # Use dask.compute to execute the computation and return a Dask array
            result = delta.compute()
        elif self.library == "pandas":
            delta = (
                dtc_value - new_dataset[rfstdtc_value].map(helpers.parse_timestamp)
            ).map(helpers.get_day_difference)

            result = delta.replace(np.nan, "")

        return result

    def ExtractMetadata(self):
        # get metadata
        metadata: pd.DataFrame = self.data_service.get_dataset_metadata(
            dataset_name=self.params.dataset_path
        )

        # extract target value. Metadata df always has one row
        target_value = metadata.get(self.params.target, pd.Series())[0]
        return target_value

    def DatasetColumnOrder(self):
        """
        Returns dataset columns as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        """
        # if self.library == "pandas":
        #     result=self.params.dataframe.columns.to_list()
        # elif self.library == "dask":
        #     result=self.params.dataframe.columns.to_list()
        # else:
        #     raise ValueError("Unsupported dataframe type")
        result = self.params.dataframe.columns.to_list()
        return result

    def LibraryColumnOrder(self):
        """
        Fetches column order for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """

        # get variables metadata from the standard model
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()

        # create a list of variable names in accordance to the "ordinal" key
        variable_names_list = [
            var["name"].replace("--", self.params.domain) for var in variables_metadata
        ]
        return list(OrderedDict.fromkeys(variable_names_list))

    def CodeListAttributes(self):
        return helpers._get_codelist_attributes(
            self.params, self.library_metadata, self.cache_service
        )

    def LibraryModelColumnOrder(self):
        """
        Fetches column order for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """
        return self._get_variable_names_list(self.params.domain, self.params.dataframe)

    def LibraryModelVariablesFilter(self):
        """
        Fetches column order for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """

        return self._get_model_filtered_variables()

    def ParentLibraryModelColumnOrder(self):
        """
        Fetches column order for a supp's parent domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """
        domain_to_datasets = helpers._get_domain_to_datasets(self.params)
        rdomain_names_list = {}
        if self.library == "dask":
            return pd.Series(
                rdomain_names_list.setdefault(
                    rdomain,
                    self._get_parent_variable_names_list(
                        domain_to_datasets, rdomain, data_service=self.data_service
                    ),
                )
                for rdomain in self.params.dataframe.compute().get(
                    "RDOMAIN", [None] * len(self.params.dataframe)
                )
            )
        else:
            return pd.Series(
                rdomain_names_list.setdefault(
                    rdomain,
                    self._get_parent_variable_names_list(
                        domain_to_datasets, rdomain, data_service=self.data_service
                    ),
                )
                for rdomain in self.params.dataframe.get(
                    "RDOMAIN", [None] * len(self.params.dataframe)
                )
            )

    def Maximum(self):
        if self.library == "dask":
            self.params.dataframe = self.params.dataframe.compute()
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].max()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).max()
        return result

    def MaxDate(self):
        if self.library == "dask":
            self.params.dataframe = self.params.dataframe.compute()
        if not self.params.grouping:
            data = pd.to_datetime(self.params.dataframe[self.params.target])
            max_date = data.max()
            if isinstance(max_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = max_date.isoformat()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).max()
        return result

    def Mean(self):
        if self.library == "dask":
            self.params.dataframe = self.params.dataframe.compute()
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].mean()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).mean()
        return result

    def Minimum(self):
        if self.library == "dask":
            self.params.dataframe = self.params.dataframe.compute()
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].min()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).min()
        return result

    def MinDate(self):
        if not self.params.grouping:
            print("in grouping")
            data = pd.to_datetime(self.params.dataframe[self.params.target])
            min_date = data.min()
            if isinstance(min_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = min_date.isoformat()
        else:
            print("not in grouping")
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).min()
        return result

    def RecordCount(self):
        """
        Returns number of records in the dataset as pd.Series like:
        0    5
        1    5
        2    5
        3    5
        4    5
        dtype: int64
        """
        record_count: int = len(self.params.dataframe)
        return record_count

    def MedDRACodeReferencesValidator(self):
        # get metadata
        if not self.params.meddra_path:
            raise ValueError("Can't execute the operation, no meddra path provided")
        code_variables = [
            MedDRAVariables.SOCCD.value,
            MedDRAVariables.HLGTCD.value,
            MedDRAVariables.HLTCD.value,
            MedDRAVariables.PTCD.value,
            MedDRAVariables.LLTCD.value,
        ]
        code_strings = [
            f"{self.params.domain}{variable}" for variable in code_variables
        ]
        cache_key = f"meddra_valid_code_hierarchies_{self.params.meddra_path}"
        valid_code_hierarchies = self.cache_service.get(cache_key)
        if not valid_code_hierarchies:
            terms: dict = self.cache_service.get(self.params.meddra_path)
            valid_code_hierarchies = MedDRATerm.get_code_hierarchies(terms)
            self.cache_service.add(cache_key, valid_code_hierarchies)
        column = str(uuid4()) + "_codes"
        self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
            "/".join, axis=1
        )
        result = self.params.dataframe[column].isin(valid_code_hierarchies)
        return result

    def WhodrugReferencesValidator(self):
        # get metadata
        """
        Checks if a reference to whodrug term points
        to the existing code in Atc Text (INA) file.
        """
        if not self.params.whodrug_path:
            raise ValueError("Can't execute the operation, no whodrug path provided")

        terms: dict = self.cache_service.get(self.params.whodrug_path)
        valid_codes: Generator = (
            term.code for term in terms[WhodrugRecordTypes.ATC_TEXT.value].values()
        )
        if self.library == "dask":
            result = self.params.dataframe.compute()[self.params.target].isin(
                valid_codes
            )
        elif self.library == "pandas":
            result = self.params.dataframe[self.params.target].isin(valid_codes)
        return result

    def WhodrugHierarchyValidator(self):
        # get metadata
        if not self.params.whodrug_path:
            raise ValueError("Can't execute the operation, no whodrug path provided")

        terms: dict = self.cache_service.get(self.params.whodrug_path)
        code_variables = [
            WhodrugVariableNames.DRUG_NAME.value,
            WhodrugVariableNames.ATC_TEXT.value,
            WhodrugVariableNames.ATC_CLASSIFICATION.value,
        ]
        code_strings = [
            f"{self.params.domain}{variable}" for variable in code_variables
        ]
        valid_code_hierarchies = helpers.get_code_hierarchies(terms)
        column = str(uuid4()) + "_codes"
        if self.library == "dask":
            self.params.dataframe = self.params.dataframe.compute()
            self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
                "/".join, axis=1
            )
        else:
            self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
                "/".join, axis=1
            )
        result = self.params.dataframe[column].isin(valid_code_hierarchies)
        return result

    def MedDRATermReferencesValidator(self):
        # get metadata
        if not self.params.meddra_path:
            raise ValueError("Can't execute the operation, no meddra path provided")
        code_variables = [
            MedDRAVariables.SOC.value,
            MedDRAVariables.HLGT.value,
            MedDRAVariables.HLT.value,
            MedDRAVariables.DECOD.value,
            MedDRAVariables.LLT.value,
        ]
        code_strings = [
            f"{self.params.domain}{variable}" for variable in code_variables
        ]
        cache_key = f"meddra_valid_term_hierarchies_{self.params.meddra_path}"
        valid_term_hierarchies = self.cache_service.get(cache_key)
        if not valid_term_hierarchies:
            terms: dict = self.cache_service.get(self.params.meddra_path)
            valid_term_hierarchies = MedDRATerm.get_term_hierarchies(terms)
            self.cache_service.add(cache_key, valid_term_hierarchies)
        column = str(uuid4()) + "_terms"
        self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
            "/".join, axis=1
        )
        result = self.params.dataframe[column].isin(valid_term_hierarchies)
        return result

    def VariableLibraryMetadata(self):
        """
        Get the variable permissibility values for all data in the current
        dataset.
        """
        variable_details: dict = self.library_metadata.variables_metadata
        if not variable_details:
            cdisc_library_service = helpers.CDISCLibraryService(
                config, self.cache_service
            )
            variable_details = cdisc_library_service.get_variables_details(
                self.params.standard, self.params.standard_version
            )
        dataset_variable_details = variable_details.get(self.params.domain, {})
        variable_metadata = {
            key: dataset_variable_details[key].get(self.params.target)
            for key in dataset_variable_details.keys()
        }
        return variable_metadata

    def VariableValueCount(self):
        # get metadata
        variable_value_count = asyncio.run(self._get_all_study_variable_value_counts())
        return variable_value_count

    def VariableCount(self):
        # get metadata
        variable_count = asyncio.run(self._get_all_study_variable_counts())
        return variable_count

    def VariableIsNull(self):
        # Always get the content dataframe. Similar to variable_exists check
        dataframe = self.data_service.get_dataset(self.params.dataset_path)
        if self.params.target.startswith("define_variable"):
            # Handle checks against define metadata
            target_column = self.evaluation_dataset[self.params.target]
            result = [
                helpers._is_target_variable_null(dataframe, value)
                for value in target_column
            ]
            return pd.Series(result)
        else:
            target_variable = self.params.target.replace("--", self.params.domain, 1)
            return helpers._is_target_variable_null(dataframe, target_variable)

    def DomainIsCustom(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        standard_data: dict = self.library_metadata.standard_metadata
        if not standard_data:
            cdisc_library_service = helpers.CDISCLibraryService(
                config, self.cache_service
            )
            standard_data = cdisc_library_service.get_standard_details(
                self.params.standard.lower(), self.params.standard_version
            )
            self.library_metadata.standard_metadata = standard_data
        return self.params.domain not in standard_data.get("domains", {})

    def DomainLabel(self):
        """
        Return the domain label for the currently executing domain
        """
        standard_data = self._retrieve_standards_metadata()
        domain_details = None
        for c in standard_data.get("classes", []):
            domain_details = search_in_list_of_dicts(
                c.get("datasets", []), lambda item: item["name"] == self.params.domain
            )
            if domain_details:
                return domain_details.get("label", "")
        return ""

    def RequiredVariables(self):
        """
        Fetches required variables for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """

        # get variables metadata from the standard model
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()
        return list(
            set(
                [
                    var["name"].replace("--", self.params.domain)
                    for var in variables_metadata
                    if self.get_allowed_variable_permissibility(var) == REQUIRED
                ]
            )
        )

    def ExpectedVariables(self):
        """
        Fetches required variables for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """

        # get variables metadata from the standard model
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()

        return list(
            set(
                [
                    var["name"].replace("--", self.params.domain)
                    for var in variables_metadata
                    if self.get_allowed_variable_permissibility(var) == EXPECTED
                ]
            )
        )

    def StudyDomains(self):
        return list({dataset.get("domain", "") for dataset in self.params.datasets})

    def ValidCodelistDates(self):
        # get metadata
        ct_packages = self.library_metadata.published_ct_packages
        if not ct_packages:
            return []
        return [
            helpers._parse_date_from_ct_package(package)
            for package in ct_packages
            if helpers._is_applicable_ct_package(self.params, package)
        ]

    def LabelReferencedVariableMetadata(self):
        """
        Generates a dataframe where each record in the dataframe
        is the variable metadata corresponding with the variable label
        found in the column provided in self.params.target.
        """
        variables_metadata = self._get_variables_metadata_from_standard()
        df = pd.DataFrame(variables_metadata).add_prefix(f"{self.params.operation_id}_")
        if self.library == "dask":
            self.evaluation_dataset = self.evaluation_dataset.compute()
        return (
            df.merge(
                self.evaluation_dataset,
                left_on=f"{self.params.operation_id}_label",
                right_on=self.params.target,
                how="right",
            )
            .filter(like=self.params.operation_id, axis=1)
            .fillna("")
        )

    def NameReferencedVariableMetadata(self):
        """
        Generates a dataframe where each record in the dataframe
        is the variable metadata corresponding with the variable name
        found in the column provided in self.params.target.
        """
        variables_metadata = self._get_variables_metadata_from_standard()
        df = pd.DataFrame(variables_metadata).add_prefix(f"{self.params.operation_id}_")
        if self.library == "dask":
            self.evaluation_dataset = self.evaluation_dataset.compute()

        return (
            df.merge(
                self.evaluation_dataset,
                left_on=f"{self.params.operation_id}_name",
                right_on=self.params.target,
                how="right",
            )
            .filter(like=self.params.operation_id, axis=1)
            .fillna("")
        )

    def DefineVariableMetadata(self):
        """
        If a target variable is specified, returns the specified metadata in the define
          for the specified target variable.
        For example:
            Input:
                - operation: define_variable_metadata
                  attribute_name: define_variable_label
                  name: LBTESTCD
            Output:
                "Laboratory Test Code"
        If no target variable specified, returns a dictionary containing the specified
          metadata in the define for all variables.
        For example:
            Input:
                - operation: define_variable_metadata
                  attribute_name: define_variable_label
            Output:
                {
                    "STUDYID": "Study Identifier",
                    "USUBJID": "Unique Subject Identifier",
                    "LBTESTCD": "Laboratory Test Code"
                    ...
                }
        """
        define_contents = self.data_service.get_define_xml_contents(
            dataset_name=os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
        )
        define_reader = DefineXMLReaderFactory.from_file_contents(define_contents)
        variables_metadata = define_reader.extract_variables_metadata(
            self.params.domain
        )
        variable_metadata = {
            metadata["define_variable_name"]: metadata.get(
                self.params.attribute_name, ""
            )
            for metadata in variables_metadata
        }
        return (
            variable_metadata.get(
                self.params.target.replace("--", self.params.domain, 1), ""
            )
            if self.params.target
            else variable_metadata
        )

    def ValidExternalDictionaryValue(self):
        if self.params.external_dictionary_type not in DictionaryTypes.values():
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the engine"
            )

        validator_map = {DictionaryTypes.MEDDRA.value: MedDRAValidator}

        validator_type = validator_map.get(self.params.external_dictionary_type)
        if not validator_type:
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the "
                + "valid_external_dictionary_value operation"
            )

        validator = validator_type(
            cache_service=self.cache_service,
            meddra_path=self.params.meddra_path,
            whodrug_path=self.params.whodrug_path,
        )
        if self.library == "dask":
            return self.params.dataframe.compute().apply(
                lambda row: validator.is_valid_term(
                    term=row[self.params.target],
                    term_type=self.params.dictionary_term_type,
                    variable=self.params.original_target,
                ),
                axis=1,
            )
        else:
            return self.params.dataframe.apply(
                lambda row: validator.is_valid_term(
                    term=row[self.params.target],
                    term_type=self.params.dictionary_term_type,
                    variable=self.params.original_target,
                ),
                axis=1,
            )

    def _handle_operation_result(self, result) -> pd.DataFrame:
        if self.params.grouping:
            return self._handle_grouped_result(result)
        elif isinstance(result, dict):
            print("in dict")
            return self._handle_dictionary_result(result)
        elif isinstance(result, pd.Series):
            print("in pd series")
            self.evaluation_dataset[self.params.operation_id] = result
            return self.evaluation_dataset
        elif isinstance(result, dd.Series):
            print("in dd series")
            self.evaluation_dataset[self.params.operation_id] = result
            return self.evaluation_dataset.compute()
        elif isinstance(result, pd.DataFrame):
            print("in pd Dataframe")
            # Assume that the operation id has been applied and
            # result matches the length of the evaluation dataset.
            return pd.concat([self.evaluation_dataset, result], axis=1)
        elif isinstance(result, dd.DataFrame):
            print("in dd Dataframe")
            return dd.concat([self.evaluation_dataset, result], axis=1)
        else:
            print("in bare else")
            # print("result :", result)
            # print(type(result))
            # Handle single results
            self.evaluation_dataset[self.params.operation_id] = pd.Series(
                [result] * len(self.evaluation_dataset)
            )
            if self.library == "dask":
                print("result :", result)
                print(type(result))
                self.evaluation_dataset = self.evaluation_dataset.compute()
            return self.evaluation_dataset

    def _handle_grouped_result(self, result):
        # Handle grouped results
        result = result.rename(columns={self.params.target: self.params.operation_id})
        target_columns = self.params.grouping + [self.params.operation_id]
        return self.evaluation_dataset.merge(
            result[target_columns], on=self.params.grouping, how="left"
        )

    def _handle_dictionary_result(self, result):
        if self.library == "dask":
            self.evaluation_dataset = self.evaluation_dataset.compute()
            self.evaluation_dataset[self.params.operation_id] = [result] * len(
                self.evaluation_dataset
            )
        else:
            self.evaluation_dataset[self.params.operation_id] = [result] * len(
                self.evaluation_dataset
            )
        return self.evaluation_dataset

    def _get_variables_metadata_from_standard(self) -> List[dict]:
        # TODO: Update to handle other standard types: adam, cdash, etc.

        return sdtm_utilities.get_variables_metadata_from_standard(
            self.params.standard,
            self.params.standard_version,
            self.params.domain,
            config,
            self.cache_service,
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

    def MedDRACodeTermPairsValidator(self):
        # get metadata
        if not self.params.meddra_path:
            raise ValueError("Can't execute the operation, no meddra path provided")
        term_type, columns = self._get_columns_by_meddra_variable_name()
        cache_key: str = get_meddra_code_term_pairs_cache_key(self.params.meddra_path)
        valid_code_term_pairs = self.cache_service.get(cache_key)
        if not valid_code_term_pairs:
            terms: dict = self.cache_service.get(self.params.meddra_path)
            valid_code_term_pairs = MedDRATerm.get_code_term_pairs(terms)
            self.cache_service.add(cache_key, valid_code_term_pairs)
        column = str(uuid4()) + "_pairs"
        self.params.dataframe[column] = list(
            zip(
                self.params.dataframe[columns[0]],
                self.params.dataframe[columns[1]],
            )
        )
        result = self.params.dataframe[column].isin(valid_code_term_pairs[term_type])
        return result

    def VariableExists(self):
        # get metadata
        dataframe = self.data_service.get_dataset(self.params.dataset_path)
        return self.params.target in dataframe

    def VariableNames(self):
        """
        Return the set of variable names for the given standard
        """
        variable_details = self.library_metadata.variables_metadata
        if not variable_details:
            cdisc_library_service = helpers.CDISCLibraryService(
                config, self.cache_service
            )
            variable_details = cdisc_library_service.get_variables_details(
                self.params.standard, self.params.standard_version
            )
        all_variables = [
            list(variable_details[dataset].values()) for dataset in variable_details
        ]

        return set(
            [variable["name"] for variables in all_variables for variable in variables]
        )

    def PermissibleVariables(self):
        """
        Fetches required variables for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """

        # get variables metadata from the standard model
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()

        return list(
            set(
                [
                    var["name"].replace("--", self.params.domain)
                    for var in variables_metadata
                    if self.get_allowed_variable_permissibility(var) == PERMISSIBLE
                ]
            )
        )

    def _get_columns_by_meddra_variable_name(
        self,
    ) -> Optional[Tuple[str, Tuple[str, str]]]:
        """
        Extracts target name from params and
        returns associated term type and dataset columns.
        """
        soccd_column = f"{self.params.domain}{MedDRAVariables.SOCCD.value}"
        soc_column = f"{self.params.domain}{MedDRAVariables.SOC.value}"
        hlgtcd_column = f"{self.params.domain}{MedDRAVariables.HLGTCD.value}"
        hlgt_column = f"{self.params.domain}{MedDRAVariables.HLGT.value}"
        hltcd_column = f"{self.params.domain}{MedDRAVariables.HLTCD.value}"
        hlt_column = f"{self.params.domain}{MedDRAVariables.HLT.value}"
        ptcd_column = f"{self.params.domain}{MedDRAVariables.PTCD.value}"
        decod_column = f"{self.params.domain}{MedDRAVariables.DECOD.value}"
        llt_column = f"{self.params.domain}{MedDRAVariables.LLT.value}"
        lltcd_column = f"{self.params.domain}{MedDRAVariables.LLTCD.value}"

        variable_pair_map = {
            soc_column: (
                TermTypes.SOC.value,
                (
                    soccd_column,
                    soc_column,
                ),
            ),
            soccd_column: (
                TermTypes.SOC.value,
                (
                    soccd_column,
                    soc_column,
                ),
            ),
            hlgt_column: (
                TermTypes.HLGT.value,
                (
                    hlgtcd_column,
                    hlgt_column,
                ),
            ),
            hlgtcd_column: (
                TermTypes.HLGT.value,
                (
                    hlgtcd_column,
                    hlgt_column,
                ),
            ),
            hlt_column: (
                TermTypes.HLT.value,
                (
                    hltcd_column,
                    hlt_column,
                ),
            ),
            hltcd_column: (
                TermTypes.HLT.value,
                (
                    hltcd_column,
                    hlt_column,
                ),
            ),
            decod_column: (
                TermTypes.PT.value,
                (
                    ptcd_column,
                    decod_column,
                ),
            ),
            ptcd_column: (
                TermTypes.PT.value,
                (
                    ptcd_column,
                    decod_column,
                ),
            ),
            llt_column: (
                TermTypes.LLT.value,
                (
                    lltcd_column,
                    llt_column,
                ),
            ),
            lltcd_column: (
                TermTypes.LLT.value,
                (
                    lltcd_column,
                    llt_column,
                ),
            ),
        }
        return variable_pair_map.get(self.params.target)

    def _retrieve_standards_metadata(self):
        return sdtm_utilities.retrieve_standard_metadata(
            standard=self.params.standard,
            standard_version=self.params.standard_version,
            cache=self.cache_service,
            config=config,
            library_metadata=self.library_metadata,
        )

    def _get_variable_names_list(self, domain, dataframe):
        # get variables metadata from the standard model
        variables_metadata: List[
            dict
        ] = self._get_variables_metadata_from_standard_model(domain, dataframe)
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
            standard=self.params.standard,
            standard_version=self.params.standard_version,
            domain=domain,
            dataframe=dataframe,
            datasets=self.params.datasets,
            dataset_path=self.params.dataset_path,
            cache=self.cache_service,
            data_service=self.data_service,
            library_metadata=self.library_metadata,
        )

    async def _get_all_study_variable_value_counts(self) -> dict:
        """
        Returns a mapping of variable values to the number
        of times that value appears in the study.
        """
        datasets_with_unique_domains = list(
            {dataset["domain"]: dataset for dataset in self.params.datasets}.values()
        )
        coroutines = [
            self._get_dataset_variable_value_count(dataset)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[Counter] = await asyncio.gather(*coroutines)
        return dict(sum(dataset_variable_value_counts, Counter()))

    async def _get_dataset_variable_value_count(self, dataset: dict) -> Counter:
        domain = dataset.get("domain")
        if is_split_dataset(self.params.datasets, domain):
            files = [
                os.path.join(self.params.directory_path, dataset.get("filename"))
                for dataset in get_corresponding_datasets(self.params.datasets, domain)
            ]
            data: pd.DataFrame = self.data_service.join_split_datasets(
                self.data_service.get_dataset, files
            )
        else:
            data: pd.DataFrame = self.data_service.get_dataset(
                os.path.join(self.params.directory_path, dataset.get("filename"))
            )
        target_variable = self.params.original_target.replace("--", domain, 1)
        if target_variable in data:
            return Counter(data[target_variable].unique())
        else:
            return Counter()

    async def _get_all_study_variable_counts(self) -> dict:
        """
        Returns a mapping of target values to the number
        of times that value appears as a variable in the study.
        """
        datasets_with_unique_domains = list(
            {dataset["domain"]: dataset for dataset in self.params.datasets}.values()
        )
        coroutines = [
            self._get_dataset_variable_count(dataset)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[int] = await asyncio.gather(*coroutines)
        return sum(dataset_variable_value_counts)

    async def _get_dataset_variable_count(self, dataset: dict) -> Counter:
        domain = dataset.get("domain", "")
        data: pd.DataFrame = self.data_service.get_dataset(
            os.path.join(self.params.directory_path, dataset.get("filename"))
        )
        target_variable = self.params.original_target.replace("--", domain, 1)
        return 1 if target_variable in data else 0

    def _get_model_filtered_variables(self):
        key = self.params.key_name
        val = self.params.key_value

        # get variables metadata from the standard
        var_standard: List[dict] = self._get_variables_metadata_from_standard()
        # get subset of the selected variables
        var_standard_selected = [var for var in var_standard if var.get(key) == val]
        # replace variable names with domain abbreviation in them
        variable_names_list = self._replace_variable_wildcards(
            var_standard_selected, self.params.domain
        )
        # sort the list
        r1_var_standard = list(OrderedDict.fromkeys(variable_names_list))
        # get variables metadata from the model
        r2_var_model: List[dict] = self._get_variable_names_list(
            self.params.domain, self.params.dataframe
        )
        # get the common variables from standard model
        common_var_list = [
            element for element in r2_var_model if element in r1_var_standard
        ]
        return common_var_list

    def _get_parent_variable_names_list(
        self, domain_to_datasets: dict, rdomain: str, data_service
    ):
        parent_datasets = domain_to_datasets.get(rdomain, [])
        if len(parent_datasets) < 1:
            return []
        parent_dataframe = data_service.get_dataset(parent_datasets[0]["filename"])
        return self._get_variable_names_list(rdomain, parent_dataframe)

    @staticmethod
    def _replace_variable_wildcards(variables_metadata, domain):
        return [var["name"].replace("--", domain) for var in variables_metadata]
