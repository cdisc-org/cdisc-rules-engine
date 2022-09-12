from typing import List, Optional, Tuple

import pandas as pd

from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
    GENERAL_OBSERVATIONS_CLASS,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    get_model_details_cache_key,
    search_in_list_of_dicts,
)


class LibraryColumnOrder(BaseOperation):
    def _execute_operation(self):
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
        # get dataset class
        dataset_class: str = self.data_service.get_dataset_class(
            self.params.dataframe, self.params.dataset_path, self.params.datasets
        )

        # get variables metadata from the standard model
        variables_metadata: List[
            dict
        ] = self._get_variables_metadata_from_standard_model(dataset_class)

        # create a list of variable names in accordance to the "ordinal" key
        variables_metadata.sort(key=lambda item: item["ordinal"])
        variable_names_list = [
            var["name"].replace("--", self.params.domain) for var in variables_metadata
        ]
        return pd.Series([variable_names_list] * len(self.params.dataframe))

    def _get_variables_metadata_from_standard_model(
        self, dataset_class: str
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
        # get model details from cache
        cache_key: str = get_model_details_cache_key(
            self.params.standard, self.params.standard_version
        )
        model_details: dict = self.cache.get(cache_key) or {}

        # get variables metadata
        domain_details: Optional[dict] = search_in_list_of_dicts(
            model_details.get("datasets", []),
            lambda item: item["name"] == self.params.domain,
        )
        if domain_details:
            # if model describes the domain -> get metadata from the model domain
            variables_metadata: List[dict] = domain_details["datasetVariables"]
        else:
            # else -> get variables metadata from the model class
            class_metadata: dict = self._get_class_metadata(
                model_details, dataset_class
            )
            variables_metadata: List[dict] = class_metadata["classVariables"]

        if dataset_class in DETECTABLE_CLASSES:
            # if the class is one of Interventions, Findings, or Events
            # -> add General Observation class variables to variables metadata
            gen_obs_class_metadata: dict = self._get_class_metadata(
                model_details, GENERAL_OBSERVATIONS_CLASS
            )
            identifiers_metadata, timing_metadata = self._sort_class_variables_by_role(
                gen_obs_class_metadata["classVariables"]
            )
            # Identifiers are added to the beginning and Timing to the end
            variables_metadata: List[dict] = (
                identifiers_metadata + variables_metadata + timing_metadata
            )

        return variables_metadata

    def _get_class_metadata(
        self,
        model_details: dict,
        dataset_class: str,
    ) -> dict:
        """
        Extracts metadata of a certain class
        from given standard model details.
        """
        class_metadata: Optional[dict] = search_in_list_of_dicts(
            model_details.get("classes", []),
            lambda item: item["name"] == dataset_class,
        )
        if not class_metadata:
            raise ValueError(
                f"Variables metadata is not found in CDISC Library. "
                f"standard={self.params.standard}, "
                f"version={self.params.standard_version}, "
                f"class={dataset_class}"
            )
        return class_metadata

    def _sort_class_variables_by_role(
        self, class_variables: List[dict]
    ) -> Tuple[List[dict], List[dict]]:
        """
        Sorts given class variables by role into 2 lists:
        Identifiers and Timing
        """
        identifier_vars: List[dict] = []
        timing_vars: List[dict] = []
        for variable in class_variables:
            role: str = variable.get("role")
            if role == VariableRoles.IDENTIFIER.value:
                identifier_vars.append(variable)
            elif role == VariableRoles.TIMING.value:
                timing_vars.append(variable)
        return identifier_vars, timing_vars
