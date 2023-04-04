import pandas as pd
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.constants.permissibility import (
    REQUIRED,
    PERMISSIBLE,
    REQUIRED_MODEL_VARIABLES,
    SEQ_VARIABLE,
    PERMISSIBILITY_KEY,
)
from abc import abstractmethod
from typing import List, Optional, Tuple
from cdisc_rules_engine.enums.variable_roles import VariableRoles

from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
)

from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS_ABOUT,
    FINDINGS,
    FINDINGS_TEST_VARIABLE,
)

from cdisc_rules_engine.utilities.utils import (
    get_model_details_cache_key,
    get_standard_details_cache_key,
    search_in_list_of_dicts,
    convert_library_class_name_to_ct_class,
)


class BaseOperation:
    def __init__(
        self,
        params: OperationParams,
        original_dataset: pd.DataFrame,
        cache_service: CacheServiceInterface,
        data_service: DataServiceInterface,
    ):
        self.params = params
        self.cache = cache_service
        self.data_service = data_service
        self.evaluation_dataset = original_dataset

    @abstractmethod
    def _execute_operation(self):
        """Perform operation calculations."""
        pass

    def execute(self) -> pd.DataFrame:
        result = self._execute_operation()
        return self._handle_operation_result(result)

    def _handle_operation_result(self, result) -> pd.DataFrame:
        if self.params.grouping:
            return self._handle_grouped_result(result)
        elif isinstance(result, dict):
            return self._handle_dictionary_result(result)
        elif isinstance(result, pd.Series):
            self.evaluation_dataset[self.params.operation_id] = result
            return self.evaluation_dataset
        else:
            # Handle single results
            self.evaluation_dataset[self.params.operation_id] = pd.Series(
                [result] * len(self.evaluation_dataset)
            )
            return self.evaluation_dataset

    def _handle_grouped_result(self, result):
        # Handle grouped results
        result = result.rename(columns={self.params.target: self.params.operation_id})
        target_columns = self.params.grouping + [self.params.operation_id]
        return self.evaluation_dataset.merge(
            result[target_columns], on=self.params.grouping, how="left"
        )

    def _handle_dictionary_result(self, result):
        self.evaluation_dataset[self.params.operation_id] = [result] * len(
            self.evaluation_dataset
        )
        return self.evaluation_dataset

    def _get_variables_metadata_from_standard(self) -> List[dict]:
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
        if not self.params.standard or not self.params.standard_version:
            raise Exception("Please provide standard and version")
        cache_key: str = get_standard_details_cache_key(
            self.params.standard, self.params.standard_version
        )
        standard_details: dict = self.cache.get(cache_key) or {}
        model = standard_details.get("_links", {}).get("model")
        class_details, domain_details = self._get_class_and_domain_metadata(
            standard_details
        )
        model_type, model_version = self._get_model_type_and_version(model)
        model_cache_key = get_model_details_cache_key(model_type, model_version)
        model_details = self.cache.get(model_cache_key) or {}

        variables_metadata = domain_details.get("datasetVariables", [])
        variables_metadata.sort(key=lambda item: item["ordinal"])
        class_name = convert_library_class_name_to_ct_class(class_details.get("name"))
        if class_name in DETECTABLE_CLASSES:
            (
                identifiers_metadata,
                class_variables_metadata,
                timing_metadata,
            ) = self.get_allowed_class_variables(model_details, class_details)
            if identifiers_metadata:
                variables_metadata = identifiers_metadata + variables_metadata
            if timing_metadata:
                variables_metadata = variables_metadata + timing_metadata

        return variables_metadata

    def get_allowed_class_variables(self, model_details: dict, class_details: dict):
        # General Observation class variables to variables metadata
        class_name = convert_library_class_name_to_ct_class(class_details.get("name"))
        variables_metadata = class_details.get("classVariables", [])
        variables_metadata.sort(key=lambda item: item["ordinal"])

        if class_name == FINDINGS_ABOUT:
            # Add FINDINGS class variables. Findings About class variables should
            # Appear in the list after the --TEST variable
            findings_class_metadata: dict = self._get_class_metadata(
                model_details, FINDINGS
            )
            findings_class_variables = findings_class_metadata["classVariables"]
            findings_class_variables.sort(key=lambda item: item["ordinal"])
            test_index = len(findings_class_variables) - 1
            for i, v in enumerate(findings_class_variables):
                if v["name"] == FINDINGS_TEST_VARIABLE:
                    test_index = i
                    variables_metadata = (
                        findings_class_variables[: test_index + 1]
                        + variables_metadata
                        + findings_class_variables[test_index + 1 :]
                    )
                    break

        gen_obs_class_metadata: dict = self._get_class_metadata(
            model_details, GENERAL_OBSERVATIONS_CLASS
        )
        identifiers_metadata, timing_metadata = self._group_class_variables_by_role(
            gen_obs_class_metadata["classVariables"]
        )
        # Identifiers are added to the beginning and Timing to the end
        identifiers_metadata.sort(key=lambda item: item["ordinal"])
        timing_metadata.sort(key=lambda item: item["ordinal"])
        return identifiers_metadata, variables_metadata, timing_metadata

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

    def _get_model_type_and_version(self, model_link) -> Tuple:
        link = model_link.get("href")
        if "sdtm" in link:
            model_type = "sdtm"
            model_version = link.split("/")[-1]
        else:
            # TODO expand to support CDASH and ADAM
            model_type = ""
            model_version = ""
        return model_type, model_version

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
            lambda item: convert_library_class_name_to_ct_class(item["name"])
            == dataset_class,
        )
        if not class_metadata:
            raise ValueError(
                f"Class metadata is not found in CDISC Library. "
                f"standard={self.params.standard}, "
                f"version={self.params.standard_version}, "
                f"class={dataset_class}"
            )
        return class_metadata

    def _get_class_and_domain_metadata(self, standard_details) -> Tuple:
        # Get domain and class details for domain. This logic is specific
        # to SDTM based standards. Needs to be expanded for other models
        for c in standard_details.get("classes"):
            domain_details = search_in_list_of_dicts(
                c.get("datasets", []), lambda item: item["name"] == self.params.domain
            )
            if domain_details:
                return c, domain_details
        return {}, {}

    def _group_class_variables_by_role(
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
