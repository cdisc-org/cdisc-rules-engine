from typing import List, Optional, Tuple

from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
    GENERAL_OBSERVATIONS_CLASS,
    FINDINGS_ABOUT,
    FINDINGS,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    get_model_details_cache_key,
    get_standard_details_cache_key,
    search_in_list_of_dicts,
)


class LibraryModelColumnOrder(BaseOperation):
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

        # get variables metadata from the standard model
        variables_metadata: List[
            dict
        ] = self._get_variables_metadata_from_standard_model()

        # create a list of variable names in accordance to the "ordinal" key
        variable_names_list = [
            var["name"].replace("--", self.params.domain) for var in variables_metadata
        ]
        return variable_names_list

    def _get_variables_metadata_from_standard_model(self) -> List[dict]:
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
        cache_key: str = get_standard_details_cache_key(
            self.params.standard, self.params.standard_version
        )

        standard_details: dict = self.cache.get(cache_key) or {}
        model = standard_details.get("_links", {}).get("model")
        model_type, model_version = self._get_model_type_and_version(model)
        model_cache_key = get_model_details_cache_key(model_type, model_version)
        model_details = self.cache.get(model_cache_key) or {}
        domain_details = self._get_model_domain_metadata(
            model_details, self.params.domain
        )
        class_title = domain_details["_links"]["parentClass"]["title"]
        class_details = self._get_model_class_metadata(model_details, class_title)
        variables_metadata = domain_details.get("datasetVariables", [])
        variables_metadata.sort(key=lambda item: item["ordinal"])
        class_name = class_details.get("name")
        if class_name in DETECTABLE_CLASSES or class_name == FINDINGS_ABOUT:
            # if the class is one of Interventions, Findings, Events, or Findings About
            # -> get class variables instead of datasetVariables add
            # General Observation class variables to variables metadata
            variables_metadata = class_details.get("classVariables", [])
            variables_metadata.sort(key=lambda item: item["ordinal"])

            if class_name == FINDINGS_ABOUT:
                # Add FINDINGS class variables. Findings About class variables should
                # Appear in the list after the --TEST variable
                findings_class_metadata: dict = self._get_model_class_metadata(
                    model_details, FINDINGS
                )
                findings_class_variables = findings_class_metadata["classVariables"]
                findings_class_variables.sort(key=lambda item: item["ordinal"])
                test_index = len(findings_class_variables) - 1
                for i, v in enumerate(findings_class_variables):
                    if v["name"].lower().endswith("test"):
                        test_index = i
                variables_metadata = (
                    findings_class_variables[: test_index + 1]
                    + variables_metadata
                    + findings_class_variables[test_index + 1 :]
                )

            gen_obs_class_metadata: dict = self._get_model_class_metadata(
                model_details, GENERAL_OBSERVATIONS_CLASS
            )
            identifiers_metadata, timing_metadata = self._sort_class_variables_by_role(
                gen_obs_class_metadata["classVariables"]
            )
            # Identifiers are added to the beginning and Timing to the end
            if identifiers_metadata:
                identifiers_metadata.sort(key=lambda item: item["ordinal"])
                variables_metadata = identifiers_metadata + variables_metadata
            if timing_metadata:
                timing_metadata.sort(key=lambda item: item["ordinal"])
                variables_metadata = variables_metadata + timing_metadata

        return variables_metadata

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

    def _get_model_class_metadata(
        self,
        model_details: dict,
        class_title: str,
    ) -> dict:
        """
        Extracts metadata of a certain class
        from given standard model details.
        """
        class_metadata: Optional[dict] = search_in_list_of_dicts(
            model_details.get("classes", []),
            lambda item: item["label"] == class_title,
        )
        if not class_metadata:
            raise ValueError(
                f"Model class metadata is not found in CDISC Library. "
                f"standard={self.params.standard}, "
                f"version={self.params.standard_version}, "
                f"class={class_title}"
            )
        return class_metadata

    def _get_model_domain_metadata(self, model_details, domain_name) -> Tuple:
        # Get domain metadata from model
        domain_details: Optional[dict] = search_in_list_of_dicts(
            model_details.get("datasets", []), lambda item: item["name"] == domain_name
        )

        if not domain_details:
            raise ValueError(
                f"Model domain metadata is not found in CDISC Library. "
                f"standard={self.params.standard}, "
                f"version={self.params.standard_version}, "
                f"domain={domain_name}"
            )

        return domain_details

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
