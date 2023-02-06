from typing import List, Optional, Tuple

from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
    GENERAL_OBSERVATIONS_CLASS,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    get_model_details_cache_key,
    get_standard_details_cache_key,
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
        class_details, domain_details = self._get_class_and_domain_metadata(
            standard_details
        )
        model_type, model_version = self._get_model_type_and_version(model)
        model_cache_key = get_model_details_cache_key(model_type, model_version)
        model_details = self.cache.get(model_cache_key) or {}

        # model class details includes all variables allowed in the domain
        model_class_details: dict = self._get_class_metadata(
            model_details, class_details.get("name")
        )
        variables_metadata: List[dict] = model_class_details.get("classVariables", [])
        variables_metadata.sort(key=lambda item: item["ordinal"])

        if class_details.get("name") in DETECTABLE_CLASSES:
            # if the class is one of Interventions, Findings, or Events
            # and the standard is SDTMIG
            # -> add General Observation class variables to variables metadata
            gen_obs_class_metadata: dict = self._get_class_metadata(
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
