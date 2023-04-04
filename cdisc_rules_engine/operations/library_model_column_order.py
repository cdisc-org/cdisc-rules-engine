from typing import List, Optional, Tuple

from cdisc_rules_engine.constants.classes import (
    DETECTABLE_CLASSES,
)
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    get_model_details_cache_key,
    get_standard_details_cache_key,
    search_in_list_of_dicts,
    convert_library_class_name_to_ct_class,
)
from collections import OrderedDict


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
        return self._get_variable_names_list(self.params.domain, self.params.dataframe)

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
        # get model details from cache
        cache_key: str = get_standard_details_cache_key(
            self.params.standard, self.params.standard_version
        )

        standard_details: dict = self.cache.get(cache_key) or {}
        model = standard_details.get("_links", {}).get("model")
        model_type, model_version = self._get_model_type_and_version(model)
        model_cache_key = get_model_details_cache_key(model_type, model_version)
        model_details = self.cache.get(model_cache_key) or {}
        domain_details = self._get_model_domain_metadata(model_details, domain)
        variables_metadata = []

        if domain_details:
            # Domain found in the model
            class_name = convert_library_class_name_to_ct_class(
                domain_details["_links"]["parentClass"]["title"]
            )
            class_details = self._get_class_metadata(model_details, class_name)
            variables_metadata = domain_details.get("datasetVariables", [])
            variables_metadata.sort(key=lambda item: item["ordinal"])
        else:
            # Domain not found in the model. Detect class name from data
            class_name = self.data_service.get_dataset_class(
                dataframe, self.params.dataset_path, self.params.datasets
            )
            class_name = convert_library_class_name_to_ct_class(class_name)
            class_details = self._get_class_metadata(model_details, class_name)

        if class_name in DETECTABLE_CLASSES:
            (
                identifiers_metadata,
                variables_metadata,
                timing_metadata,
            ) = self.get_allowed_class_variables(model_details, class_details)
            # Identifiers are added to the beginning and Timing to the end
            if identifiers_metadata:
                variables_metadata = identifiers_metadata + variables_metadata
            if timing_metadata:
                variables_metadata = variables_metadata + timing_metadata

        return variables_metadata

    def _get_model_domain_metadata(self, model_details, domain_name) -> Tuple:
        # Get domain metadata from model
        domain_details: Optional[dict] = search_in_list_of_dicts(
            model_details.get("datasets", []), lambda item: item["name"] == domain_name
        )

        return domain_details

    @staticmethod
    def _replace_variable_wildcards(variables_metadata, domain):
        return [var["name"].replace("--", domain) for var in variables_metadata]
