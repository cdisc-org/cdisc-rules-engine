from functools import partial
from typing import Callable, List, Set, Optional

from cdisc_library_client import CDISCLibraryClient

from cdisc_rules_engine.enums.library_endpoints import LibraryEndpoints
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.utilities.utils import (
    get_metadata_cache_key,
    get_rules_cache_key,
)


class CDISCLibraryService:
    def __init__(self, api_key, cache_service_obj):
        self._api_key = api_key
        self._client = CDISCLibraryClient(
            self._api_key, base_api_url="https://api.library.cdisc.org/api"
        )
        self.cache = cache_service_obj

    def cache_library_json(self, uri: str) -> dict:
        """
        Makes a library request to the provided URI,
        caches the response and returns the response
        """
        data = self._client.get_api_json(uri)
        cache_key = get_metadata_cache_key(uri)
        self.cache.add(cache_key, data)
        return data

    def get_all_rule_catalogs(self) -> List[dict]:
        catalogs = self._client.get_rule_catalogs()
        return catalogs.values()

    def get_rules_by_catalog(self, standard, version):
        rules = self._client.get_rules_catalog(standard, version)
        return {
            "key_prefix": get_rules_cache_key(standard, version),
            "rules": [Rule.from_cdisc_metadata(rule) for rule in rules.values()],
        }

    def get_all_ct_packages(self) -> List[dict]:
        """
        Queries the CDISC Library for all ct packages.
        Returns list of packages from the library in the form:
        [
            {
                "href": "/mdr/ct/packages/adamct-2014-09-26",
                "title": "ADaM Controlled Terminology Package 19 Effective 2014-09-26",
                "type": "Terminology"
            },
            {
                "href": "/mdr/ct/packages/adamct-2015-12-18",
                "title": "ADaM Controlled Terminology Package 24 Effective 2015-12-18",
                "type": "Terminology"
            }...
        ]
        """
        data = self._client.get_api_json(LibraryEndpoints.PACKAGES.value)
        return data["_links"].get("packages", [])

    def get_all_products(self) -> dict:
        """
        Queries the CDISC Library for all products available.
        Returns a dictionary of products in the form:
        {
            "data-analysis": [adam products],
            "data-collection": [cdash products],
            "data-tabulation": [sdtm products]
        }
        """
        data = self._client.get_api_json(LibraryEndpoints.PRODUCTS.value)
        return data["_links"]

    def get_all_tabulation_ig_standards(self) -> List[dict]:
        """
        Queries the CDISC library for all data tabulation products.
        Returns a list of link objects to sdtmig and sendig products:
        [
            {
                "href": "/mdr/sdtmig/3-1-2",
                "title": "Study Data Tabulation Model ...",
                "type": "Implementation Guide"
            }...
        ]
        """
        standards = []
        data = self._client.get_api_json(LibraryEndpoints.DATA_TABULATION.value)
        standards.extend(data.get("_links", {}).get("sdtmig", []))
        standards.extend(data.get("_links", {}).get("sendig", []))
        return standards

    def get_all_collection_ig_standards(self) -> List[dict]:
        """
        Queries the CDISC library for all data collection products.
        Returns a list of link objects to cdashig products:
        [
            {
                "href": "uri",
                "title": "",
                "type": "Implementation Guide"
            }...
        ]
        """
        standards = []
        data = self._client.get_api_json(LibraryEndpoints.DATA_COLLECTION.value)
        standards.extend(data.get("_links", {}).get("cdashig", []))
        return standards

    def get_all_analysis_ig_standards(self) -> List[dict]:
        """
        Queries the CDISC library for all data analysis products.
        Returns a list of link objects to adam products:
        [
            {
                "href": "uri",
                "title": "title",
                "type": "Implementation Guide"
            }...
        ]
        """
        standards = []
        data = self._client.get_api_json(LibraryEndpoints.DATA_ANALYSIS.value)
        igs = [
            ig
            for ig in data.get("_links", {}).get("adam", [])
            if ig.get("type") == "Implementation Guide"
        ]
        standards.extend(igs)
        return standards

    def get_codelist_terms_map(self, package_version: str) -> dict:
        uri = f"/mdr/ct/packages/{package_version}"
        package = self._client.get_api_json(uri)
        codelist_map = {"package": package_version}
        for codelist in package.get("codelists"):
            terms_map = {
                "extensible": codelist.get("extensible", "").lower() == "true",
            }
            allowed_values = []
            for term in codelist.get("terms", []):
                allowed_values.append(term.get("preferredTerm"))
                allowed_values.append(term.get("submissionValue"))
                allowed_values.extend(term.get("synonyms", []))
            terms_map["allowed_terms"] = allowed_values
            codelist_map[codelist.get("conceptId")] = terms_map
        return codelist_map

    def get_variable_codelists_map(self, standard_type: str, version: str) -> dict:
        """
        Generates a map of variables -> codelists based on standard type and version
        The variables in a standard document have a "codelist"
        attribute which contains links to 1 or more CDISC CT codelists.
        This method parses those uri's for the C-CODE.
        Output:
        {
            "name": sdtmig-3-4-codelists
            "variable_name": ["C123", "C234"...],
            ...
        }
        """
        standard_codelist_function_map = {
            "sdtmig": self._get_tabulation_ig_codelists,
            "sendig": self._get_tabulation_ig_codelists,
            "cdashig": self._get_collection_ig_codelists,
            "adam": self._get_analysis_ig_codelists,
        }
        data: dict = self._get_standard(standard_type, version)
        terms = standard_codelist_function_map.get(
            standard_type, self._get_tabulation_ig_codelists
        )(data)
        return {"name": f"{standard_type}-{version}-codelists", **terms}

    def get_variables_details(self, standard_type: str, version: str) -> dict:
        """
        Returns a map of variable name -> details.
        Input:
            standard_type: "sdtmig"
            version: "3-1-2"
        Output:
            {
                "AEACN": {
                    "_links": {...},
                    "core": "Exp",
                    "description": "...",
                    "label": "Action Taken with Study Treatment",
                },
                "AEACNOTH": {
                    "_links": {...},
                    "core": "Perm",
                    "description": "...",
                    "label": "Other Action Taken",
                },
                ...
            }
        """
        standard_data: dict = self._get_standard(standard_type, version)
        return self._extract_variables_details_from_standard(
            standard_data, standard_type
        )

    def get_standard_details(self, standard_type: str, version: str) -> dict:
        """
        Accepts standard type and version and returns details of a standard like:
        {
            "_links": {
                "model": {
                    "href": "/mdr/sdtm/1-2",
                    "title": "Study Data Tabulation Model Version 1.2",
                    "type": "Foundational Model",
                },
                "self": {
                    "href": "/mdr/sdtmig/3-1-2",
                    "title": "Study Data Tabulation Model ...",
                    "type": "Implementation Guide",
                },
            },
            "description": "CDISC Version 3.1.2 (V3.1.2) Study Data ...",
            "effectiveDate": "2008-11-12",
            "label": "Study Data Tabulation Model Implementation Guide: ...",
            "name": "SDTMIG v3.1.2",
            "registrationStatus": "Final",
            "source": "Prepared by the CDISC Submission Data Standards Team",
            "version": "3.1.2",
            "classes": [
                {...},
                {...},
                ...
            ],
            "domains": {
                "CO",
                "DM",
                "SE",
                "SV",
            }
        }
        """
        standard_data: dict = self._get_standard(standard_type, version)
        domains: Set[str] = self._extract_domain_names_from_tabulation_standard(
            standard_data
        )
        if domains:
            standard_data["domains"] = domains
        return standard_data

    def get_model_details(self, standard_details: dict) -> Optional[dict]:
        """
        Gets details of a standard model:

        {
           "_links":{...},
           "classes":[...],
           "datasets":[...],
           "description":"This is Version 2.0 of the Study ...",
           "effectiveDate":"2021-11-29",
           "label":"Study Data Tabulation Model Version 2.0",
           "name":"SDTM v2.0",
           "registrationStatus":"Final",
           "source":"Prepared by the CDISC Submission Data Standards Team",
           "version":"2-0",
           "standard_type":"sdtm"
        }
        """
        model: Optional[dict] = standard_details.get("_links", {}).get("model")
        if not model:
            return
        standard_href: List[str] = model["href"].split("/")
        standard_type: str = standard_href[-2]
        model_version: str = standard_href[-1]
        model_data: dict = self._get_model(standard_type, model_version)
        model_data["standard_type"] = standard_type
        return model_data

    def _get_standard(self, standard_type: str, version: str) -> dict:
        """
        Requests a standard definition from the library.
        Internal method, not for usage in the client code.
        """
        standard_get_function_map = {
            "sdtmig": partial(self._client.get_sdtmig, version),
            "sendig": partial(self._client.get_sendig, version),
            "adam": partial(self._client.get_adam, version),
            "cdashig": partial(self._client.get_cdashig, version),
        }
        function_to_call: Callable = standard_get_function_map.get(
            standard_type, self._client.get_sdtmig
        )
        return function_to_call()

    def _get_model(self, standard_type: str, model_version: str) -> dict:
        """
        Requests a model definition from the library.
        Internal method, not for usage in the client code.
        """
        standard_get_function_map = {
            "sdtm": partial(self._client.get_sdtm, model_version),
            "adam": partial(self._client.get_adam, model_version),
            "cdash": partial(self._client.get_cdash, model_version),
        }
        function_to_call: Callable = standard_get_function_map.get(
            standard_type, partial(self._client.get_sdtm, model_version)
        )
        return function_to_call()

    def _extract_variables_details_from_standard(
        self, standard_data: dict, standard_name: str
    ) -> dict:
        output = {}
        keys = {
            "sdtmig": {
                "classes_key": "classes",
                "datasets_key": "datasets",
                "variables_key": "datasetVariables",
            },
            "sendig": {
                "classes_key": "classes",
                "datasets_key": "datasets",
                "variables_key": "datasetVariables",
            },
            "cdashig": {
                "classes_key": "classes",
                "datasets_key": "domains",
                "variables_key": "fields",
            },
            "adam": {
                "classes_key": "dataStructures",
                "datasets_key": "analysisVariableSets",
                "variables_key": "analysisVariables",
            },
        }
        standard_keys: dict = keys[standard_name]
        for cls in standard_data.get(standard_keys["classes_key"], []):
            for dataset in cls.get(standard_keys["datasets_key"], []):
                for variable in dataset.get(standard_keys["variables_key"], []):
                    output[variable["name"]] = variable
        return output

    def _get_tabulation_ig_codelists(self, data: dict) -> dict:
        """
        Extract the codelists for each variable in a cdashig.
        This dictionary of terms is then merged with the terms gathered from the
        implementation guides models.
        Returns a dictionary in the form:
        {
            "variable_name": ["C123", "C234"...],
            ...
        }
        """
        terms = {}
        for cls in data.get("classes", []):
            for dataset in cls.get("datasets", []):
                for variable in dataset.get("datasetVariables", []):
                    codelists = variable["_links"].get("codelist", [])
                    ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                    if ccodes:
                        terms[variable.get("name")] = (
                            terms.get(variable.get("name"), []) + ccodes
                        )
        model = data["_links"].get("model")
        model_terms = {}
        if model:
            model_version = model.get("href", "").split("/")[-1]
            model_data = self._client.get_sdtm(model_version)
            model_terms = self._get_tabulation_model_codelists(model_data)
        terms = self._merge_codelist_maps(terms, model_terms)
        return terms

    def _get_collection_ig_codelists(self, data: dict) -> dict:
        """
        Extract the codelists for each variable in a cdashig.
        This dictionary of terms is then merged with the terms gathered from the
        implementation guides models.
        Returns a dictionary in the form:
        {
            "variable_name": ["C123", "C234"...],
            ...
        }
        """
        terms = {}
        for cls in data.get("classes", []):
            for dataset in cls.get("domains", []):
                for variable in dataset.get("fields", []):
                    codelists = variable["_links"].get("codelist", [])
                    ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                    if ccodes:
                        terms[variable.get("name")] = (
                            terms.get(variable.get("name"), []) + ccodes
                        )
        model = data["_links"].get("model")
        model_terms = {}
        if model:
            model_version = model.get("href", "").split("/")[-1]
            model_data = self._client.get_cdash(model_version)
            model_terms = self._get_collection_model_codelists(model_data)
        terms = self._merge_codelist_maps(terms, model_terms)
        return terms

    def _get_analysis_ig_codelists(self, data: dict) -> dict:
        """
        Extract the codelists for each variable in an adamig.
        Returns a dictionary in the form:
        {
            "variable_name": ["C123", "C234"...],
            ...
        }
        """
        terms = {}
        for structure in data.get("dataStructures", []):
            for varset in structure.get("analysisVariableSets", []):
                for variable in varset.get("analysisVariables", []):
                    codelists = variable["_links"].get("codelist", [])
                    ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                    if ccodes:
                        terms[variable.get("name")] = (
                            terms.get(variable.get("name"), []) + ccodes
                        )
        return terms

    def _get_tabulation_model_codelists(self, data: dict) -> dict:
        """
        Extract the codelists for each variable in a model.
        Returns a dictionary in the form:
        {
            "variable_name": ["C123", "C234"...],
            ...
        }
        """
        terms = {}
        for cls in data.get("classes", []):
            for variable in cls.get("classVariables", []):
                codelists = variable["_links"].get("codelist", [])
                ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                if ccodes:
                    terms[variable.get("name")] = (
                        terms.get(variable.get("name"), []) + ccodes
                    )

        for dataset in data.get("datasets", []):
            for variable in dataset.get("datasetVariables", []):
                codelists = variable["_links"].get("codelist", [])
                ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                if ccodes:
                    terms[variable.get("name")] = (
                        terms.get(variable.get("name"), []) + ccodes
                    )
        return terms

    def _get_collection_model_codelists(self, data: dict) -> dict:
        """
        Extract the codelists for each variable in a model.
        Returns a dictionary in the form:
        {
            "variable_name": ["C123", "C234"...],
            ...
        }
        """
        terms = {}
        for cls in data.get("classes", []):
            for variable in cls.get("cdashModelFields", []):
                codelists = variable["_links"].get("codelist", [])
                ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                if ccodes:
                    terms[variable.get("name")] = (
                        terms.get(variable.get("name"), []) + ccodes
                    )

        for dataset in data.get("domains", []):
            for variable in dataset.get("fields", []):
                codelists = variable["_links"].get("codelist", [])
                ccodes = [codelist["href"].split("/")[-1] for codelist in codelists]
                if ccodes:
                    terms[variable.get("name")] = (
                        terms.get(variable.get("name"), []) + ccodes
                    )
        return terms

    def _merge_codelist_maps(self, initial: dict, new_map: dict) -> dict:
        """
        Merges two maps with arrays as values.
        If a key exists in the initial map, the array of values is extended.
        Otherwise it will add the key to the map
        """
        result = initial
        for key in new_map:
            if key in result:
                result[key].extend(new_map[key])
            else:
                result[key] = new_map[key]
        return result

    def _extract_domain_names_from_tabulation_standard(
        self, standard_data: dict
    ) -> Set[str]:
        """
        Accepts tabulation standard data and extracts domain names.
        Input example:
        {
            "registrationStatus": "Final",
            "source": "Prepared by the CDISC Submission Data Standards Team",
            "version": "3.1.2",
            "classes": [
                {
                    "label": "General Observation Class",
                    "name": "General Observations",
                    "ordinal": "1",
                    ...
                },
                {
                    "label": "Special-Purpose Datasets",
                    "name": "Special-Purpose",
                    "ordinal": "2",
                    "datasets": [
                        {
                            "name": "CO",
                            ...
                        },
                        {
                            "name": "DM",
                            ...
                        },
                    ],
                    ...
                },
            ],
            ...
        }

        Output example:
        {"CO", "DM", ...}
        """
        domain_names: Set[str] = set()
        for cls in standard_data.get("classes", []):
            for dataset in cls.get("datasets", []):
                domain_names.add(dataset.get("name"))
        return domain_names
