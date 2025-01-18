import asyncio
import pickle
from functools import partial
from typing import Iterable, List, Optional
import os
from cdisc_library_client.custom_exceptions import (
    ResourceNotFoundException as LibraryResourceNotFoundException,
)

from cdisc_rules_engine.enums.library_endpoints import LibraryEndpoints
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.utilities.utils import (
    get_library_variables_metadata_cache_key,
    get_standard_details_cache_key,
    get_model_details_cache_key,
)
from scripts.script_utils import load_and_parse_local_rule
from cdisc_rules_engine.constants.cache_constants import PUBLISHED_CT_PACKAGES


class CachePopulator:
    def __init__(
        self,
        cache: CacheServiceInterface,
        library_service: CDISCLibraryService = None,
        local_rules_path=None,
        local__rules_id=None,
        remove_local_rules=None,
        cache_path=str,
    ):
        self.cache = cache
        self.library_service = library_service
        self.local_rules_path = local_rules_path
        self.local_rules_id = local__rules_id
        self.remove_local_rules = remove_local_rules
        self.cache_path = cache_path

    async def load_cache_data(self):
        """
        This function populates a cache implementation with
        all data necessary for running rules against local data.
        Including
        * rules (from CDISC Library and optionally local draft rules)
        * library metadata
        * codelist metadata
        """
        if self.remove_local_rules:
            self.remove_specified_rules(self.cache_path)

        elif self.local_rules_path and self.local_rules_id:
            local_rules: List[dict] = await self._get_local_rules(self.local_rules_path)
            key_prefix = f"local/{self.local_rules_id}/"
            for rules in local_rules:
                self.cache.add_batch(local_rules, "custom_id", prefix=key_prefix)
        # send request to get all rules
        elif not self.local_rules_path and not self.remove_local_rules:
            self.library_service.cache_library_json(LibraryEndpoints.PRODUCTS.value)
            self.library_service.cache_library_json(LibraryEndpoints.RULES.value)
            rules_lists: List[dict] = await self._get_rules_from_cdisc_library()
            for rules in rules_lists:
                self.cache.add_batch(
                    rules.get("rules", []), "core_id", prefix=rules.get("key_prefix")
                )
            # save codelists to cache as a map of codelist to terms
            codelist_term_maps = await self._get_codelist_term_maps()
            self.cache.add_batch(codelist_term_maps, "package")

            # Add a list of all published ct packages to the cache
            available_packages = [
                package.get("package")
                for package in codelist_term_maps
                if "package" in package
            ]
            self.cache.add(PUBLISHED_CT_PACKAGES, available_packages)

            # save standard codelists to cache as a map of variable to allowed_values
            standards = self.library_service.get_all_tabulation_ig_standards()
            standards.extend(self.library_service.get_all_collection_ig_standards())
            standards.extend(self.library_service.get_all_analysis_ig_standards())
            standards.extend(self.library_service.get_tig_standards())

            variable_codelist_maps = await self._get_variable_codelist_maps(standards)
            self.cache.add_batch(variable_codelist_maps, "name")

            # save details of all standards to cache
            standards_details: List[
                dict
            ] = await self._async_get_details_of_all_standards(standards)
            self.cache.add_batch(standards_details, "cache_key", pop_cache_key=True)

            # save details of all standard's models to cache
            standards_models: Iterable[
                dict
            ] = await self._async_get_details_of_all_standards_models(standards_details)
            self.cache.add_batch(standards_models, "cache_key", pop_cache_key=True)

            # save variables metadata to cache
            variables_metadata: Iterable[dict] = await self._get_variables_metadata(
                standards
            )
            self.cache.add_batch(variables_metadata, "cache_key", pop_cache_key=True)
        else:
            raise ValueError(
                "Must Specify either local_rules_path and local_rules_id, remove_local_rules, or neither"
            )
        return self.cache

    async def _get_local_rules(self, local_rules_path: str) -> List[dict]:
        """
        Retrieve local rules from the file system.
        """
        rules = []
        custom_ids = set()
        # Ensure the directory exists
        if not os.path.isdir(local_rules_path):
            raise FileNotFoundError(f"The directory {local_rules_path} does not exist")
        rule_files = [
            os.path.join(local_rules_path, file)
            for file in os.listdir(local_rules_path)
            if file.endswith((".json", ".yml", ".yaml"))
        ]
        # Iterate through all files in the directory provided
        for rule_file in rule_files:
            rule = load_and_parse_local_rule(rule_file)
            if rule and rule["custom_id"] not in custom_ids:
                rules.append(rule)
                custom_ids.add(rule["custom_id"])
            else:
                print(f"Skipping rule with duplicate custom_id: {rule['custom_id']}")
        return rules

    async def load_codelists(self, packages: List[str]):
        coroutines = [
            self._async_get_codelist_terms_map(package) for package in packages
        ]
        codelist_term_maps = await asyncio.gather(*coroutines)
        self.cache.add_batch(codelist_term_maps, "package")

    async def load_available_ct_packages(self):
        packages = self.library_service.get_all_ct_packages()
        available_packages = [
            package.get("href", "").split("/")[-1] for package in packages
        ]
        self.cache.add(PUBLISHED_CT_PACKAGES, available_packages)

    async def load_standard(
        self, standard: str, version: str, standard_substandard: str = None
    ):
        if not standard_substandard:
            standards = [{"href": f"/mdr/{standard}/{version}"}]
            variable_codelist_maps = await self._get_variable_codelist_maps(standards)
            self.cache.add_batch(variable_codelist_maps, "name")
        else:
            standards = [
                {"href": f"/mdr/integrated/{standard}/{version}/{standard_substandard}"}
            ]
            variable_codelist_maps = await self._get_variable_codelist_maps(standards)
            self.cache.add_batch(variable_codelist_maps, "name")
        # save details of all standards to cache
        standards_details: List[dict] = await self._async_get_details_of_all_standards(
            standards
        )
        self.cache.add_batch(standards_details, "cache_key", pop_cache_key=True)

        # save details of all standard's models to cache
        standards_models: Iterable[
            dict
        ] = await self._async_get_details_of_all_standards_models(standards_details)
        self.cache.add_batch(standards_models, "cache_key", pop_cache_key=True)
        # save variables metadata to cache
        variables_metadata: Iterable[dict] = await self._get_variables_metadata(
            standards
        )
        self.cache.add_batch(variables_metadata, "cache_key", pop_cache_key=True)

    def remove_specified_rules(self, cache):
        pickle_file = os.path.join(cache, "local_rules.pkl")
        if os.path.exists(pickle_file):
            try:
                with open(pickle_file, "rb") as f:
                    existing_rules = pickle.load(f)
                print(f"Loaded {len(existing_rules)} rules from {pickle_file}")
                for key, value in existing_rules.items():
                    self.cache.add(key, value)
            except Exception as e:
                print(f"Error loading rules from {pickle_file}: {e}")
        else:
            print(f"No existing rules file found at {pickle_file}")

        # Remove specified rules
        if self.remove_local_rules == "ALL":
            print("Clearing all local rules")
            self.cache.clear_all("local/")
        else:
            prefix_to_remove = f"local/{self.remove_local_rules}/"
            print(f"Clearing rules with prefix: {prefix_to_remove}")
            self.cache.clear_all(prefix_to_remove)

        remaining_rules = self.cache.filter_cache(prefix="local/")
        print(f"Remaining local rules after removal: {len(remaining_rules)}")

    def save_rules_locally(self, file_path: str):
        """
        Store cached rules in rules.pkl in cache path directory
        """
        rules_data = self.cache.filter_cache("rules")
        with open(file_path, "wb") as f:
            pickle.dump(rules_data, f)

    def save_removed_rules_locally(self, file_path: str, remove_rules: str):
        """
        Store rules remaining after removal in cache path directory
        """
        remaining_rules = self.cache.filter_cache(prefix="local/")
        try:
            with open(file_path, "wb") as f:
                pickle.dump(remaining_rules, f)
            print(f"Successfully saved remaining rules to {file_path}")
        except Exception as e:
            print(f"Error occurred while writing remaining rules to file: {e}")

    def save_local_rules_locally(self, file_path: str, local_rules_id: str):
        """
        Store cached local rules in local_rules.pkl in cache path directory
        """
        existing_rules = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, "rb") as f:
                    existing_rules = pickle.load(f)
            except Exception as e:
                print(f"Error loading existing rules: {e}")
        current_prefix = f"local/{local_rules_id}/"
        if any(rule.startswith(current_prefix) for rule in existing_rules):
            raise ValueError(
                f"Rules with prefix '{current_prefix}' already exist in the cache."
            )
        current_rules = self.cache.filter_cache(prefix=current_prefix)
        existing_rules = {
            k: v for k, v in existing_rules.items() if not k.startswith(current_prefix)
        }
        for key, value in current_rules.items():
            existing_rules[key] = value

        # Save updated rules
        try:
            with open(file_path, "wb") as f:
                pickle.dump(existing_rules, f)
            print(f"Successfully saved updated local rules to {file_path}")
        except Exception as e:
            print(f"Error occurred while writing to file: {e}")

    def save_ct_packages_locally(self, file_path: str):
        """
        Store cached ct pacakage metadata in
        codelist_term_maps.pkl in cache path directory
        """
        cts = self.cache.get_by_regex("*ct-*")
        for ct in cts:
            with open(os.path.join(file_path, f"{ct}.pkl"), "wb") as f:
                pickle.dump(cts.get(ct), f)

    def save_variable_codelist_maps_locally(self, file_path: str):
        """
        Store cached variable codelist metadata in
        variable_codelist_maps.pkl in cache path directory
        """
        variable_codelist_maps = self.cache.get_by_regex("*-codelists*")
        with open(file_path, "wb") as f:
            pickle.dump(variable_codelist_maps, f)

    def save_standards_metadata_locally(self, file_path: str):
        """
        Store cached standards metadata in standards_details.pkl in cache path directory
        """
        standards = self.cache.filter_cache("standards")
        with open(file_path, "wb") as f:
            pickle.dump(standards, f)

    def save_standards_models_locally(self, file_path: str):
        """
        Store cached standards models metadata in
        standards_models.pkl in cache path directory
        """
        standards_models = self.cache.filter_cache("models")
        with open(file_path, "wb") as f:
            pickle.dump(standards_models, f)

    def save_variables_metadata_locally(self, file_path: str):
        """
        Store cached variables metadata in
        variables_metadata.pkl in cache path directory
        """
        variables_metadata = self.cache.filter_cache("library_variables_metadata")
        with open(file_path, "wb") as f:
            pickle.dump(variables_metadata, f)

    async def _get_rules_from_cdisc_library(self) -> List[List[dict]]:
        """
        Requests rules from CDISC Library.
        """
        catalogs = self.library_service.get_all_rule_catalogs()
        coroutines = [
            self._async_get_rules_by_catalog(catalog.get("href"))
            for catalog in catalogs
        ]
        rules = await asyncio.gather(*coroutines)
        return rules

    async def _async_get_rules_by_catalog(self, catalog_link: str) -> List[dict]:
        loop = asyncio.get_event_loop()
        standard = catalog_link.split("/")[-2]
        standard_version = catalog_link.split("/")[-1]
        rules: dict = await loop.run_in_executor(
            None, self.library_service.get_rules_by_catalog, standard, standard_version
        )
        return rules

    async def _get_codelist_term_maps(self) -> List[dict]:
        """
        For each CT package in CDISC library, creates mapping with:
        1. Submission value lookup: Map of submission values to codelist/term IDs
        2. Full codelist data: Complete metadata and terms keyed by codelist ID
        {
            "package": "adamct-2024-03-29",
            "submission_lookup": {
                "GAD02PC": {"codelist": "C172334", "term": "N/A"},     # this is at codelist level
                "GAD02TS": {"codelist": "C172334", "term": "C172451"}, # this is at term level
            "C172334": {
            "definition": "A parameter code codelist for the Generalized Anxiety Disorder - 7 Version 2 Questionnaire
            (GAD-7 V2) to support the calculation of total score in ADaM.",
            "extensible": False,
            "name": "Generalized Anxiety Disorder - 7 Version 2 Questionnaire Parameter Code",
            "preferredTerm": "CDISC ADaM Generalized Anxiety Disorder-7 Version 2 Questionnaire Parameter
            Code Terminology",
            "submissionValue": "GAD02PC",
            "synonyms": ["Generalized Anxiety Disorder - 7 Version 2 Questionnaire Parameter Code"],
            "terms": [{
                "conceptId": "C172451",
                "definition": "Generalized Anxiety Disorder - 7 Version 2 - Total score used for analysis.",
                "preferredTerm": "Generalized Anxiety Disorder - 7 Version 2 - Total Score for Analysis",
                "submissionValue": "GAD02TS",
                "synonyms": ["GAD02-Total Score - Analysis"],
                "extensible": False
        }]
        }
        """
        packages = self.library_service.get_all_ct_packages()
        coroutines = [
            self._async_get_codelist_terms_map(package.get("href", "").split("/")[-1])
            for package in packages
        ]
        codelist_term_maps = await asyncio.gather(*coroutines)
        return codelist_term_maps

    async def _async_get_codelist_terms_map(self, package_version: str) -> dict:
        loop = asyncio.get_event_loop()
        terms_map: dict = await loop.run_in_executor(
            None, self.library_service.get_codelist_terms_map, package_version
        )
        return terms_map

    async def _get_variable_codelist_maps(self, standards: List[dict]) -> List[dict]:
        coroutines = []
        for standard in standards:
            href_parts = standard.get("href", "").split("/")
            if len(href_parts) >= 5 and href_parts[-4] == "integrated":
                coroutines.append(
                    self._async_get_variable_codelist_map(
                        href_parts[-3], href_parts[-2], href_parts[-1]
                    )
                )
            else:
                coroutines.append(
                    self._async_get_variable_codelist_map(
                        href_parts[-2], href_parts[-1]
                    )
                )
        variable_codelist_maps = await asyncio.gather(*coroutines)
        return variable_codelist_maps

    async def _async_get_variable_codelist_map(
        self,
        standard_type: str,
        standard_version: str,
        standard_substandard: str = None,
    ) -> dict:
        loop = asyncio.get_event_loop()
        variables_map: dict = await loop.run_in_executor(
            None,
            self.library_service.get_variable_codelists_map,
            standard_type,
            standard_version,
            standard_substandard,
        )
        return variables_map

    async def _async_get_details_of_all_standards(
        self, standards: List[dict]
    ) -> List[dict]:
        """
        Gets details for each given standard.
        """
        coroutines = []
        for standard in standards:
            href_parts = standard.get("href", "").split("/")
            if len(href_parts) >= 5 and href_parts[-4] == "integrated":
                coroutines.append(
                    self._async_get_standard_details(
                        href_parts[-3], href_parts[-2], href_parts[-1]
                    )
                )
            else:
                coroutines.append(
                    self._async_get_standard_details(href_parts[-2], href_parts[-1])
                )
        return await asyncio.gather(*coroutines)

    async def _async_get_standard_details(
        self,
        standard_type: str,
        standard_version: str,
        standard_substandard: str = None,
    ) -> dict:
        """
        Gets details of a given standard.
        """
        loop = asyncio.get_event_loop()
        standard_details: dict = await loop.run_in_executor(
            None,
            self.library_service.get_standard_details,
            standard_type,
            standard_version,
            standard_substandard,
        )
        standard_details["cache_key"] = get_standard_details_cache_key(
            standard_type, standard_version, standard_substandard
        )
        return standard_details

    async def _async_get_details_of_all_standards_models(
        self, standards_details: List[dict]
    ) -> Iterable[dict]:
        """
        Returns a list of dicts containing model metadata for each standard.
        """
        coroutines = [
            self._async_get_details_of_standard_model(standard)
            for standard in standards_details
        ]
        standards_models: Iterable[dict] = await asyncio.gather(*coroutines)
        return filter(lambda item: item is not None, standards_models)

    async def _async_get_details_of_standard_model(
        self, standard_details: dict
    ) -> Optional[dict]:
        """
        Returns details of a standard model as a dictionary.
        """
        loop = asyncio.get_event_loop()
        model: Optional[dict] = await loop.run_in_executor(
            None, self.library_service.get_model_details, standard_details
        )
        if not model:
            return
        model["cache_key"] = get_model_details_cache_key(
            model["standard_type"], model["version"]
        )
        return model

    async def _get_variables_metadata(self, standards: List[dict]) -> Iterable[dict]:
        """
        Returns a list of dicts of variables metadata for each standard.
        """
        coroutines = []
        for standard in standards:
            href_parts = standard.get("href", "").split("/")
            if len(href_parts) >= 5 and href_parts[-4] == "integrated":
                coroutines.append(
                    self._async_get_variables_metadata(
                        href_parts[-3], href_parts[-2], href_parts[-1]
                    )
                )
            else:
                coroutines.append(
                    self._async_get_variables_metadata(href_parts[-2], href_parts[-1])
                )
        metadata = await asyncio.gather(*coroutines)
        return filter(lambda item: item is not None, metadata)

    async def _async_get_variables_metadata(
        self,
        standard_type: str,
        standard_version: str,
        standard_substandard: str = None,
    ) -> Optional[dict]:
        """
        Returns variables metadata for a given standard.
        """
        loop = asyncio.get_event_loop()
        try:
            variables_metadata: dict = await loop.run_in_executor(
                None,
                partial(
                    self.library_service.get_variables_details,
                    standard_type,
                    standard_version,
                    standard_substandard,
                ),
            )
        except LibraryResourceNotFoundException:
            return None
        return {
            "cache_key": get_library_variables_metadata_cache_key(
                standard_type, standard_version, standard_substandard
            ),
            **variables_metadata,
        }
