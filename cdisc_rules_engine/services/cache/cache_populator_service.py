import asyncio
import pickle
import json
from functools import partial
from typing import Iterable, List, Optional
import os
from cdisc_library_client.custom_exceptions import (
    ResourceNotFoundException as LibraryResourceNotFoundException,
)

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.utilities.utils import (
    get_library_variables_metadata_cache_key,
    get_standard_details_cache_key,
    get_model_details_cache_key,
)
from scripts.script_utils import load_and_parse_rule
from cdisc_rules_engine.constants.cache_constants import PUBLISHED_CT_PACKAGES


class CachePopulator:
    def __init__(
        self,
        cache: CacheServiceInterface,
        library_service: CDISCLibraryService = None,
        custom_rules_directory=None,
        custom_rule_path=None,
        remove_custom_rules=None,
        update_custom_rule=None,
        custom_standards=None,
        remove_custom_standards=None,
        cache_path="",
    ):
        self.cache = cache
        self.library_service = library_service
        self.custom_rules_directory = custom_rules_directory
        self.custom_rule_path = custom_rule_path
        self.remove_custom_rules = remove_custom_rules
        self.update_custom_rule = update_custom_rule
        self.custom_standards = custom_standards
        self.remove_custom_standards = remove_custom_standards
        self.cache_path = cache_path

    async def update_cache(self):
        coroutines = (
            self.save_ct_packages_locally(),
            self.save_rules_locally(),
            self.save_standards_metadata_locally(),
        )
        await asyncio.gather(*coroutines)

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
        standards_models: Iterable[dict] = (
            await self._async_get_details_of_all_standards_models(standards_details)
        )
        self.cache.add_batch(standards_models, "cache_key", pop_cache_key=True)
        # save variables metadata to cache
        variables_metadata: Iterable[dict] = await self._get_variables_metadata(
            standards
        )
        self.cache.add_batch(variables_metadata, "cache_key", pop_cache_key=True)

    async def save_rules_locally(self):
        """
        Creates rules_directory, a map from standard/version/substandard to rule IDs
        and rules.pkl, a flat dictionary with core_ids as keys and rule objects, in cache path directory
        """
        rules_lists = await self._get_rules_from_cdisc_library()

        rules_directory = {}
        rules_by_core_id = {}

        rules_directory = self.process_catalogs(rules_lists, rules_directory)
        rules_directory, rules_by_core_id = self.process_rules_lists(
            rules_directory, rules_by_core_id, rules_lists
        )

        with open(
            os.path.join(self.cache_path, DefaultFilePaths.RULES_CACHE_FILE.value), "wb"
        ) as f:
            pickle.dump(rules_by_core_id, f)

        with open(
            os.path.join(self.cache_path, DefaultFilePaths.RULES_DICTIONARY.value), "wb"
        ) as f:
            pickle.dump(rules_directory, f)

    def process_catalogs(self, rules_lists, rules_directory):
        """
        Process key_prefixes from rules_lists to initialize the rules_directory structure
        """
        for catalog_rules in rules_lists:
            key = catalog_rules.get("key_prefix", "")
            parts = key.split("/")
            standard = parts[0]
            version = parts[1]
            # need special handling for adam, as it has a different structure "adam/adamig-1-1"
            if standard.lower() == "adam":
                version_parts = version.split("-")
                new_standard = version_parts[0]
                new_version = "-".join(version_parts[1:])
                key = f"{new_standard}/{new_version}"
            rules_directory[key] = []

        return rules_directory

    def process_rules_lists(self, rules_directory, rules_by_core_id, rules_lists):
        """
        Process rules lists to populate rules_directory and rules_by_core_id
        """
        for catalog_rules in rules_lists:
            rules = catalog_rules.get("rules", [])
            for rule in rules:
                core_id = rule.get("core_id", "")
                standards = rule.get("standards", [])
                for standard in standards:
                    std_name = standard.get("Name", "").lower()
                    std_version = standard.get("Version", "").replace(".", "-")
                    std_substandard = standard.get("Substandard", None)

                    if std_name and std_version:
                        key = f"{std_name}/{std_version}"
                        if (
                            key in rules_directory
                            and core_id not in rules_directory[key]
                        ):
                            rules_directory[key].append(core_id)
                        if std_substandard:
                            sub_key = f"{key}/{std_substandard.lower()}"
                            if sub_key not in rules_directory:
                                rules_directory[sub_key] = []
                            if core_id not in rules_directory[sub_key]:
                                rules_directory[sub_key].append(core_id)

                rules_by_core_id[core_id] = rule
        return rules_directory, rules_by_core_id

    async def save_ct_packages_locally(self):
        """
        Store cached ct package metadata in
        codelist_term_maps.pkl in cache path directory
        """
        # save codelists to cache as a map of codelist to terms
        codelist_term_maps = await self._get_codelist_term_maps()
        for package in codelist_term_maps:
            with open(
                os.path.join(self.cache_path, f"{package['package']}.pkl"), "wb"
            ) as f:
                pickle.dump(package, f)

    @staticmethod
    def _remove_cache_key(item: dict):
        item.pop("cache_key", None)
        return item

    def _save_standard(
        self, item_list: List[dict], cache_key: str, path: DefaultFilePaths
    ):
        item_dict = {
            item[cache_key]: self._remove_cache_key(item) for item in item_list
        }
        with open(
            os.path.join(self.cache_path, path.value),
            "wb",
        ) as f:
            pickle.dump(
                item_dict,
                f,
            )

    async def save_standards_metadata_locally(self):
        """
        Store cached standards metadata in standards_details.pkl in cache path directory
        """
        standards = self.library_service.get_all_tabulation_ig_standards()
        standards.extend(self.library_service.get_all_collection_ig_standards())
        standards.extend(self.library_service.get_all_analysis_ig_standards())
        standards.extend(self.library_service.get_tig_standards())

        coroutines = [
            self._async_get_details_of_all_standards(standards),
            self._get_variable_codelist_maps(standards),
            self._get_variables_metadata(standards),
        ]
        standards_details = await coroutines[0]
        coroutines.append(
            self._async_get_details_of_all_standards_models(standards_details)
        )

        item_lists = (standards_details, *(await asyncio.gather(*coroutines[1:4])))
        for index, args in enumerate(
            (
                # details of all standards
                (
                    "cache_key",
                    DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE,
                ),
                # map of variable to allowed_values
                (
                    "name",
                    DefaultFilePaths.VARIABLE_CODELIST_CACHE_FILE,
                ),
                # variables metadata
                (
                    "cache_key",
                    DefaultFilePaths.VARIABLE_METADATA_CACHE_FILE,
                ),
                # details of all standard's models
                (
                    "cache_key",
                    DefaultFilePaths.STANDARD_MODELS_CACHE_FILE,
                ),
            )
        ):
            self._save_standard(item_lists[index], *args)

    async def _get_rules_from_cdisc_library(self):
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

    async def _async_get_rules_by_catalog(self, catalog_link: str):
        loop = asyncio.get_event_loop()
        standard = catalog_link.split("/")[-2]
        standard_version = catalog_link.split("/")[-1]
        rules = await loop.run_in_executor(
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

    def add_custom_rules(self):  # noqa
        """Process and save the rule(s) from the specified directory or file path."""
        rule_files = []
        if self.custom_rules_directory and os.path.isdir(self.custom_rules_directory):
            for file in os.listdir(self.custom_rules_directory):
                if file.endswith((".json", ".yml", ".yaml")):
                    rule_files.append(os.path.join(self.custom_rules_directory, file))
            if not rule_files:
                raise ValueError(
                    f"No rule files found in {self.custom_rules_directory}"
                )
        elif self.custom_rule_path:
            for path in self.custom_rule_path:
                if os.path.isfile(path) and path.endswith((".json", ".yml", ".yaml")):
                    rule_files.append(path)
            else:
                print(f"Warning: {path} is not a valid file. Skipping.")
        else:
            raise ValueError("Invalid directory or path specified")
        custom_rules_file = os.path.join(
            self.cache_path, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value
        )
        existing_rules = {}

        if os.path.exists(custom_rules_file):
            with open(custom_rules_file, "rb") as f:
                existing_rules = pickle.load(f)
        skipped, added = self.parse_and_save_custom_rules(
            rule_files, existing_rules, custom_rules_file
        )
        if added:
            print(f"Added {len(added)} rules: {', '.join(added)}")
        else:
            print("No rules added")
        if skipped:
            print(f"Skipped {len(skipped)} rules: {', '.join(skipped)}")

    def parse_and_save_custom_rules(
        self,
        rule_files: List[str],
        existing_rules: dict,
        custom_rules_file: str,
        allow_updates=False,
    ):
        skipped_rules = []
        added_rules = []
        for rule_file in rule_files:
            try:
                rule_dict = load_and_parse_rule(rule_file)
                core_id = rule_dict["core_id"]
                if core_id in existing_rules and not allow_updates:
                    print(
                        f"Rule {core_id} already exists. Use update_custom_rule to update it."
                    )
                    skipped_rules.append(core_id)
                    continue
                existing_rules[core_id] = rule_dict
                added_rules.append(core_id)
            except Exception as e:
                print(f"Error processing rule file {rule_file}: {e}")
        if len(added_rules) > 0:
            with open(custom_rules_file, "wb") as f:
                pickle.dump(existing_rules, f)
        else:
            print("No rules were added as all currently exist in the cache.")
        return skipped_rules, added_rules

    def update_custom_rule_in_cache(self):
        """Update an existing custom rule in the cache."""
        rule_files = []
        if self.update_custom_rule and os.path.isfile(self.update_custom_rule):
            rule_files = [self.update_custom_rule]
        else:
            raise ValueError(f"{self.update_custom_rule} is an invalid file")
        custom_rules_file = os.path.join(
            self.cache_path, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value
        )
        existing_rules = {}
        if os.path.exists(custom_rules_file):
            with open(custom_rules_file, "rb") as f:
                existing_rules = pickle.load(f)
        skipped_rules, updated_rules = self.parse_and_save_custom_rules(
            rule_files, existing_rules, custom_rules_file, allow_updates=True
        )
        if updated_rules:
            print(f"Updated rule: {''.join(updated_rules)}")
        else:
            print("Failed to update rule")

    def remove_custom_rules_from_cache(self):
        """
        Remove specified custom rules from the cache.
        """
        if not self.remove_custom_rules:
            raise ValueError("No rules specified for removal")
        custom_rules_file = os.path.join(
            self.cache_path, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value
        )
        if not os.path.exists(custom_rules_file):
            raise ValueError("No custom rules cache found. Nothing to remove.")

        with open(custom_rules_file, "rb") as f:
            existing_rules = pickle.load(f)
        removed_rules = []
        if self.remove_custom_rules.upper() == "ALL":
            removed_rules = list(existing_rules.keys())
            existing_rules = {}
        else:
            rule_ids_to_remove = [
                rule_id.strip() for rule_id in self.remove_custom_rules.split(",")
            ]
            for rule_id in rule_ids_to_remove:
                if rule_id in existing_rules:
                    del existing_rules[rule_id]
                    removed_rules.append(rule_id)
                else:
                    print(f"Rule {rule_id} not found in cache. Skipping.")

        if removed_rules:
            with open(custom_rules_file, "wb") as f:
                pickle.dump(existing_rules, f)
        return removed_rules

    def add_custom_standard_to_cache(self):
        """Add or update a custom standard to the cache."""
        if not os.path.isfile(self.custom_standards):
            raise ValueError("Invalid standard filepath")
        with open(self.custom_standards, "r") as f:
            new_standard = json.load(f)

        # Validate the input format
        if not isinstance(new_standard, dict):
            raise ValueError("Custom standard must be a dictionary")

        # Load existing standards if available
        custom_standards_file = os.path.join(
            self.cache_path, "custom_rules_dictionary.pkl"
        )
        custom_rules_dictionary = {}

        if os.path.exists(custom_standards_file):
            with open(custom_standards_file, "rb") as f:
                custom_rules_dictionary = pickle.load(f)
            print(
                f"Loaded existing custom standards dictionary with {len(custom_rules_dictionary)} entries"
            )

        # Update the dictionary with new standard(s)
        updated_count = 0
        new_count = 0
        for standard_key, rule_ids in new_standard.items():
            if standard_key in custom_rules_dictionary:
                custom_rules_dictionary[standard_key] = rule_ids
                updated_count += 1
            else:
                custom_rules_dictionary[standard_key] = rule_ids
                new_count += 1

        # Save the updated dictionary
        with open(custom_standards_file, "wb") as f:
            pickle.dump(custom_rules_dictionary, f)

        if updated_count > 0 and new_count > 0:
            print(
                f"Updated {updated_count} existing standards and added {new_count} new standards"
            )
        elif updated_count > 0:
            print(f"Updated {updated_count} existing standards")
        else:
            print(f"Added {new_count} new standards")

    def remove_custom_standards_from_cache(self):
        """Remove custom standards from the cache."""
        if not self.remove_custom_standards:
            return

        # Load the existing dictionary
        custom_standards_file = os.path.join(
            self.cache_path, "custom_rules_dictionary.pkl"
        )
        if not os.path.exists(custom_standards_file):
            print(f"No custom standards file found at {custom_standards_file}")
            return

        with open(custom_standards_file, "rb") as f:
            custom_rules_dictionary = pickle.load(f)

        # Remove each specified standard
        removed_count = 0
        for standard_key in self.remove_custom_standards:
            if standard_key in custom_rules_dictionary:
                del custom_rules_dictionary[standard_key]
                removed_count += 1
                print(f"Removed standard '{standard_key}'")
            else:
                print(f"Standard '{standard_key}' not found")

        # Save the updated dictionary
        with open(custom_standards_file, "wb") as f:
            pickle.dump(custom_rules_dictionary, f)

        print(
            f"Removed {removed_count} standards. Remaining standards: {len(custom_rules_dictionary)}"
        )
