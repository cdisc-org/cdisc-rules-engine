import asyncio
import pickle
import json
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from functools import partial
from typing import Dict, Iterable, List, Optional, Tuple
import os
from cdisc_library_client.custom_exceptions import (
    ResourceNotFoundException as LibraryResourceNotFoundException,
)

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.utilities.utils import (
    get_library_variables_metadata_cache_key,
    get_standard_details_cache_key,
    get_model_details_cache_key,
)
from scripts.script_utils import load_and_parse_rule
from cdisc_rules_engine.constants.cache_constants import PUBLISHED_CT_PACKAGES

# the parts of update_cache, downloaded in parallel
BRANCH_CT_PACKAGES = "CT packages"
BRANCH_RULES = "rules"
BRANCH_STANDARDS = "standards metadata"
UPDATE_CACHE_BRANCHES = (BRANCH_CT_PACKAGES, BRANCH_RULES, BRANCH_STANDARDS)


def is_library_access_error(error: Exception) -> bool:
    """Whether CDISC Library refused access (401), e.g. members-only content for a key without member access."""
    # cdisc-library-client >= 0.1.7 raises LibraryAccessError (status_code 401), older versions a plain Exception
    return getattr(error, "status_code", None) == 401 or "returned unsuccessful 401 response" in str(error)


@dataclass
class CacheUpdateReport:
    """
    Outcome of update_cache: the branches that failed as a whole (keeping their previously cached files),
    and the single items skipped within the other branches (keeping their previously cached version).
    """

    failed_branches: Dict[str, str] = field(default_factory=dict)
    skipped_items: Dict[str, List[str]] = field(default_factory=dict)
    access_denied: bool = False
    # rules with a JSONata check: cached, but not listed in the rules directory, as this engine cannot execute them
    unlisted_jsonata_rules: set = field(default_factory=set)

    def record_branch_failure(self, branch: str, error: Exception):
        self.failed_branches[branch] = f"{type(error).__name__}: {error}"
        self.access_denied = self.access_denied or is_library_access_error(error)

    def record_skipped_item(self, branch: str, item: str, error: Exception):
        self.skipped_items.setdefault(branch, []).append(f"{item} ({type(error).__name__}: {error})")
        self.access_denied = self.access_denied or is_library_access_error(error)

    @property
    def succeeded(self) -> bool:
        """At least one branch was updated, and CDISC Library did not refuse access anywhere."""
        return len(self.failed_branches) < len(UPDATE_CACHE_BRANCHES) and not self.access_denied

    def summary(self, max_items_per_branch: int = 10) -> str:
        note = (
            f"\n{len(self.unlisted_jsonata_rules)} rules with a JSONata check were cached but not listed for "
            "validation, as this engine does not support JSONata."
            if self.unlisted_jsonata_rules
            else ""
        )
        if not self.failed_branches and not self.skipped_items:
            return "Cache updated successfully" + note
        lines = ["Cache update completed with problems:" if self.succeeded else "Cache update failed:"]
        for branch in UPDATE_CACHE_BRANCHES:
            if branch in self.failed_branches:
                lines.append(f"- {branch}: NOT updated, previous cache kept - {self.failed_branches[branch]}")
                continue
            skipped = self.skipped_items.get(branch, [])
            lines.append(
                f"- {branch}: updated" + (f", {len(skipped)} skipped (previous version kept):" if skipped else "")
            )
            lines.extend(f"    {item}" for item in skipped[:max_items_per_branch])
            if len(skipped) > max_items_per_branch:
                lines.append(f"    ... and {len(skipped) - max_items_per_branch} more (see the log)")
        if self.access_denied:
            lines.append(
                "CDISC Library refused access (401): the API key is invalid, expired, or lacks the member access "
                "needed for members-only content such as CT packages and standards metadata."
            )
        return "\n".join(lines) + note


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
        self.report = CacheUpdateReport()

    async def update_cache(self) -> CacheUpdateReport:
        """
        Downloads CT packages, rules and standards metadata from CDISC Library, in parallel branches.
        A failure not tied to a single item (e.g. listing the CT packages) only fails its own branch, which
        then keeps its previously cached files. A failure of a single item (a rule, a CT package, a standard)
        only skips that item, which keeps its previously cached version.
        """
        self.report = CacheUpdateReport()
        branches = {
            BRANCH_CT_PACKAGES: self.save_ct_packages_locally(),
            BRANCH_RULES: self.save_rules_locally(),
            BRANCH_STANDARDS: self.save_standards_metadata_locally(),
        }
        results = await asyncio.gather(*branches.values(), return_exceptions=True)
        for branch, result in zip(branches, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to update the cached {branch}: {result}")
                self.report.record_branch_failure(branch, result)
            elif isinstance(result, BaseException):
                raise result
        return self.report

    async def _skip_on_failure(self, branch: Optional[str], item: str, awaitable):
        """
        Awaits the download of a single item. With a branch, a failure is reported and the item skipped (None);
        without one (outside of update_cache), the failure is raised as usual.
        """
        if branch is None:
            return await awaitable
        try:
            return await awaitable
        except Exception as e:
            logger.warning(f"Skipped {item} while updating the cached {branch}: {e}")
            self.report.record_skipped_item(branch, item, e)
            return None

    def _is_partial(self, branch: str) -> bool:
        return bool(self.report.skipped_items.get(branch))

    @staticmethod
    def _dump_pickles(files: List[Tuple[str, dict, bool]]):
        """
        Writes each (path, data, merge_with_existing) to a temporary file first, and only then moves them all into
        place, so that a failed or interrupted write leaves the cached files as they were.
        With merge_with_existing, entries of the existing file that are missing from data are kept.
        """
        temporary_paths = []
        try:
            for path, data, merge_with_existing in files:
                if merge_with_existing and os.path.exists(path):
                    with open(path, "rb") as f:
                        data = {**pickle.load(f), **data}
                temporary_paths.append(f"{path}.{uuid.uuid4().hex}.partial")
                with open(temporary_paths[-1], "wb") as f:
                    pickle.dump(data, f)
        except BaseException:
            for temporary_path in temporary_paths:
                with suppress(FileNotFoundError):
                    os.remove(temporary_path)
            raise
        for (path, _, _), temporary_path in zip(files, temporary_paths):
            os.replace(temporary_path, path)

    async def load_codelists(self, packages: List[str]):
        coroutines = [self._async_get_codelist_terms_map(package) for package in packages]
        codelist_term_maps = await asyncio.gather(*coroutines)
        self.cache.add_batch(codelist_term_maps, "package")

    async def load_available_ct_packages(self):
        packages = self.library_service.get_all_ct_packages()
        available_packages = [package.get("href", "").split("/")[-1] for package in packages]
        self.cache.add(PUBLISHED_CT_PACKAGES, available_packages)

    async def load_standard(self, standard: str, version: str, standard_substandard: str = None):
        if not standard_substandard:
            standards = [{"href": f"/mdr/{standard}/{version}"}]
            variable_codelist_maps = await self._get_variable_codelist_maps(standards)
            self.cache.add_batch(variable_codelist_maps, "name")
        else:
            standards = [{"href": f"/mdr/integrated/{standard}/{version}/{standard_substandard}"}]
            variable_codelist_maps = await self._get_variable_codelist_maps(standards)
            self.cache.add_batch(variable_codelist_maps, "name")
        # save details of all standards to cache
        standards_details: List[dict] = await self._async_get_details_of_all_standards(standards)
        self.cache.add_batch(standards_details, "cache_key", pop_cache_key=True)

        # save details of all standard's models to cache
        standards_models: Iterable[dict] = await self._async_get_details_of_all_standards_models(standards_details)
        self.cache.add_batch(standards_models, "cache_key", pop_cache_key=True)
        # save variables metadata to cache
        variables_metadata: Iterable[dict] = await self._get_variables_metadata(standards)
        self.cache.add_batch(variables_metadata, "cache_key", pop_cache_key=True)

    async def save_rules_locally(self):
        """
        Creates rules_directory, a map from standard/version/substandard to rule IDs
        and rules.pkl, a flat dictionary with core_ids as keys and rule objects, in cache path directory
        """
        rules_lists = await self._get_rules_from_cdisc_library()
        for catalog_rules in rules_lists:
            for core_id, error in catalog_rules.get("skipped_rules", []):
                item = f"rule {core_id} ({catalog_rules.get('key_prefix')})"
                logger.warning(f"Skipped {item} while updating the cached {BRANCH_RULES}: {error}")
                self.report.record_skipped_item(BRANCH_RULES, item, error)

        rules_directory = {}
        rules_by_core_id = {}

        rules_directory = self.process_catalogs(rules_lists, rules_directory)
        rules_directory, rules_by_core_id = self.process_rules_lists(rules_directory, rules_by_core_id, rules_lists)

        rules_path = os.path.join(self.cache_path, DefaultFilePaths.RULES_CACHE_FILE.value)
        directory_path = os.path.join(self.cache_path, DefaultFilePaths.RULES_DICTIONARY.value)
        partial_update = self._is_partial(BRANCH_RULES)
        if partial_update and os.path.exists(directory_path):
            # skipped rules and catalogs keep their cached version, so they stay listed too
            with open(directory_path, "rb") as f:
                for key, core_ids in pickle.load(f).items():
                    listed = rules_directory.setdefault(key, [])
                    listed.extend(
                        core_id
                        for core_id in core_ids
                        if core_id not in listed and core_id not in self.report.unlisted_jsonata_rules
                    )
        self._dump_pickles([(rules_path, rules_by_core_id, partial_update), (directory_path, rules_directory, False)])

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
                try:
                    directory_keys = self._rule_directory_keys(rule)
                except Exception as e:
                    # e.g. a standard without a name or version: only this rule is skipped
                    item = f"rule {core_id} ({catalog_rules.get('key_prefix')})"
                    logger.warning(f"Skipped {item} while updating the cached {BRANCH_RULES}: {e}")
                    self.report.record_skipped_item(BRANCH_RULES, item, e)
                    continue
                for key, is_substandard in directory_keys:
                    if is_substandard and key not in rules_directory:
                        rules_directory[key] = []
                    if key in rules_directory and core_id not in rules_directory[key]:
                        rules_directory[key].append(core_id)
                rules_by_core_id[core_id] = rule

        # rules with a JSONata check are cached, but not listed, as validation cannot execute them
        jsonata_rules = {
            core_id for core_id, rule in rules_by_core_id.items() if isinstance(rule.get("conditions"), str)
        }
        self.report.unlisted_jsonata_rules |= jsonata_rules
        for core_ids in rules_directory.values():
            core_ids[:] = [core_id for core_id in core_ids if core_id not in jsonata_rules]
        return rules_directory, rules_by_core_id

    @staticmethod
    def _rule_directory_keys(rule: dict) -> List[Tuple[str, bool]]:
        """The rules_directory keys a rule is listed under, each flagged True if it is a substandard key."""
        directory_keys = []
        for standard in rule.get("standards", []):
            std_name = standard.get("Name", "").lower()
            std_version = standard.get("Version", "").replace(".", "-")
            std_substandard = standard.get("Substandard", None)

            if std_name and std_version:
                key = f"{std_name}/{std_version}"
                directory_keys.append((key, False))
                if std_substandard:
                    directory_keys.append((f"{key}/{std_substandard.lower()}", True))
        return directory_keys

    async def save_ct_packages_locally(self):
        """
        Store cached ct package metadata in
        codelist_term_maps.pkl in cache path directory
        """
        # save codelists to cache as a map of codelist to terms
        # one file per package, so skipped packages simply keep their cached file
        codelist_term_maps = await self._get_codelist_term_maps()
        for package in codelist_term_maps:
            self._dump_pickles([(os.path.join(self.cache_path, f"{package['package']}.pkl"), package, False)])

    @staticmethod
    def _remove_cache_key(item: dict):
        item.pop("cache_key", None)
        return item

    def _standard_file(
        self,
        item_list: List[dict],
        cache_key: str,
        path: DefaultFilePaths,
        merge_with_existing: bool = False,
    ) -> Tuple[str, dict, bool]:
        item_dict = {item[cache_key]: self._remove_cache_key(item) for item in item_list}
        return os.path.join(self.cache_path, path.value), item_dict, merge_with_existing

    async def save_standards_metadata_locally(self):
        """
        Store cached standards metadata in standards_details.pkl in cache path directory
        """
        standards = self.library_service.get_all_tabulation_ig_standards()
        standards.extend(self.library_service.get_all_collection_ig_standards())
        standards.extend(self.library_service.get_all_analysis_ig_standards())
        standards.extend(self.library_service.get_tig_standards())

        standards_details = await self._async_get_details_of_all_standards(standards, BRANCH_STANDARDS)
        item_lists = (
            standards_details,
            *await asyncio.gather(
                self._get_variable_codelist_maps(standards, BRANCH_STANDARDS),
                self._get_variables_metadata(standards, BRANCH_STANDARDS),
                self._async_get_details_of_all_standards_models(standards_details, BRANCH_STANDARDS),
            ),
        )
        # with skipped standards, their cached entries are kept
        partial_update = self._is_partial(BRANCH_STANDARDS)
        standard_files = []
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
            standard_files.append(self._standard_file(item_lists[index], *args, merge_with_existing=partial_update))
        self._dump_pickles(standard_files)

    async def _get_rules_from_cdisc_library(self):
        """
        Requests rules from CDISC Library.
        """
        catalogs = self.library_service.get_all_rule_catalogs()
        coroutines = [
            self._skip_on_failure(
                BRANCH_RULES,
                f"rules catalog {catalog.get('href')}",
                self._async_get_rules_by_catalog(catalog.get("href")),
            )
            for catalog in catalogs
        ]
        rules = await asyncio.gather(*coroutines)
        return [catalog_rules for catalog_rules in rules if catalog_rules is not None]

    async def _async_get_rules_by_catalog(self, catalog_link: str):
        loop = asyncio.get_event_loop()
        standard = catalog_link.split("/")[-2]
        standard_version = catalog_link.split("/")[-1]
        rules = await loop.run_in_executor(None, self.library_service.get_rules_by_catalog, standard, standard_version)
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
        package_versions = [package.get("href", "").split("/")[-1] for package in packages]
        coroutines = [
            self._skip_on_failure(
                BRANCH_CT_PACKAGES,
                f"CT package {version}",
                self._async_get_codelist_terms_map(version),
            )
            for version in package_versions
        ]
        codelist_term_maps = await asyncio.gather(*coroutines)
        return [terms_map for terms_map in codelist_term_maps if terms_map is not None]

    async def _async_get_codelist_terms_map(self, package_version: str) -> dict:
        loop = asyncio.get_event_loop()
        terms_map: dict = await loop.run_in_executor(None, self.library_service.get_codelist_terms_map, package_version)
        return terms_map

    @staticmethod
    def _standard_from_href(href: str) -> tuple:
        """(standard type, version[, substandard]) from a standard's library link."""
        href_parts = href.split("/")
        if len(href_parts) >= 5 and href_parts[-4] == "integrated":
            return href_parts[-3], href_parts[-2], href_parts[-1]
        return href_parts[-2], href_parts[-1]

    async def _for_standard(self, fetch, standard: dict):
        """Awaits fetch for a standard, parsing its link here so that a malformed one only fails this standard."""
        return await fetch(*self._standard_from_href(standard.get("href", "")))

    async def _get_variable_codelist_maps(self, standards: List[dict], branch: Optional[str] = None) -> List[dict]:
        coroutines = []
        for standard in standards:
            coroutine = self._for_standard(self._async_get_variable_codelist_map, standard)
            coroutines.append(
                self._skip_on_failure(
                    branch,
                    f"variable codelist map of {standard.get('href')}",
                    coroutine,
                )
            )
        variable_codelist_maps = await asyncio.gather(*coroutines)
        return [item for item in variable_codelist_maps if item is not None]

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
        self, standards: List[dict], branch: Optional[str] = None
    ) -> List[dict]:
        """
        Gets details for each given standard.
        """
        coroutines = []
        for standard in standards:
            coroutine = self._for_standard(self._async_get_standard_details, standard)
            coroutines.append(self._skip_on_failure(branch, f"details of {standard.get('href')}", coroutine))
        standards_details = await asyncio.gather(*coroutines)
        return [item for item in standards_details if item is not None]

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
        self, standards_details: List[dict], branch: Optional[str] = None
    ) -> Iterable[dict]:
        """
        Returns a list of dicts containing model metadata for each standard.
        """
        coroutines = [
            self._skip_on_failure(
                branch,
                f"model of {standard.get('cache_key')}",
                self._async_get_details_of_standard_model(standard),
            )
            for standard in standards_details
        ]
        standards_models: Iterable[dict] = await asyncio.gather(*coroutines)
        return filter(lambda item: item is not None, standards_models)

    async def _async_get_details_of_standard_model(self, standard_details: dict) -> Optional[dict]:
        """
        Returns details of a standard model as a dictionary.
        """
        loop = asyncio.get_event_loop()
        model: Optional[dict] = await loop.run_in_executor(
            None, self.library_service.get_model_details, standard_details
        )
        if not model:
            return
        model["cache_key"] = get_model_details_cache_key(model["standard_type"], model["version"])
        return model

    async def _get_variables_metadata(self, standards: List[dict], branch: Optional[str] = None) -> Iterable[dict]:
        """
        Returns a list of dicts of variables metadata for each standard.
        """
        coroutines = []
        for standard in standards:
            coroutine = self._for_standard(self._async_get_variables_metadata, standard)
            coroutines.append(self._skip_on_failure(branch, f"variables metadata of {standard.get('href')}", coroutine))
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
                raise ValueError(f"No rule files found in {self.custom_rules_directory}")
        elif self.custom_rule_path:
            for path in self.custom_rule_path:
                if os.path.isfile(path) and path.endswith((".json", ".yml", ".yaml")):
                    rule_files.append(path)
                else:
                    print(f"Warning: {path} is not a valid file. Skipping.")
        else:
            raise ValueError("Invalid directory or path specified")
        custom_rules_file = os.path.join(self.cache_path, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value)
        existing_rules = {}

        if os.path.exists(custom_rules_file):
            with open(custom_rules_file, "rb") as f:
                existing_rules = pickle.load(f)
        skipped, added = self.parse_and_save_custom_rules(rule_files, existing_rules, custom_rules_file)
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
                    print(f"Rule {core_id} already exists. Use update_custom_rule to update it.")
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
        custom_rules_file = os.path.join(self.cache_path, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value)
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
        custom_rules_file = os.path.join(self.cache_path, DefaultFilePaths.CUSTOM_RULES_CACHE_FILE.value)
        if not os.path.exists(custom_rules_file):
            raise ValueError("No custom rules cache found. Nothing to remove.")

        with open(custom_rules_file, "rb") as f:
            existing_rules = pickle.load(f)
        removed_rules = []
        if self.remove_custom_rules.upper() == "ALL":
            removed_rules = list(existing_rules.keys())
            existing_rules = {}
        else:
            rule_ids_to_remove = [rule_id.strip() for rule_id in self.remove_custom_rules.split(",")]
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
        custom_standards_file = os.path.join(self.cache_path, "custom_rules_dictionary.pkl")
        custom_rules_dictionary = {}

        if os.path.exists(custom_standards_file):
            with open(custom_standards_file, "rb") as f:
                custom_rules_dictionary = pickle.load(f)
            print(f"Loaded existing custom standards dictionary with {len(custom_rules_dictionary)} entries")

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
            print(f"Updated {updated_count} existing standards and added {new_count} new standards")
        elif updated_count > 0:
            print(f"Updated {updated_count} existing standards")
        else:
            print(f"Added {new_count} new standards")

    def remove_custom_standards_from_cache(self):
        """Remove custom standards from the cache."""
        if not self.remove_custom_standards:
            return

        # Load the existing dictionary
        custom_standards_file = os.path.join(self.cache_path, "custom_rules_dictionary.pkl")
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

        print(f"Removed {removed_count} standards. Remaining standards: {len(custom_rules_dictionary)}")
