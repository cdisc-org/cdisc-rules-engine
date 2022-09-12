import asyncio
import pickle
from functools import partial
from typing import Iterable, List, Optional

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


class CachePopulator:
    def __init__(
        self, cache: CacheServiceInterface, library_service: CDISCLibraryService
    ):
        self.cache = cache
        self.library_service = library_service

    async def load_cache_data(self):
        """
        This function populates a cache implementation with
        all data necessary for running rules against local data.
        Including
        * rules
        * library metadata
        * codelist metadata
        """
        # send request to get all rules
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

        # save standard codelists to cache as a map of variable to allowed_values
        standards = self.library_service.get_all_tabulation_ig_standards()
        standards.extend(self.library_service.get_all_collection_ig_standards())
        standards.extend(self.library_service.get_all_analysis_ig_standards())

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

        return self.cache

    def save_rules_locally(self, cache_path: str):
        """
        Store cached rules in rules.pkl in cache path directory
        """
        rules_data = self.cache.filter_cache("rules")
        with open(f"{cache_path}/rules.pkl", "wb") as f:
            pickle.dump(rules_data, f)

    def save_ct_packages_locally(self, cache_path: str):
        """
        Store cached ct pacakage metadata in
        codelist_term_maps.pkl in cache path directory
        """
        cts = self.cache.get_by_regex(".*ct.*")
        with open(f"{cache_path}/codelist_term_maps.pkl", "wb") as f:
            pickle.dump(cts, f)

    def save_variable_codelist_maps_locally(self, cache_path: str):
        """
        Store cached variable codelist metadata in
        variable_codelist_maps.pkl in cache path directory
        """
        variable_codelist_maps = self.cache.get_by_regex(".*codelists.*")
        with open(f"{cache_path}/variable_codelist_maps.pkl", "wb") as f:
            pickle.dump(variable_codelist_maps, f)

    def save_standards_metadata_locally(self, cache_path: str):
        """
        Store cached standards metadata in standards_details.pkl in cache path directory
        """
        standards = self.cache.filter_cache("standards")
        with open(f"{cache_path}/standards_details.pkl", "wb") as f:
            pickle.dump(standards, f)

    def save_standards_models_locally(self, cache_path: str):
        """
        Store cached standards models metadata in
        standards_models.pkl in cache path directory
        """
        standards_models = self.cache.filter_cache("models")
        with open(f"{cache_path}/standards_models.pkl", "wb") as f:
            pickle.dump(standards_models, f)

    def save_variables_metadata_locally(self, cache_path: str):
        """
        Store cached variables metadata in
        variables_metadata.pkl in cache path directory
        """
        variables_metadata = self.cache.filter_cache("library_variables_metadata")
        with open(f"{cache_path}/variables_metadata.pkl", "wb") as f:
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
        For each CT package in CDISC library,
        generate a map of codelist to codelist term. Ex:
        {
            "package": "sdtmct-2021-12-17"
            "C123": {
                "extensible": True,
                "allowed_terms": ["TEST", "HOUR"]
            }
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
        coroutines = [
            self._async_get_variable_codelist_map(
                standard.get("href", "").split("/")[-2],
                standard.get("href", "").split("/")[-1],
            )
            for standard in standards
        ]
        variable_codelist_maps = await asyncio.gather(*coroutines)
        return variable_codelist_maps

    async def _async_get_variable_codelist_map(
        self, standard_type: str, standard_version: str
    ) -> dict:
        loop = asyncio.get_event_loop()
        variables_map: dict = await loop.run_in_executor(
            None,
            self.library_service.get_variable_codelists_map,
            standard_type,
            standard_version,
        )
        return variables_map

    async def _async_get_details_of_all_standards(
        self, standards: List[dict]
    ) -> List[dict]:
        """
        Gets details for each given standard.
        """
        coroutines = [
            self._async_get_standard_details(
                standard.get("href", "").split("/")[-2],
                standard.get("href", "").split("/")[-1],
            )
            for standard in standards
        ]
        return await asyncio.gather(*coroutines)

    async def _async_get_standard_details(
        self, standard_type: str, standard_version: str
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
        )
        standard_details["cache_key"] = get_standard_details_cache_key(
            standard_type, standard_version
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
        coroutines = [
            self._async_get_variables_metadata(
                standard.get("href", "").split("/")[-2],
                standard.get("href", "").split("/")[-1],
            )
            for standard in standards
        ]
        metadata = await asyncio.gather(*coroutines)
        return filter(lambda item: item is not None, metadata)

    async def _async_get_variables_metadata(
        self, standard_type: str, standard_version: str
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
                ),
            )
        except LibraryResourceNotFoundException:
            return None
        return {
            "cache_key": get_library_variables_metadata_cache_key(
                standard_type, standard_version
            ),
            **variables_metadata,
        }
