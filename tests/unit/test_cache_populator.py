"""
This module contains unit tests for CDISCLibraryService class.
The tests do not send real requests to CDISC Library, the requests are mocked.
"""

import json
import os
from unittest.mock import MagicMock, patch

from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.cache.cache_populator_service import CachePopulator
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
import pytest

@pytest.mark.asyncio
@patch(
    "cdisc_rules_engine.services.cache.cache_populator_service.pickle.dump"
)
async def test_save_rules_locally_no_rules_found(mock_dump):
    library_service = MagicMock(CDISCLibraryService)
    library_service.get_all_rule_catalogs.return_value = []
    cache_populator = CachePopulator(InMemoryCacheService(), library_service)
    await cache_populator.save_rules_locally()
    mock_dump.assert_not_called()

@pytest.mark.asyncio
@patch(
    "cdisc_rules_engine.services.cache.cache_populator_service.pickle.dump"
)
async def test_save_ct_packages_locally_no_packages_found(mock_dump):
    library_service = MagicMock(CDISCLibraryService)
    library_service.get_all_ct_packages.return_value = []
    cache_populator = CachePopulator(InMemoryCacheService(), library_service)
    await cache_populator.save_ct_packages_locally()
    mock_dump.assert_not_called()

@pytest.mark.asyncio
@patch(
    "cdisc_rules_engine.services.cache.cache_populator_service.pickle.dump"
)
async def test_save_standards_metadata_locally_none_found(mock_dump):
    library_service = MagicMock(CDISCLibraryService)
    library_service.get_all_tabulation_ig_standards.return_value = []
    library_service.get_all_collection_ig_standards.return_value = []
    library_service.get_all_analysis_ig_standards.return_value = []
    library_service.get_tig_standards.return_value = []
    cache_populator = CachePopulator(InMemoryCacheService(), library_service)
    await cache_populator.save_standards_metadata_locally()
    mock_dump.assert_not_called()
