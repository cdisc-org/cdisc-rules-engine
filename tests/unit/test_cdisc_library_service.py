"""
This module contains unit tests for CDISCLibraryService class.
The tests do not send real requests to CDISC Library, the requests are mocked.
"""

import json
import os
import pytest
from unittest.mock import MagicMock, patch

from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService

def test_library_service_requires_key_for_default_url():
    """Test that API key is required when using default CDISC Library URL."""
    with pytest.raises(ValueError, match="CDISC_LIBRARY_API_KEY is required"):
        CDISCLibraryService("", "", MagicMock())

def test_library_service_allows_custom_url_without_key():
    """Test that custom URL works without API key."""
    # Should not raise an error
    library_service = CDISCLibraryService("", "https://custom-library.example.com/api", MagicMock())
    assert library_service is not None

def test_library_service_with_key_and_default_url():
    """Test normal case with API key and default URL."""
    library_service = CDISCLibraryService("test-key", "", MagicMock())
    assert library_service is not None

@patch(
    "cdisc_rules_engine.services.cdisc_library_service.CDISCLibraryClient.get_sdtmig"
)
def test_get_standard_details(mock_get_sdtmig: MagicMock):
    """
    Unit test for get_standard_details method.
    """
    with open(
        f"{os.path.dirname(__file__)}/../resources/mock_library_responses/get_sdtmig_response.json"
    ) as file:
        mock_sdtmig_details: dict = json.loads(file.read())
    mock_get_sdtmig.return_value = mock_sdtmig_details

    library_service = CDISCLibraryService("DUMMY_API_KEY", "", MagicMock())
    standard_details: dict = library_service.get_standard_details("sdtmig", "3-1-2")
    # expected is that mocked sdtmig details is extended with "domains" key
    assert standard_details == {
        "domains": {
            "CO",
            "DM",
            "SE",
            "SV",
        },
        **mock_sdtmig_details,
    }
