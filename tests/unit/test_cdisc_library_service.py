"""
This module contains unit tests for CDISCLibraryService class.
The tests do not send real requests to CDISC Library, the requests are mocked.
"""

import json
import os
from unittest.mock import MagicMock, patch

from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService


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

    library_service = CDISCLibraryService(config, MagicMock())
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
