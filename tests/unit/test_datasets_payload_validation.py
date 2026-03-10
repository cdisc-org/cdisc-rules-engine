"""
Unit tests for datasets payload validation and API error handling.
Covers TestRule Azure function: validate_datasets_payload and handle_exception.
"""

import json
import importlib
import sys
from unittest.mock import MagicMock

import pytest
from cdisc_rules_engine.exceptions.custom_exceptions import (
    CTPackageNotFoundError,
    LibraryMetadataNotFoundError,
)
from scripts.script_utils import library_metadata_not_found_message


class _MockHttpResponse:
    def __init__(self, body, status_code=200):
        self.status_code = status_code
        self._body = body if isinstance(body, bytes) else body.encode("utf-8")

    def get_body(self):
        return self._body


_mock_func = MagicMock()
_mock_func.HttpResponse = _MockHttpResponse
_mock_azure = MagicMock()
_mock_azure.functions = _mock_func
sys.modules["azure"] = _mock_azure
sys.modules["azure.functions"] = _mock_func


def _get_testrule_module():
    return importlib.import_module("TestRule")


class TestValidateDatasetsPayload:
    """Test validate_datasets_payload raises clear, actionable errors."""

    def test_missing_required_properties_raises_bad_request_with_datasets_guidance(
        self,
    ):
        testrule = _get_testrule_module()
        datasets = [
            {
                "filename": "dm.xpt",
                "domain": "DM",
                "records": {"USUBJID": ["1"]},
                "variables": [{"name": "USUBJID"}],
            }
        ]
        with pytest.raises(testrule.BadRequestError) as exc_info:
            testrule.validate_datasets_payload(datasets)
        msg = str(exc_info.value)
        assert "Test data is incorrect and missing required formatting" in msg

    def test_missing_multiple_required_properties_raises_with_datasets_guidance(self):
        testrule = _get_testrule_module()
        datasets = [
            {
                "filename": "dm.xpt",
            }
        ]
        with pytest.raises(testrule.BadRequestError) as exc_info:
            testrule.validate_datasets_payload(datasets)
        msg = str(exc_info.value)
        assert "Test data is incorrect and missing required formatting" in msg

    def test_valid_payload_passes(self):
        testrule = _get_testrule_module()
        datasets = [
            {
                "filename": "dm.xpt",
                "label": "Demographics",
                "domain": "DM",
                "records": {"USUBJID": ["1"]},
                "variables": [{"name": "USUBJID"}],
            }
        ]
        testrule.validate_datasets_payload(datasets)

    def test_missing_variable_metadata_raises_bad_request(self):
        testrule = _get_testrule_module()
        datasets = [
            {
                "filename": "dm.xpt",
                "label": "Demographics",
                "domain": "DM",
                "records": {"USUBJID": ["1"]},
                "variables": [None],
            }
        ]
        with pytest.raises(testrule.BadRequestError) as exc_info:
            testrule.validate_datasets_payload(datasets)
        assert "variable metadata" in str(exc_info.value)


class TestHandleException:
    """Test that handle_exception returns user-friendly JSON for clients."""

    def test_bad_request_error_returns_400_with_message(self):
        testrule = _get_testrule_module()
        e = testrule.BadRequestError(
            "Test data is incorrect and missing required formatting."
        )
        response = testrule.handle_exception(e)
        assert response.status_code == 400
        body = json.loads(response.get_body().decode())
        assert body["error"] == "BadRequestError"
        assert "message" in body
        assert (
            "Test data is incorrect and missing required formatting" in body["message"]
        )

    def test_key_error_for_rule_returns_400_with_bad_request_error_type(self):
        testrule = _get_testrule_module()
        e = KeyError("'rule' required in request")
        response = testrule.handle_exception(e)
        assert response.status_code == 400
        body = json.loads(response.get_body().decode())
        assert body["error"] == "BadRequestError"
        assert (
            "rule" in body["message"].lower() or "required" in body["message"].lower()
        )

    def test_key_error_for_datasets_returns_400_with_bad_request_error_type(self):
        testrule = _get_testrule_module()
        e = KeyError("'datasets' required in request")
        response = testrule.handle_exception(e)
        assert response.status_code == 400
        body = json.loads(response.get_body().decode())
        assert body["error"] == "BadRequestError"

    def test_library_metadata_not_found_error_returns_400_with_message(self):
        testrule = _get_testrule_module()
        e = LibraryMetadataNotFoundError(
            library_metadata_not_found_message("sdtmig", "3-4")
        )
        response = testrule.handle_exception(e)
        assert response.status_code == 400
        body = json.loads(response.get_body().decode())
        assert body["error"] == "LibraryMetadataNotFoundError"
        assert "sdtmig" in body["message"]
        assert "3.4" in body["message"] or "version" in body["message"]

    def test_ct_package_not_found_error_returns_400_with_message(self):
        testrule = _get_testrule_module()
        e = CTPackageNotFoundError(
            "Controlled terminology package(s) not found: bad-ct-pkg."
        )
        response = testrule.handle_exception(e)
        assert response.status_code == 400
        body = json.loads(response.get_body().decode())
        assert body["error"] == "CTPackageNotFoundError"
        assert "not found" in body["message"]
        assert "bad-ct-pkg" in body["message"]

    def test_other_exception_returns_400_unknown_exception(self):
        testrule = _get_testrule_module()
        e = ValueError("Something else went wrong")
        response = testrule.handle_exception(e)
        assert response.status_code == 400
        body = json.loads(response.get_body().decode())
        assert body["error"] == "Unknown Exception"
        assert "Something else went wrong" in body["message"]
