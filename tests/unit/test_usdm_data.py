from typing import List
from unittest.mock import patch, MagicMock
import pandas as pd
from cdisc_rules_engine.rules_engine import RulesEngine
import unittest
from click.testing import CliRunner
from core import list_dataset_metadata
import os
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.services.data_services import USDMDataService
from pytest import mark

dataset_file = "USDM_EliLilly_NCT03421379_Diabetes.json"
dataset_path = f"{os.path.dirname(__file__)}/../resources/{dataset_file}"


class TestListDatasetMetadata(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_dataset_metadata_with_valid_paths(self):
        result = self.runner.invoke(
            list_dataset_metadata,
            [
                "-dp",
                os.path.join("tests", "resources", dataset_file),
            ],
        )
        expected_output = """[
    {
        "domain": "",
        "filename": "USDM_EliLilly_NCT03421379_Diabetes.json","""
        self.assertEqual(result.exit_code, 0)
        self.assertIn(expected_output, result.output)


def test_get_datasets():
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    datasets = data_service.get_datasets()
    assert len(datasets) == 35


@mark.parametrize(
    "dataset_name, record_count",
    [("Activity", 321), ("string", 1309), ("Study", 1)],
)
def test_get_dataset(dataset_name, record_count):
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    data = data_service.get_dataset(dataset_name=dataset_name)
    assert isinstance(data, pd.DataFrame)
    assert data.shape[0] == record_count


def test_get_raw_dataset_metadata():
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    data = data_service.get_raw_dataset_metadata(dataset_name="Code")
    assert data.records == "93"


def test_validate_rule_single_dataset_check(dataset_rule_greater_than: dict):
    """
    The test checks the rules validation for a single dataset.
    In this case the rules does not have "datasets" key
    and datasets map is also empty.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "ECCOOLVAR": [20, 100, 10, 34],
            "AESTDY": [1, 2, 40, 50],
        }
    )
    with patch(
        "cdisc_rules_engine.services.data_services.USDMDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="usdm"
        ).validate_single_rule(dataset_rule_greater_than, dataset_path, [{}], "EC")
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "variables": ["ECCOOLVAR"],
                "message": "Value for ECCOOLVAR greater than 30.",
                "errors": [
                    {"value": {"ECCOOLVAR": 100}, "row": 2},
                    {"value": {"ECCOOLVAR": 34}, "row": 4},
                ],
            }
        ]


def test_get_variables_metdata():
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    data = data_service.get_variables_metadata(dataset_name="StudyIdentifier")
    assert isinstance(data, pd.DataFrame)
    expected_keys = [
        "variable_name",
        "variable_format",
        "variable_order_number",
        "variable_data_type",
        "variable_label",
    ]
    for key in expected_keys:
        assert key in data
