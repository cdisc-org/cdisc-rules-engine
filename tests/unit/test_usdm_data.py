from typing import List
from unittest.mock import patch, MagicMock
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
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
        "domain": null,
        "filename": "USDM_EliLilly_NCT03421379_Diabetes.json","""
        self.assertEqual(result.exit_code, 0)
        self.assertIn(expected_output, result.output)


def test_get_datasets():
    USDMDataService._instance = None
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    datasets = data_service.get_datasets()
    assert len(datasets) == 33


@mark.parametrize(
    "domain_name, record_count",
    [("Activity", 225), ("string", 1309), ("Study", 1)],
)
def test_get_dataset(domain_name, record_count):
    USDMDataService._instance = None
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    dataset_name = os.path.join(dataset_path, "{}.json".format(domain_name))
    data = data_service.get_dataset(dataset_name=dataset_name)
    assert isinstance(data, PandasDataset)
    assert len(data) == record_count


def test_get_raw_dataset_metadata():
    USDMDataService._instance = None
    cache = MagicMock()
    cache.get_dataset.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=cache, dataset_path=dataset_path
    )
    data = data_service.get_raw_dataset_metadata(
        dataset_name=os.path.join(dataset_path, "Code.json")
    )
    assert data.record_count == 117


def test_validate_rule_single_dataset_check(dataset_rule_greater_than: dict):
    """
    The test checks the rules validation for a single dataset.
    In this case the rules does not have "datasets" key
    and datasets map is also empty.
    """
    dataset_mock = PandasDataset.from_dict(
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
            standard="usdm", dataset_paths=[dataset_path]
        ).validate_single_dataset(
            dataset_rule_greater_than,
            [],
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="USDM_EliLilly_NCT03421379_Diabetes.json",
                full_path=dataset_path,
            ),
        )

        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "USDM_EliLilly_NCT03421379_Diabetes.json",
                "domain": "EC",
                "variables": ["ECCOOLVAR"],
                "message": "Value for ECCOOLVAR greater than 30.",
                "errors": [
                    {
                        "value": {"ECCOOLVAR": 100},
                        "dataset": "USDM_EliLilly_NCT03421379_Diabetes.json",
                        "row": 2,
                    },
                    {
                        "value": {"ECCOOLVAR": 34},
                        "dataset": "USDM_EliLilly_NCT03421379_Diabetes.json",
                        "row": 4,
                    },
                ],
            }
        ]


def test_get_variables_metdata():
    USDMDataService._instance = None
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = USDMDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, dataset_path=dataset_path
    )
    data = data_service.get_variables_metadata(
        dataset_name=os.path.join(dataset_path, "StudyIdentifier.json")
    )
    assert isinstance(data, PandasDataset)
    expected_keys = [
        "variable_name",
        "variable_format",
        "variable_order_number",
        "variable_data_type",
        "variable_label",
    ]
    for key in expected_keys:
        assert key in data
