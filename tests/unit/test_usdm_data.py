import json
import os
import unittest
from typing import List
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from pytest import mark

from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.rules_engine import RulesEngine
from cdisc_rules_engine.services.data_services import USDMDataService
from core import list_dataset_metadata

dataset_file = "USDM_EliLilly_NCT03421379_Diabetes.json"
dataset_path = f"{os.path.dirname(__file__)}/../resources/{dataset_file}"


class TestListDatasetMetadata(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_list_dataset_metadata_with_valid_paths(self):
        # Use absolute path based on test file location for portability
        test_dataset_path = os.path.join(
            os.path.dirname(__file__), "..", "resources", dataset_file
        )
        result = self.runner.invoke(
            list_dataset_metadata,
            ["-dp", test_dataset_path],
        )

        self.assertEqual(result.exit_code, 0)

        # Parse and validate JSON output
        output_data = json.loads(result.output)
        self.assertIsInstance(output_data, list)
        self.assertGreater(len(output_data), 0)

        # Find metadata for our test file
        test_filename = os.path.basename(test_dataset_path)
        metadata = next(
            (m for m in output_data if m.get("filename") == test_filename),
            None
        )
        self.assertIsNotNone(metadata, f"Metadata for {test_filename} not found")

        # Verify required attributes
        self.assertEqual(metadata["filename"], test_filename)
        self.assertIsNone(metadata["domain"])  # USDM datasets have domain=None
        self.assertIn("full_path", metadata)
        self.assertIn("file_size", metadata)
        self.assertIn("label", metadata)
        self.assertIn("modification_date", metadata)


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
