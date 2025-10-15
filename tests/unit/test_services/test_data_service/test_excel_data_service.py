import os
import tempfile
from unittest.mock import MagicMock

import pytest
import pandas as pd
from openpyxl import Workbook

from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.services.data_services import ExcelDataService
from cdisc_rules_engine.models.dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_name",
    ("ecaa.xpt", "ecbb.xpt", "suppec.xpt"),
)
def test_get_dataset(dataset_name):
    dataset_path = f"{os.path.dirname(__file__)}/../../../resources/test_datasets.xlsx"
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = ExcelDataService.get_instance(
        config=ConfigService(),
        cache_service=mock_cache,
        dataset_implementation=PandasDataset,
        dataset_path=dataset_path,
    )
    data = data_service.get_dataset(dataset_name=dataset_name)
    assert isinstance(data, PandasDataset)


@pytest.mark.parametrize(
    "expected_result",
    (
        {
            "dataset_location": "ecaa.xpt",
            "dataset_label": "Exposure as Collected AA",
            "dataset_name": "ECAA",
            "dataset_size": 0,
            "record_count": 8,
        },
        {
            "dataset_location": "ecbb.xpt",
            "dataset_label": "Exposure as Collected BB",
            "dataset_name": "ECBB",
            "dataset_size": 0,
            "record_count": 3,
        },
        {
            "dataset_location": "suppec.xpt",
            "dataset_label": "Supplemental Qualifiers for EC",
            "dataset_name": "SUPPEC",
            "dataset_size": 0,
            "record_count": 11,
        },
    ),
)
def test_get_dataset_metadata(expected_result):
    dataset_path = f"{os.path.dirname(__file__)}/../../../resources/test_datasets.xlsx"
    cache_mock = MagicMock()
    cache_mock.get_dataset.return_value = None
    data_service = ExcelDataService(
        cache_mock, MagicMock(), MagicMock(), dataset_path=dataset_path
    )
    metadata = data_service.get_dataset_metadata(
        dataset_name=expected_result["dataset_location"]
    )
    assert metadata["dataset_label"][0] == expected_result["dataset_label"]
    assert metadata["dataset_name"][0] == expected_result["dataset_name"]
    assert metadata["dataset_size"][0] == expected_result["dataset_size"]
    assert metadata["dataset_location"][0] == expected_result["dataset_location"]
    assert metadata["record_count"][0] == expected_result["record_count"]


@pytest.mark.parametrize(
    "dataset_name",
    ("ecaa.xpt", "ecbb.xpt", "suppec.xpt"),
)
def test_get_variables_metdata(dataset_name):
    dataset_path = f"{os.path.dirname(__file__)}/../../../resources/test_datasets.xlsx"
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = ExcelDataService.get_instance(
        config=ConfigService(),
        cache_service=mock_cache,
        dataset_implementation=PandasDataset,
        dataset_path=dataset_path,
    )
    data = data_service.get_variables_metadata(dataset_name=dataset_name, datasets=[])
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


def test_na_value_preserved_not_converted_to_nan():
    """
    Test that 'NA' string values are preserved and not converted to NaN/None.
    This tests the fix for issue #1211 where 'NA' (a valid controlled terminology term)
    was being incorrectly treated as a missing value.
    """
    # Create a temporary Excel file with 'NA' as a data value
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_file:
        temp_path = tmp_file.name

    try:
        # Create workbook with test data
        wb = Workbook()

        # Create Datasets sheet (required by ExcelDataService)
        datasets_sheet = wb.active
        datasets_sheet.title = "Datasets"
        datasets_sheet.append(["Filename", "Label", "Dataset Name"])
        datasets_sheet.append(["test.xpt", "Test Dataset", "TEST"])

        # Create test dataset sheet with 'NA' values
        test_sheet = wb.create_sheet("test.xpt")
        # Row 1: Variable names
        test_sheet.append(["STUDYID", "DOMAIN", "USUBJID", "NYTEST"])
        # Row 2: Variable labels
        test_sheet.append(["Study ID", "Domain", "Unique Subject ID", "NY Test"])
        # Row 3: Variable types
        test_sheet.append(["Char", "Char", "Char", "Char"])
        # Row 4: Variable lengths
        test_sheet.append(["10", "2", "20", "2"])
        # Row 5+: Data rows - including 'NA' which should be preserved
        test_sheet.append(["STUDY001", "DM", "SUBJECT001", "NA"])
        test_sheet.append(["STUDY001", "DM", "SUBJECT002", "NY"])
        test_sheet.append(["STUDY001", "DM", "SUBJECT003", "NA"])

        wb.save(temp_path)
        wb.close()

        # Test the ExcelDataService
        mock_cache = MagicMock()
        mock_cache.get_dataset.return_value = None

        data_service = ExcelDataService.get_instance(
            config=ConfigService(),
            cache_service=mock_cache,
            dataset_implementation=PandasDataset,
            dataset_path=temp_path,
        )

        # Get the dataset
        dataset = data_service.get_dataset(dataset_name="test.xpt")

        # Assertions
        assert isinstance(dataset, PandasDataset)
        assert "NYTEST" in dataset.data.columns

        # The critical assertion: 'NA' should be preserved as the string 'NA'
        # not converted to None or pd.NA
        nytest_values = dataset.data["NYTEST"].tolist()
        assert nytest_values == ["NA", "NY", "NA"], (
            f"Expected ['NA', 'NY', 'NA'] but got {nytest_values}. "
            "'NA' should be preserved as a string, not converted to NaN/None"
        )

        # Additional check: verify no NaN/None values where 'NA' should be
        assert dataset.data["NYTEST"].iloc[0] == "NA"
        assert dataset.data["NYTEST"].iloc[2] == "NA"
        assert pd.notna(dataset.data["NYTEST"].iloc[0])  # Should NOT be NaN
        assert pd.notna(dataset.data["NYTEST"].iloc[2])  # Should NOT be NaN

    finally:
        # Cleanup temporary file
        os.unlink(temp_path)
