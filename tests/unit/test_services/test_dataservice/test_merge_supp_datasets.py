from unittest.mock import MagicMock, patch
import pytest
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)
import pandas as pd
import pandas.testing as pdt


@pytest.fixture
def data_service():
    return LocalDataService(MagicMock(), MagicMock(), MagicMock())


def test_process_supp(data_service):
    # Create a supplementary dataset with 'QNAM' and 'QVAL' for processing.
    supp_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "QNAM": ["X", "Y", "Z"],
                "QVAL": [10, 20, 30],
                "QLABEL": ["Label1", "Label2", "Label3"],
            }
        )
    )
    processed_dataset = data_service.process_supp(supp_dataset)
    assert all(
        column in processed_dataset.data.columns for column in ["X", "Y", "Z"]
    ), "New "
    "columns based on QNAM should be added."
    pd.testing.assert_series_equal(
        processed_dataset.data["X"],
        pd.Series([10, pd.NA, pd.NA], name="X", dtype="object"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        processed_dataset.data["Y"],
        pd.Series([pd.NA, 20, pd.NA], name="Y", dtype="object"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        processed_dataset.data["Z"],
        pd.Series([pd.NA, pd.NA, 30], name="Z", dtype="object"),
        check_names=False,
    )
    assert "QNAM" not in processed_dataset.data.columns, "'QNAM' should be dropped."
    assert "QVAL" not in processed_dataset.data.columns, "'QVAL' should be dropped."
    assert "QLABEL" not in processed_dataset.data.columns, "'QVAL' should be dropped."


@patch.object(LocalDataService, "check_filepath", return_value=False)
@patch.object(LocalDataService, "_async_get_datasets")
def test_merge_supp_dataset(mock_async_get_datasets, mock_check_filepath, data_service):
    # Setup example datasets
    parent_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "A": ["1", "2", "3"],
            }
        )
    )

    supp_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "IDVAR": ["A", "A", "A"],
                "IDVARVAL": ["1", "2", "3"],
                "QNAM": ["X", "Y", "Z"],
                "QVAL": [10, 20, 30],
                "QLABEL": ["Label1", "Label2", "Label3"],
            }
        )
    )

    mock_async_get_datasets.return_value = [parent_dataset, supp_dataset]

    # Function to simulate dataset fetching based on names
    def dummy_func(dataset_name, **kwargs):
        if dataset_name == "parent_dataset":
            return parent_dataset
        elif dataset_name == "supp_dataset":
            return supp_dataset

    merged_dataset = data_service.merge_supp_dataset(
        func_to_call=dummy_func, dataset_names=["parent_dataset", "supp_dataset"]
    )
    expected_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "A": ["1", "2", "3"],
                "X": [10, pd.NA, pd.NA],
                "Y": [pd.NA, 20, pd.NA],
                "Z": [pd.NA, pd.NA, 30],
            }
        )
    )
    pdt.assert_frame_equal(
        merged_dataset.data, expected_dataset.data, check_dtype=False
    )
    assert len(merged_dataset.data) == len(parent_dataset.data), "The"
    " length of the merged dataset should match the parent dataset."
