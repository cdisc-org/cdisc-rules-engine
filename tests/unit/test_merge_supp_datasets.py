from unittest.mock import MagicMock, patch
import pytest
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)
from cdisc_rules_engine.utilities.data_processor import DataProcessor
import pandas as pd
import pandas.testing as pdt


@pytest.fixture
def data_service():
    return LocalDataService(MagicMock(), MagicMock(), MagicMock())


def test_process_supp():
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
    processed_dataset = DataProcessor.process_supp(supp_dataset)
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
def test_merge_pivot_supp_dataset(
    mock_async_get_datasets, mock_check_filepath, data_service
):
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

    merged_dataset = DataProcessor.merge_pivot_supp_dataset(
        data_service.dataset_implementation, parent_dataset, supp_dataset
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


@patch.object(LocalDataService, "check_filepath", return_value=False)
@patch.object(LocalDataService, "_async_get_datasets")
def test_merge_supp_dataset_multi_idvar(mock_async_get_datasets, data_service):
    parent_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY1", "STUDY1", "STUDY1", "STUDY1"],
                "USUBJID": ["001", "001", "002", "002"],
                "ECSEQ": ["1", "2", "1", "2"],
                "ECENDY": ["5", "7", "5", "7"],
                "ECTRT": ["Treatment A", "Treatment B", "Treatment A", "Treatment B"],
            }
        )
    )

    supp_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY1", "STUDY1", "STUDY1", "STUDY1", "STUDY1", "STUDY1"],
                "USUBJID": ["001", "001", "001", "001", "002", "002"],
                "RDOMAIN": ["EC", "EC", "EC", "EC", "EC", "EC"],
                "IDVAR": ["ECSEQ", "ECSEQ", "ECENDY", "ECENDY", "ECSEQ", "ECENDY"],
                "IDVARVAL": ["1", "2", "7", "7", "1", "5"],
                "QNAM": ["ECLOC", "ECLOC", "ECSITE", "ECREGION", "ECLOC", "ECSITE"],
                "QVAL": [
                    "Left Arm",
                    "Right Arm",
                    "Site A",
                    "Region 1",
                    "Left Leg",
                    "Site B",
                ],
                "QLABEL": [
                    "Location",
                    "Location",
                    "Site",
                    "Region",
                    "Location",
                    "Site",
                ],
                "QORIG": ["CRF", "CRF", "CRF", "CRF", "CRF", "CRF"],
                "QEVAL": ["", "", "", "", "", ""],
            }
        )
    )

    mock_async_get_datasets.return_value = [parent_dataset, supp_dataset]
    merged_dataset = DataProcessor.merge_pivot_supp_dataset(
        data_service.dataset_implementation, parent_dataset, supp_dataset
    )
    assert "ECLOC" in merged_dataset.columns, "ECLOC column should be added from SUPP"
    assert "ECSITE" in merged_dataset.columns, "ECSITE column should be added from SUPP"
    assert (
        "ECREGION" in merged_dataset.columns
    ), "ECREGION column should be added from SUPP"

    row1 = merged_dataset.data[
        (merged_dataset.data["USUBJID"] == "001")
        & (merged_dataset.data["ECSEQ"] == "1")
    ].iloc[0]
    assert row1["ECLOC"] == "Left Arm", "ECLOC should match ECSEQ=1"
    assert pd.isna(row1["ECSITE"]), "ECSITE should be NaN for ECSEQ=1"

    row2 = merged_dataset.data[
        (merged_dataset.data["USUBJID"] == "001")
        & (merged_dataset.data["ECSEQ"] == "2")
    ].iloc[0]
    assert row2["ECLOC"] == "Right Arm", "ECLOC should match ECSEQ=2"
    assert row2["ECSITE"] == "Site A", "ECSITE should match ECENDY=7"
    assert row2["ECREGION"] == "Region 1", "ECREGION should match ECENDY=7"

    row3 = merged_dataset.data[
        (merged_dataset.data["USUBJID"] == "002")
        & (merged_dataset.data["ECSEQ"] == "1")
    ].iloc[0]
    assert row3["ECLOC"] == "Left Leg", "ECLOC should match ECSEQ=1"
    assert row3["ECSITE"] == "Site B", "ECSITE should match ECENDY=5"
    assert len(merged_dataset.data) == len(
        parent_dataset.data
    ), "Merged dataset should have same number of rows as parent"


@patch.object(LocalDataService, "check_filepath", return_value=False)
@patch.object(LocalDataService, "_async_get_datasets")
def test_merge_supp_dataset_multi_idvar_aggregation(
    mock_async_get_datasets, data_service
):
    parent_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY1", "STUDY1"],
                "USUBJID": ["001", "001"],
                "ECSEQ": ["1", "2"],
                "ECENDY": ["5", "7"],
                "ECTRT": ["Treatment A", "Treatment B"],
            }
        )
    )
    supp_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
                "USUBJID": ["001", "001", "001"],
                "RDOMAIN": ["EC", "EC", "EC"],
                "IDVAR": ["ECENDY", "ECENDY", "ECSEQ"],
                "IDVARVAL": ["7", "7", "1"],
                "QNAM": ["ECLOC", "ECSITE", "ECLOC"],
                "QVAL": ["Location A", "Site A", "Location B"],
                "QLABEL": ["Location", "Site", "Location"],
                "QORIG": ["CRF", "CRF", "CRF"],
                "QEVAL": ["", "", ""],
            }
        )
    )

    mock_async_get_datasets.return_value = [parent_dataset, supp_dataset]

    merged_dataset = DataProcessor.merge_pivot_supp_dataset(
        data_service.dataset_implementation, parent_dataset, supp_dataset
    )

    assert len(merged_dataset.data) == len(
        parent_dataset.data
    ), "Should not create duplicate rows when aggregating multiple QNAM for same IDVAR/IDVARVAL"

    # Both ECLOC and ECSITE from ECENDY=7 should be present in the ECSEQ=2 row
    row = merged_dataset.data[merged_dataset.data["ECSEQ"] == "2"].iloc[0]
    assert row["ECLOC"] == "Location A", "ECLOC from ECENDY=7 should be merged"
    assert row["ECSITE"] == "Site A", "ECSITE from ECENDY=7 should be merged"


@patch.object(LocalDataService, "check_filepath", return_value=False)
@patch.object(LocalDataService, "_async_get_datasets")
def test_merge_supp_dataset_multi_idvar_same_qnam_validation_error(
    mock_async_get_datasets, data_service
):
    parent_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY1"],
                "USUBJID": ["001"],
                "ECSEQ": ["1"],
                "ECENDY": ["5"],
                "ECTRT": ["Treatment A"],
            }
        )
    )

    supp_dataset = PandasDataset(
        pd.DataFrame(
            {
                "STUDYID": ["STUDY1", "STUDY1"],
                "USUBJID": ["001", "001"],
                "RDOMAIN": ["EC", "EC"],
                "IDVAR": ["ECSEQ", "ECSEQ"],
                "IDVARVAL": ["1", "1"],
                "QNAM": ["ECLOC", "ECLOC"],
                "QVAL": ["Location A", "Location B"],
                "QLABEL": ["Location", "Location"],
                "QORIG": ["CRF", "CRF"],
                "QEVAL": ["", ""],
            }
        )
    )

    mock_async_get_datasets.return_value = [parent_dataset, supp_dataset]

    with pytest.raises(ValueError, match="Multiple records with the same QNAM"):
        DataProcessor.merge_pivot_supp_dataset(
            data_service.dataset_implementation, parent_dataset, supp_dataset
        )
