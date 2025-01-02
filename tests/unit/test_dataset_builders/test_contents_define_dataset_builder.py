import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.dataset_builders.contents_define_dataset_builder import (
    ContentsDefineDatasetBuilder,
)
from cdisc_rules_engine.services.data_services import DummyDataService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


define_metadata = [
    {
        "define_dataset_name": "TS",
        "define_dataset_label": "Trial Summary",
        "define_dataset_location": "ts.xpt",
        "define_dataset_class": "TRIAL DESIGN",
        "define_dataset_structure": "One record per trial summary parameter value",
        "define_dataset_is_non_standard": "",
    },
    {
        "define_dataset_name": "DM",
        "define_dataset_label": "Demographics",
        "define_dataset_location": "dm.xpt",
        "define_dataset_class": "SPECIAL PURPOSE",
        "define_dataset_structure": "One record per subject",
        "define_dataset_is_non_standard": "",
    },
    {
        "define_dataset_name": "AE",
        "define_dataset_label": "Adverse Events",
        "define_dataset_location": "ae.xpt",
        "define_dataset_class": "EVENTS",
        "define_dataset_structure": "One record per adverse event",
        "define_dataset_is_non_standard": "",
    },
]

data_metadata = {
    "dataset_size": [10, 0, 1000],
    "dataset_location": ["ts.xpt", "dm.xpt", "ae.xpt"],
    "dataset_name": ["TS", "DM", "AE"],
    "dataset_label": ["Trial Summary", "Demographics", "Adverse Events"],
}

expected_results = {
    "ts.xpt": {
        "dataset_size": [10, 10, 10],
        "dataset_location": ["ts.xpt", "ts.xpt", "ts.xpt"],
        "dataset_name": ["TS", "TS", "TS"],
        "dataset_label": ["Trial Summary", "Trial Summary", "Trial Summary"],
        "define_dataset_name": ["TS", "TS", "TS"],
        "define_dataset_label": ["Trial Summary", "Trial Summary", "Trial Summary"],
        "define_dataset_location": ["ts.xpt", "ts.xpt", "ts.xpt"],
        "define_dataset_class": ["TRIAL DESIGN", "TRIAL DESIGN", "TRIAL DESIGN"],
        "define_dataset_structure": [
            "One record per trial summary parameter value",
            "One record per trial summary parameter value",
            "One record per trial summary parameter value",
        ],
        "define_dataset_is_non_standard": ["", "", ""],
    },
    "dm.xpt": {
        "dataset_size": [0, 0, 0],
        "dataset_location": ["dm.xpt", "dm.xpt", "dm.xpt"],
        "dataset_name": ["DM", "DM", "DM"],
        "dataset_label": ["Demographics", "Demographics", "Demographics"],
        "define_dataset_name": ["DM", "DM", "DM"],
        "define_dataset_label": ["Demographics", "Demographics", "Demographics"],
        "define_dataset_location": ["dm.xpt", "dm.xpt", "dm.xpt"],
        "define_dataset_class": [
            "SPECIAL PURPOSE",
            "SPECIAL PURPOSE",
            "SPECIAL PURPOSE",
        ],
        "define_dataset_structure": [
            "One record per subject",
            "One record per subject",
            "One record per subject",
        ],
        "define_dataset_is_non_standard": ["", "", ""],
    },
    "ae.xpt": {
        "dataset_size": [1000, 1000, 1000],
        "dataset_location": ["ae.xpt", "ae.xpt", "ae.xpt"],
        "dataset_name": ["AE", "AE", "AE"],
        "dataset_label": ["Adverse Events", "Adverse Events", "Adverse Events"],
        "define_dataset_name": ["AE", "AE", "AE"],
        "define_dataset_label": ["Adverse Events", "Adverse Events", "Adverse Events"],
        "define_dataset_location": ["ae.xpt", "ae.xpt", "ae.xpt"],
        "define_dataset_class": ["EVENTS", "EVENTS", "EVENTS"],
        "define_dataset_structure": [
            "One record per adverse event",
            "One record per adverse event",
            "One record per adverse event",
        ],
        "define_dataset_is_non_standard": ["", "", ""],
    },
}


@pytest.mark.parametrize(
    "dataset_path, expected",
    [
        ("ts.xpt", expected_results["ts.xpt"]),
        ("dm.xpt", expected_results["dm.xpt"]),
        ("ae.xpt", expected_results["ae.xpt"]),
    ],
)
def test_contents_define_dataset_builder(dataset_path, expected):
    define_df = pd.DataFrame(define_metadata)
    dataset_df = pd.DataFrame(data_metadata)

    builder = ContentsDefineDatasetBuilder(
        rule=None,
        data_service=DummyDataService(MagicMock(), MagicMock(), MagicMock(), data=[]),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path=dataset_path,
        datasets=[data_metadata],
        domain=None,
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    )

    with patch.object(
        builder, "_get_define_xml_dataframe", return_value=PandasDataset(define_df)
    ), patch.object(
        builder, "_get_dataset_dataframe", return_value=PandasDataset(dataset_df)
    ):

        result = builder.build()

    # Ensure columns are in the expected order
    expected_df = pd.DataFrame(expected)
    result_df = result.data[expected_df.columns]

    # Ensure columns are in the expected order
    expected_df = pd.DataFrame(expected)
    result_df = result.data[expected_df.columns]

    # Check that columns are the same
    assert list(result_df.columns) == list(expected_df.columns), "Columns do not match"

    # Check that the values in each column match
    pd.testing.assert_frame_equal(result_df, expected_df)
