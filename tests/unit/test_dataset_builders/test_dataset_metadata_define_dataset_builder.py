import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.dataset_builders.dataset_metadata_define_dataset_builder import (
    DatasetMetadataDefineDatasetBuilder,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
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
    "dataset_domain": ["TS", "DM", "AE"],
    "dataset_columns": [
        ["STUDYID", "DOMAIN"],
        ["STUDYID", "USUBJID"],
        ["STUDYID", "AETERM"],
    ],
    "is_ap": [False, False, False],
    "ap_suffix": ["", "", ""],
}

expected_results = {
    "ts.xpt": {
        "dataset_size": 10,
        "dataset_location": "ts.xpt",
        "dataset_name": "TS",
        "dataset_label": "Trial Summary",
        "dataset_domain": "TS",
        "dataset_columns": ["STUDYID", "DOMAIN"],
        "is_ap": False,
        "ap_suffix": "",
        "define_dataset_name": "TS",
        "define_dataset_label": "Trial Summary",
        "define_dataset_location": "ts.xpt",
        "define_dataset_class": "TRIAL DESIGN",
        "define_dataset_structure": "One record per trial summary parameter value",
        "define_dataset_is_non_standard": "",
    },
    "dm.xpt": {
        "dataset_size": 0,
        "dataset_location": "dm.xpt",
        "dataset_name": "DM",
        "dataset_label": "Demographics",
        "dataset_domain": "DM",
        "dataset_columns": ["STUDYID", "USUBJID"],
        "is_ap": False,
        "ap_suffix": "",
        "define_dataset_name": "DM",
        "define_dataset_label": "Demographics",
        "define_dataset_location": "dm.xpt",
        "define_dataset_class": "SPECIAL PURPOSE",
        "define_dataset_structure": "One record per subject",
        "define_dataset_is_non_standard": "",
    },
    "ae.xpt": {
        "dataset_size": 1000,
        "dataset_location": "ae.xpt",
        "dataset_name": "AE",
        "dataset_label": "Adverse Events",
        "dataset_domain": "AE",
        "dataset_columns": ["STUDYID", "AETERM"],
        "is_ap": False,
        "ap_suffix": "",
        "define_dataset_name": "AE",
        "define_dataset_label": "Adverse Events",
        "define_dataset_location": "ae.xpt",
        "define_dataset_class": "EVENTS",
        "define_dataset_structure": "One record per adverse event",
        "define_dataset_is_non_standard": "",
    },
}


@pytest.mark.parametrize(
    "dataset_path",
    [
        "ts.xpt",
        "dm.xpt",
        "ae.xpt",
    ],
)
def test_dataset_metadata_define_dataset_builder(dataset_path):
    define_df = pd.DataFrame(define_metadata)
    dataset_df = pd.DataFrame(data_metadata)

    builder = DatasetMetadataDefineDatasetBuilder(
        rule=None,
        data_service=DummyDataService(MagicMock(), MagicMock(), MagicMock(), data=[]),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path=dataset_path,
        datasets=[data_metadata],
        dataset_metadata=SDTMDatasetMetadata(full_path=dataset_path),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    )

    with (
        patch.object(
            builder, "_get_define_xml_dataframe", return_value=PandasDataset(define_df)
        ),
        patch.object(
            builder, "_get_dataset_dataframe", return_value=PandasDataset(dataset_df)
        ),
    ):

        result = builder.build()

    expected_df = pd.DataFrame(
        [
            expected_results["ts.xpt"],
            expected_results["dm.xpt"],
            expected_results["ae.xpt"],
        ]
    ).astype(object)

    result_df = result.data[expected_df.columns].reset_index(drop=True)

    # Check that columns are the same
    assert list(result_df.columns) == list(expected_df.columns), "Columns do not match"

    # Check that the values in each column match
    pd.testing.assert_frame_equal(result_df, expected_df)
