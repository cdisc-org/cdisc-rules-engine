from typing import List
import pandas as pd
import pytest
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset
from cdisc_rules_engine.enums.join_types import JoinTypes


def test_filter_dataset_columns_by_metadata_and_rule():
    """
    Unit test for DataProcessor.filter_dataset_columns_by_metadata_and_rule function.
    """
    columns: List[str] = ["STUDYID", "DOMAIN", "AESEV", "AESER"]
    define_metadata: List[dict] = [
        {
            "define_variable_name": "AESEV",
            "define_variable_origin_type": "Collected",
        },
        {
            "define_variable_name": "AESER",
            "define_variable_origin_type": "Collected",
        },
    ]
    library_metadata: dict = {
        "STUDYID": {
            "core": "Exp",
        },
        "DOMAIN": {
            "core": "Exp",
        },
        "AESEV": {
            "core": "Perm",
        },
        "AESER": {
            "core": "Perm",
        },
        "AESEQ": {
            "core": "Exp",
        },
    }
    rule: dict = {
        "variable_origin_type": "Collected",
        "variable_core_status": "Perm",
    }
    filtered_columns: List[str] = (
        DataProcessor.filter_dataset_columns_by_metadata_and_rule(
            columns, define_metadata, library_metadata, rule
        )
    )
    assert filtered_columns == [
        "AESEV",
        "AESER",
    ]


@pytest.mark.parametrize(
    "join_type, expected_df",
    [
        (
            JoinTypes.INNER,
            PandasDataset(
                pd.DataFrame(
                    {
                        "USUBJID": [
                            "CDISC01",
                            "CDISC01",
                        ],
                        "DOMAIN": [
                            "BE",
                            "BE",
                        ],
                        "BESEQ": [
                            1,
                            3,
                        ],
                        "BEREFID": [
                            "SAMPLE_1",
                            "SAMPLE_3",
                        ],
                        "REFID": [
                            "SAMPLE_1",
                            "SAMPLE_3",
                        ],
                        "PARENT": [
                            "",
                            "SAMPLE_1",
                        ],
                        "LEVEL": [
                            1,
                            2,
                        ],
                    }
                )
            ),
        ),
        (
            JoinTypes.LEFT,
            PandasDataset(
                pd.DataFrame(
                    {
                        "USUBJID": [
                            "CDISC01",
                            "CDISC01",
                            "CDISC01",
                        ],
                        "DOMAIN": [
                            "BE",
                            "BE",
                            "BE",
                        ],
                        "BESEQ": [
                            1,
                            2,
                            3,
                        ],
                        "BEREFID": [
                            "SAMPLE_1",
                            "SAMPLE_2",
                            "SAMPLE_3",
                        ],
                        "REFID": [
                            "SAMPLE_1",
                            None,
                            "SAMPLE_3",
                        ],
                        "PARENT": [
                            "",
                            None,
                            "SAMPLE_1",
                        ],
                        "LEVEL": pd.Series(
                            [
                                1,
                                None,
                                2,
                            ],
                            dtype="object",
                        ),
                        "_merge_RELSPEC": pd.Categorical(
                            [
                                "both",
                                "left_only",
                                "both",
                            ],
                            categories=["left_only", "right_only", "both"],
                            ordered=False,
                        ),
                    }
                ),
            ),
        ),
    ],
)
def test_merge_datasets_on_join_type(join_type: JoinTypes, expected_df: PandasDataset):
    """
    Unit test for DataProcessor.merge_sdtm_datasets method.
    Test cases when either inner or left join type is specified.
    """
    # prepare data
    left_dataset: PandasDataset = PandasDataset.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "BE",
                "BE",
                "BE",
            ],
            "BESEQ": [
                1,
                2,
                3,
            ],
            "BEREFID": [
                "SAMPLE_1",
                "SAMPLE_2",
                "SAMPLE_3",
            ],
        }
    )
    right_dataset: PandasDataset = PandasDataset.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
            ],
            "REFID": [
                "SAMPLE_1",
                "SAMPLE_3",
            ],
            "PARENT": [
                "",
                "SAMPLE_1",
            ],
            "LEVEL": [
                1,
                2,
            ],
        }
    )

    # call the tested function and check the results
    merged_df: PandasDataset = DataProcessor.merge_sdtm_datasets(
        left_dataset=left_dataset,
        left_dataset_match_keys=["USUBJID", "BEREFID"],
        right_dataset=right_dataset,
        right_dataset_domain_name="RELSPEC",
        right_dataset_match_keys=["USUBJID", "REFID"],
        join_type=join_type,
    )
    assert merged_df.equals(expected_df)


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_merge_pivot_supp_dataset_single_idvar(dataset_implementation):
    left_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC01", "CDISC01"],
            "DOMAIN": ["AE", "AE", "AE"],
            "AESEQ": [1, 2, 3],
            "AETERM": ["Headache", "Nausea", "Fatigue"],
        }
    )
    right_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC01", "CDISC01"],
            "RDOMAIN": ["AE", "AE", "AE"],
            "IDVAR": ["AESEQ", "AESEQ", "AESEQ"],
            "IDVARVAL": ["1", "2", "3"],
            "QNAM": ["AESPID", "AESPID", "AESPID"],
            "QVAL": ["SP001", "SP002", "SP003"],
        }
    )

    merged_df = DataProcessor.merge_pivot_supp_dataset(
        dataset_implementation=dataset_implementation,
        left_dataset=left_dataset,
        right_dataset=right_dataset,
    )
    if isinstance(merged_df, DaskDataset):
        result_data = merged_df.data.compute()
    else:
        result_data = merged_df.data

    # Verify pivot
    assert "AESPID" in merged_df.columns, "AESPID column should be created from QNAM"
    assert "QNAM" not in merged_df.columns, "QNAM should be dropped after pivot"
    assert "QVAL" not in merged_df.columns, "QVAL should be dropped after pivot"
    assert result_data[result_data["AESEQ"] == "1"]["AESPID"].values[0] == "SP001"
    assert result_data[result_data["AESEQ"] == "2"]["AESPID"].values[0] == "SP002"
    assert result_data[result_data["AESEQ"] == "3"]["AESPID"].values[0] == "SP003"
    assert len(result_data) == 3


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_merge_pivot_supp_dataset_multiple_idvar(dataset_implementation):
    left_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC01", "CDISC01"],
            "DOMAIN": ["EC", "EC", "EC"],
            "ECSEQ": [1, 2, 3],
            "ECENDY": [5, 7, 10],
            "ECTRT": ["Treatment A", "Treatment B", "Treatment C"],
        }
    )
    right_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC01", "CDISC01", "CDISC01"],
            "RDOMAIN": ["EC", "EC", "EC", "EC"],
            "IDVAR": ["ECSEQ", "ECSEQ", "ECENDY", "ECENDY"],
            "IDVARVAL": ["1", "2", "7", "10"],
            "QNAM": ["ECLOC", "ECLOC", "ECSITE", "ECSITE"],
            "QVAL": ["Left Arm", "Right Arm", "Site A", "Site B"],
        }
    )

    merged_df = DataProcessor.merge_pivot_supp_dataset(
        dataset_implementation=dataset_implementation,
        left_dataset=left_dataset,
        right_dataset=right_dataset,
    )
    if isinstance(merged_df, DaskDataset):
        result_data = merged_df.data.compute()
    else:
        result_data = merged_df.data

    # Verify pivot
    assert "ECLOC" in merged_df.columns, "ECLOC column should be created from QNAM"
    assert "ECSITE" in merged_df.columns, "ECSITE column should be created from QNAM"
    assert "QNAM" not in merged_df.columns, "QNAM should be dropped after pivot"
    assert "QVAL" not in merged_df.columns, "QVAL should be dropped after pivot"

    row1 = result_data[result_data["ECSEQ"] == "1"].iloc[0]
    assert row1["ECLOC"] == "Left Arm"
    assert pd.isna(row1["ECSITE"])
    row2 = result_data[result_data["ECSEQ"] == "2"].iloc[0]
    assert row2["ECLOC"] == "Right Arm"
    assert row2["ECSITE"] == "Site A"
    row3 = result_data[result_data["ECSEQ"] == "3"].iloc[0]
    assert pd.isna(row3["ECLOC"])
    assert row3["ECSITE"] == "Site B"
    assert len(result_data) == 3


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_merge_pivot_supp_dataset_blank_idvar(dataset_implementation):
    left_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC02"],
            "DOMAIN": ["DM", "DM"],
            "AGE": [45, 52],
        }
    )
    right_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC02"],
            "RDOMAIN": ["DM", "DM"],
            "IDVAR": ["", ""],  # Blank IDVAR
            "IDVARVAL": ["", ""],
            "QNAM": ["DMPOPFLAG", "DMPOPFLAG"],
            "QVAL": ["Y", "N"],
        }
    )
    merged_df = DataProcessor.merge_pivot_supp_dataset(
        dataset_implementation=dataset_implementation,
        left_dataset=left_dataset,
        right_dataset=right_dataset,
    )
    if isinstance(merged_df, DaskDataset):
        result_data = merged_df.data.compute()
    else:
        result_data = merged_df.data

    # Verify pivot
    assert (
        "DMPOPFLAG" in merged_df.columns
    ), "DMPOPFLAG column should be created from QNAM"
    assert "QNAM" not in merged_df.columns, "QNAM should be dropped after pivot"
    assert "QVAL" not in merged_df.columns, "QVAL should be dropped after pivot"
    assert (
        result_data[result_data["USUBJID"] == "CDISC01"]["DMPOPFLAG"].values[0] == "Y"
    )
    assert (
        result_data[result_data["USUBJID"] == "CDISC02"]["DMPOPFLAG"].values[0] == "N"
    )
    assert len(result_data) == 2
