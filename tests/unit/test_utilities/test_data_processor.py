from typing import List
import pandas as pd
import pytest
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.models.dataset import PandasDataset
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
