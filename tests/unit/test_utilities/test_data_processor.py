from typing import List
from unittest.mock import patch
import os
import pandas as pd
import pytest
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.enums.join_types import JoinTypes
import numpy as np


@pytest.mark.parametrize(
    "data",
    [
        (
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "RDOMAIN": ["AE", "EC", "EC", "AE"],
                        "IDVAR": ["AESEQ", "ECSEQ", "ECSEQ", "AESEQ"],
                        "IDVARVAL": [1, 2, 1, 3],
                    }
                )
            )
        ),
        (PandasDataset(pd.DataFrame.from_dict({"RSUBJID": [1, 4, 6000]}))),
    ],
)
def test_preprocess_relationship_dataset(data):
    dataset_metadata = [
        SDTMDatasetMetadata(
            name=domain,
            first_record={"DOMAIN": domain},
            filename=f"{domain.lower()}.xpt",
        )
        for domain in ["AE", "EC", "SUPP", "DM"]
    ]
    ae = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": [4, 5, 6],
                "STUDYID": [101, 201, 300],
                "AESEQ": [1, 2, 3],
            }
        )
    )
    ec = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECSTDY": [500, 4],
                "STUDYID": [201, 101],
                "ECSEQ": [2, 1],
            }
        )
    )
    dm = PandasDataset(pd.DataFrame.from_dict({"USUBJID": [1, 2, 3, 4, 5, 6000]}))
    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae,
        os.path.join("path", "ec.xpt"): ec,
        os.path.join("path", "dm.xpt"): dm,
        os.path.join("path", "data.xpt"): data,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        data_processor = DataProcessor(cache=InMemoryCacheService())
        reference_data = data_processor.preprocess_relationship_dataset(
            "path", data, dataset_metadata
        )
        if "IDVAR" in data:
            idvars = data["IDVAR"]
            domains = data["RDOMAIN"]
            for i, idvar in enumerate(idvars):
                assert idvar in reference_data[domains[i]]
        elif "RSUBJID" in data:
            assert "RSUBJID" in reference_data["DM"]
            assert np.array_equal(reference_data["DM"]["RSUBJID"], dm["USUBJID"])


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


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_merge_datasets_on_relationship_columns(dataset_implementation):
    """
    Unit test for DataProcessor.merge_datasets_on_relationship_columns method.
    """
    # prepare data
    left_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                1,
                2,
                3,
            ],
        }
    )
    right_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                "1.0",
                "2",
                "3.0",
                "3.0",
            ],
        }
    )

    # call the tested function and check the results
    merged_df = DataProcessor.merge_datasets_on_relationship_columns(
        left_dataset=left_dataset,
        left_dataset_match_keys=[],
        right_dataset=right_dataset,
        right_dataset_match_keys=[],
        right_dataset_domain_name="SUPPAE",
        column_with_names="IDVAR",
        column_with_values="IDVARVAL",
    )
    merged_df.data = merged_df.data.sort_values("AESEQ")
    expected_df = dataset_implementation.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                1.0,
                2.0,
                3.0,
                3.0,
            ],
            "USUBJID.SUPPAE": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                1.0,
                2.0,
                3.0,
                3.0,
            ],
        }
    )
    assert merged_df.equals(expected_df)


@pytest.mark.parametrize("dataset_implementation", [PandasDataset])
def test_merge_datasets_on_string_relationship_columns(dataset_implementation):
    """
    Unit test for DataProcessor.merge_datasets_on_relationship_columns method.
    Test the case when the columns that describe the relation
    are of a string type.
    """
    # prepare data
    left_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
            ],
        }
    )
    right_dataset = dataset_implementation.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
                "CDISC_IC",
            ],
        }
    )

    # call the tested function and check the results
    merged_df = DataProcessor.merge_datasets_on_relationship_columns(
        left_dataset=left_dataset,
        left_dataset_match_keys=[],
        right_dataset=right_dataset,
        right_dataset_match_keys=[],
        right_dataset_domain_name="SUPPAE",
        column_with_names="IDVAR",
        column_with_values="IDVARVAL",
    )
    expected_df = dataset_implementation.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
                "CDISC_IC",
            ],
            "USUBJID.SUPPAE": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
                "CDISC_IC",
            ],
        }
    )
    assert merged_df.equals(expected_df)


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
