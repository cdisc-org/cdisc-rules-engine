import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.dataset_builders.domain_list_with_define_builder import (
    DomainListWithDefineDatasetBuilder,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.data_services import DummyDataService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)


define_metadata = [
    {
        "define_dataset_name": "AE",
        "define_dataset_label": "Adverse Events",
        "define_dataset_location": "ae.xpt",
        "define_dataset_domain": "AE",
        "define_dataset_class": "EVENTS",
        "define_dataset_structure": "One record per adverse event",
        "define_dataset_is_non_standard": "",
        "define_dataset_has_no_data": False,
        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "AESEQ"],
        "define_dataset_variables": ["STUDYID", "USUBJID", "AETERM", "AESEQ"],
    },
    {
        "define_dataset_name": "DM",
        "define_dataset_label": "Demographics",
        "define_dataset_location": "dm.xpt",
        "define_dataset_domain": "DM",
        "define_dataset_class": "SPECIAL PURPOSE",
        "define_dataset_structure": "One record per subject",
        "define_dataset_is_non_standard": "",
        "define_dataset_has_no_data": False,
        "define_dataset_key_sequence": ["STUDYID", "USUBJID"],
        "define_dataset_variables": ["STUDYID", "USUBJID", "AGE", "SEX"],
    },
    {
        "define_dataset_name": "SE",
        "define_dataset_label": "Subject Elements",
        "define_dataset_location": "se.xpt",
        "define_dataset_domain": "SE",
        "define_dataset_class": "EVENTS",
        "define_dataset_structure": "One record per subject element",
        "define_dataset_is_non_standard": "",
        "define_dataset_has_no_data": True,
        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "SESEQ"],
        "define_dataset_variables": ["STUDYID", "USUBJID", "SESEQ"],
    },
    {
        "define_dataset_name": "EC",
        "define_dataset_label": "Exposure as Collected",
        "define_dataset_location": "ec.xpt",
        "define_dataset_domain": "EC",
        "define_dataset_class": "INTERVENTIONS",
        "define_dataset_structure": "One record per exposure",
        "define_dataset_is_non_standard": "",
        "define_dataset_has_no_data": False,
        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "ECSEQ"],
        "define_dataset_variables": ["STUDYID", "USUBJID", "ECTRT", "ECSEQ"],
    },
]


@pytest.mark.parametrize(
    "mock_datasets,define_meta,expected_results,test_description",
    [
        (
            [
                MagicMock(unsplit_name="AE", filename="ae.xpt"),
                MagicMock(unsplit_name="DM", filename="dm.xpt"),
                MagicMock(unsplit_name="SE", filename="se.xpt"),
                MagicMock(unsplit_name="EC", filename="ec.xpt"),
            ],
            define_metadata,
            pd.DataFrame(
                [
                    {
                        "domain": "AE",
                        "filename": "ae.xpt",
                        "define_dataset_name": "AE",
                        "define_dataset_label": "Adverse Events",
                        "define_dataset_location": "ae.xpt",
                        "define_dataset_domain": "AE",
                        "define_dataset_class": "EVENTS",
                        "define_dataset_structure": "One record per adverse event",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "AESEQ"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "AETERM",
                            "AESEQ",
                        ],
                    },
                    {
                        "domain": "DM",
                        "filename": "dm.xpt",
                        "define_dataset_name": "DM",
                        "define_dataset_label": "Demographics",
                        "define_dataset_location": "dm.xpt",
                        "define_dataset_domain": "DM",
                        "define_dataset_class": "SPECIAL PURPOSE",
                        "define_dataset_structure": "One record per subject",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "AGE",
                            "SEX",
                        ],
                    },
                    {
                        "domain": "SE",
                        "filename": "se.xpt",
                        "define_dataset_name": "SE",
                        "define_dataset_label": "Subject Elements",
                        "define_dataset_location": "se.xpt",
                        "define_dataset_domain": "SE",
                        "define_dataset_class": "EVENTS",
                        "define_dataset_structure": "One record per subject element",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": True,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "SESEQ"],
                        "define_dataset_variables": ["STUDYID", "USUBJID", "SESEQ"],
                    },
                    {
                        "domain": "EC",
                        "filename": "ec.xpt",
                        "define_dataset_name": "EC",
                        "define_dataset_label": "Exposure as Collected",
                        "define_dataset_location": "ec.xpt",
                        "define_dataset_domain": "EC",
                        "define_dataset_class": "INTERVENTIONS",
                        "define_dataset_structure": "One record per exposure",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "ECSEQ"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "ECTRT",
                            "ECSEQ",
                        ],
                    },
                ]
            ).astype(object),
            "all_datasets_exist",
        ),
        (
            [
                MagicMock(unsplit_name="AE", filename="ae.xpt"),
                MagicMock(unsplit_name="DM", filename="dm.xpt"),
                MagicMock(unsplit_name="EC", filename="ec.xpt"),
            ],
            define_metadata,
            pd.DataFrame(
                [
                    {
                        "domain": "AE",
                        "filename": "ae.xpt",
                        "define_dataset_name": "AE",
                        "define_dataset_label": "Adverse Events",
                        "define_dataset_location": "ae.xpt",
                        "define_dataset_domain": "AE",
                        "define_dataset_class": "EVENTS",
                        "define_dataset_structure": "One record per adverse event",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "AESEQ"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "AETERM",
                            "AESEQ",
                        ],
                    },
                    {
                        "domain": "DM",
                        "filename": "dm.xpt",
                        "define_dataset_name": "DM",
                        "define_dataset_label": "Demographics",
                        "define_dataset_location": "dm.xpt",
                        "define_dataset_domain": "DM",
                        "define_dataset_class": "SPECIAL PURPOSE",
                        "define_dataset_structure": "One record per subject",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "AGE",
                            "SEX",
                        ],
                    },
                    {
                        "domain": "SE",
                        "filename": None,
                        "define_dataset_name": "SE",
                        "define_dataset_label": "Subject Elements",
                        "define_dataset_location": "se.xpt",
                        "define_dataset_domain": "SE",
                        "define_dataset_class": "EVENTS",
                        "define_dataset_structure": "One record per subject element",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": True,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "SESEQ"],
                        "define_dataset_variables": ["STUDYID", "USUBJID", "SESEQ"],
                    },
                    {
                        "domain": "EC",
                        "filename": "ec.xpt",
                        "define_dataset_name": "EC",
                        "define_dataset_label": "Exposure as Collected",
                        "define_dataset_location": "ec.xpt",
                        "define_dataset_domain": "EC",
                        "define_dataset_class": "INTERVENTIONS",
                        "define_dataset_structure": "One record per exposure",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "ECSEQ"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "ECTRT",
                            "ECSEQ",
                        ],
                    },
                ]
            ).astype(object),
            "some_datasets_missing",
        ),
        (
            [],
            define_metadata,
            pd.DataFrame(
                [
                    {
                        "domain": "AE",
                        "filename": None,
                        "define_dataset_name": "AE",
                        "define_dataset_label": "Adverse Events",
                        "define_dataset_location": "ae.xpt",
                        "define_dataset_domain": "AE",
                        "define_dataset_class": "EVENTS",
                        "define_dataset_structure": "One record per adverse event",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "AESEQ"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "AETERM",
                            "AESEQ",
                        ],
                    },
                    {
                        "domain": "DM",
                        "filename": None,
                        "define_dataset_name": "DM",
                        "define_dataset_label": "Demographics",
                        "define_dataset_location": "dm.xpt",
                        "define_dataset_domain": "DM",
                        "define_dataset_class": "SPECIAL PURPOSE",
                        "define_dataset_structure": "One record per subject",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "AGE",
                            "SEX",
                        ],
                    },
                    {
                        "domain": "SE",
                        "filename": None,
                        "define_dataset_name": "SE",
                        "define_dataset_label": "Subject Elements",
                        "define_dataset_location": "se.xpt",
                        "define_dataset_domain": "SE",
                        "define_dataset_class": "EVENTS",
                        "define_dataset_structure": "One record per subject element",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": True,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "SESEQ"],
                        "define_dataset_variables": ["STUDYID", "USUBJID", "SESEQ"],
                    },
                    {
                        "domain": "EC",
                        "filename": None,
                        "define_dataset_name": "EC",
                        "define_dataset_label": "Exposure as Collected",
                        "define_dataset_location": "ec.xpt",
                        "define_dataset_domain": "EC",
                        "define_dataset_class": "INTERVENTIONS",
                        "define_dataset_structure": "One record per exposure",
                        "define_dataset_is_non_standard": "",
                        "define_dataset_has_no_data": False,
                        "define_dataset_key_sequence": ["STUDYID", "USUBJID", "ECSEQ"],
                        "define_dataset_variables": [
                            "STUDYID",
                            "USUBJID",
                            "ECTRT",
                            "ECSEQ",
                        ],
                    },
                ]
            ).astype(object),
            "no_datasets_exist",
        ),
    ],
)
def test_domain_list_with_define_dataset_builder(
    mock_datasets, define_meta, expected_results, test_description
):
    builder = DomainListWithDefineDatasetBuilder(
        rule=None,
        data_service=DummyDataService(MagicMock(), MagicMock(), MagicMock(), data=[]),
        cache_service=None,
        rule_processor=None,
        data_processor=None,
        dataset_path="ae.xpt",
        datasets=mock_datasets,
        dataset_metadata=SDTMDatasetMetadata(full_path="ae.xpt"),
        define_xml_path=None,
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    )

    with patch.object(builder, "get_define_metadata", return_value=define_meta):
        result = builder.build()

    result_df = result.data.reset_index(drop=True)

    if expected_results.empty:
        assert result_df.empty, f"Expected empty DataFrame for {test_description}"
    else:
        assert list(result_df.columns) == list(
            expected_results.columns
        ), f"Columns do not match for {test_description}"
        pd.testing.assert_frame_equal(result_df, expected_results, check_dtype=False)
