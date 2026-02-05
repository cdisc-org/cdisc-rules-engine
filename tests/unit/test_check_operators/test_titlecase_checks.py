import pytest
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["Subject ID", "Adverse Event", "Date of First Dose"]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["SUBJECT ID", "ADVERSE EVENT", "DATE OF FIRST DOSE"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["subject id", "adverse event", "date of first dose"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["Subject ID", "ADVERSE EVENT", "date of first dose"]},
            PandasDataset,
            [True, False, False],
        ),
        (
            {"target": ["Subject ID", "Adverse Event", "Date of First Dose"]},
            DaskDataset,
            [True, True, True],
        ),
        (
            {"target": ["SUBJECT ID", "ADVERSE EVENT"]},
            DaskDataset,
            [False, False],
        ),
    ],
)
def test_is_title_case_basic(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["Subject ID", "Study ID", "Patient ID"]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Subject Id", "Study id", "Patient Id"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["Unique Subject ID", "Primary Study ID"]},
            PandasDataset,
            [True, True],
        ),
        (
            {"target": ["Subject ID", "Study ID"]},
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_is_title_case_id_acronym(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "Date of First Dose",
                    "End of Study",
                    "Reason for No Treatment",
                ]
            },
            PandasDataset,
            [True, True, True],
        ),
        (
            {
                "target": [
                    "Date Of First Dose",
                    "End Of Study",
                    "Reason For No Treatment",
                ]
            },
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["Of the Study", "For No Reason"]},
            PandasDataset,
            [True, True],
        ),
        (
            {"target": ["Study Of The Year", "Reason For The Record"]},
            PandasDataset,
            [False, False],
        ),
        (
            {"target": ["Date of First Dose", "End of Study"]},
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_is_title_case_small_words(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["Epi/Pandemic", "Date/Time", "Start/End"]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["epi/pandemic", "date/time", "start/end"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["EPI/PANDEMIC", "DATE/TIME", "START/END"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["Subject ID/Number", "Study ID/Code"]},
            PandasDataset,
            [True, True],
        ),
        (
            {"target": ["Epi/Pandemic Pre-Specified"]},
            PandasDataset,
            [True],
        ),
        (
            {"target": ["epi/pandemic pre-specified"]},
            PandasDataset,
            [False],
        ),
        (
            {"target": ["EPI/PANDEMIC PRE-SPECIFIED"]},
            PandasDataset,
            [False],
        ),
        (
            {"target": ["Epi/Pandemic", "Date/Time"]},
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_is_title_case_slash_separated(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["Pre-Specified", "Post-Treatment", "Non-Serious"]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["pre-specified", "post-treatment", "non-serious"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["PRE-SPECIFIED", "POST-TREATMENT", "NON-SERIOUS"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["Pre-Specified Event", "Post-Treatment Assessment"]},
            PandasDataset,
            [True, True],
        ),
        (
            {"target": ["Pre-Specified", "Post-Treatment"]},
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_is_title_case_hyphenated(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["", "", ""]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": [None, None, None]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Subject ID", "", None, "ADVERSE EVENT"]},
            PandasDataset,
            [True, True, True, False],
        ),
        (
            {"target": ["", None]},
            DaskDataset,
            [True, True],
        ),
    ],
)
def test_is_title_case_null_empty(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {
                "target": [
                    "Subject ID",
                    "Adverse Event ID",
                    "Date of First Dose",
                    "Reason for No Treatment",
                    "Pre-Specified Event",
                    "Epi/Pandemic Study",
                ]
            },
            PandasDataset,
            [True, True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "subject id",
                    "ADVERSE EVENT ID",
                    "Date Of First Dose",
                    "reason for no treatment",
                    "PRE-SPECIFIED EVENT",
                    "epi/pandemic study",
                ]
            },
            PandasDataset,
            [False, False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "Subject ID",
                    "adverse event id",
                    "Date of First Dose",
                    "REASON FOR NO TREATMENT",
                ]
            },
            PandasDataset,
            [True, False, True, False],
        ),
        (
            {
                "target": [
                    "Subject ID",
                    "Date of First Dose",
                    "Pre-Specified Event",
                ]
            },
            DaskDataset,
            [True, True, True],
        ),
    ],
)
def test_is_title_case_complex(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["Subject ID", "Adverse Event", "Date of First Dose"]},
            PandasDataset,
            [False, False, False],
        ),
        (
            {"target": ["SUBJECT ID", "adverse event", "Date Of First Dose"]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Subject ID", "ADVERSE EVENT", "Date of First Dose"]},
            PandasDataset,
            [False, True, False],
        ),
        (
            {"target": ["Subject ID", "ADVERSE EVENT"]},
            DaskDataset,
            [False, True],
        ),
    ],
)
def test_is_not_title_case(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_not_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": [123, 456, 789]},
            PandasDataset,
            [True, True, True],
        ),
        (
            {"target": ["Subject ID", 123, None, "ADVERSE EVENT"]},
            PandasDataset,
            [True, True, True, False],
        ),
    ],
)
def test_is_title_case_numeric_values(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.is_title_case({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))
