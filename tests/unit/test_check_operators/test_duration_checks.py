import pytest
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "data,dataset_type,expected_result",
    [
        (
            {"target": ["P1Y", "P1M", "P1D", "PT1H"]},
            PandasDataset,
            [False, False, False, False],
        ),
        (
            {"target": ["P1Y2M3D", "P1DT2H3M", "PT1H30M", "P1W"]},
            DaskDataset,
            [False, False, False, False],
        ),
        (
            {"target": ["P", "1Y", "PT", "P1S"]},
            PandasDataset,
            [True, True, True, True],
        ),
        (
            {"target": ["P1Y1M1DT1H1M1.5S", "PT1.5S", "P1DT1H1M1.123S", "P1YT"]},
            DaskDataset,
            [False, False, False, True],
        ),
        (
            {"target": ["P-1Y", "P1M2D3H", "P1D1H", "PT24H"]},
            PandasDataset,
            [True, True, True, False],
        ),
    ],
)
def test_invalid_duration(data, dataset_type, expected_result):
    df = dataset_type.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.invalid_duration({"target": "target"})
    assert result.equals(df.convert_to_series(expected_result))


def test_invalid_duration_edge_cases():
    data = {
        "target": [
            "P1Y2M3W4DT5H6M7.89S",
            "PT0.1S",
            "P0D",
            "P1Y2M3DT",
            "P1.5Y",
            "P1M2.5D",
        ]
    }
    df = PandasDataset.from_dict(data)
    dataframe_type = DataframeType({"value": df})
    result = dataframe_type.invalid_duration({"target": "target"})
    expected = [False, False, False, True, True, True]
    assert result.equals(df.convert_to_series(expected))
