import pytest
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_type, data, expected_result",
    [
        (
            PandasDataset,
            {
                "variable_name": ["COVAL", "COVAL1", "COVAL2", "OTHERVAR"],
                "variable_label": ["Comments", "Comment1", "Comment 2", "Other"],
            },
            [False, False, False, False],
        ),
        (
            DaskDataset,
            {
                "variable_name": ["COVAL", "COVAL1", "COVAL2"],
                "variable_label": ["Comments", "Wrong Label", "Comment2"],
            },
            [False, True, False],
        ),
    ],
)
def test_inconsistent_enumerated_column_labels(dataset_type, data, expected_result):
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).inconsistent_enumerated_column_labels(
        {"target": "COVAL", "comparator": "Comment"}
    )
    assert result.tolist() == expected_result
