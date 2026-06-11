from unittest.mock import MagicMock

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.map import Map
from pytest import mark


@mark.parametrize(
    "map, expected",
    [
        (
            [
                {"parent_entity": "Timing", "parent_rel": "type", "output": "C201264"},
                {
                    "parent_entity": "Timing",
                    "parent_rel": "relativeToFrom",
                    "output": "C201265",
                },
            ],
            ["C201264", "C201265"],
        ),
        (
            [
                {
                    "output": "C201264",
                },
            ],
            ["C201264", "C201264"],
        ),
    ],
)
def test_map(operation_params, map, expected):
    operation_params.map = map
    evaluation_dataset = PandasDataset.from_dict(
        {
            "parent_entity": ["Timing", "Timing"],
            "parent_rel": ["type", "relativeToFrom"],
        }
    )

    operation = Map(
        operation_params,
        evaluation_dataset,
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )

    result = operation._execute_operation()
    assert result.tolist() == expected
