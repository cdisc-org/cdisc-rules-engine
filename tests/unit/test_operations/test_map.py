from unittest.mock import MagicMock

import pandas as pd
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.map import Map


def test_map(operation_params):
    operation_params.map = [
        {"parent_entity": "Timing", "parent_rel": "type", "output": "C201264"},
        {
            "parent_entity": "Timing",
            "parent_rel": "relativeToFrom",
            "output": "C201265",
        },
    ]
    evaluation_dataset = PandasDataset.from_dict(
        {
            "parent_entity": ["Timing", "Timing"],
            "parent_rel": ["type", "relativeToFrom"],
        }
    )
    expected = pd.Series(["C201264", "C201265"])

    operation = Map(
        operation_params,
        evaluation_dataset,
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )

    result = operation._execute_operation()
    assert result.equals(expected)
