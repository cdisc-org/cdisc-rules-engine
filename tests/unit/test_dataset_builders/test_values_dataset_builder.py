import pytest
from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)


@pytest.mark.parametrize(
    "variable_data_type, variable_value, expected_value",
    [
        ("integer", "0012", 2),
        ("integer", "-12", 3),
        ("integer", 12, 2),
        ("float", 12.2, 3),
        ("float", "0012.33", 4),
        ("float", -12.33, 5),
        ("text", "0012.33", 7),
        ("datetime", "2023-06-07T00:00:00", None),
        ("date", "2023-06-07", None),
        ("time", "00:00:00", None),
        ("partialDate", "2023-06", None),
        ("partialDatetime", "2023-06-07T12:00", None),
        ("partialTime", "12:20", None),
        ("incompleteDatetime", "2023-06T12:00:24", None),
        ("durationDatetime", "P3Y6M4DT12H30M5S", None),
        ("intervalDatetime", "2023-06/2023-07", None),
    ],
)
def test_calculate_variable_value_length(
    variable_value, variable_data_type: str, expected_value: int
):
    result = ValuesDatasetBuilder.calculate_variable_value_length(
        variable_value, variable_data_type
    )
    assert result == expected_value
