import pytest

from .helpers import assert_series_equals, create_sql_operators
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult


@pytest.mark.parametrize(
    "data,target,version,value_is_literal,external_dictionary_type,expected_result",
    [
        (
            {"target": ["2025-11-02", "2026-03-01"]},
            "target",
            "28.1",
            True,
            "meddra",
            [True, False],
        ),
        (
            {"target": ["2025-11-02", "2026-03-01"]},
            "target",
            "$version",
            False,
            "meddra",
            [True, False],
        ),
    ],
)
def test_sql_latest_available_exdict(
    data, target, version, value_is_literal, external_dictionary_type, expected_result
):
    sql_ops = create_sql_operators(
        data,
        extra_operation_variables={
            "$version": SqlOperationResult(query="SELECT '28.1' AS value", type="constant", subtype="Char")
        },
    )
    result = sql_ops.is_latest_available_external_dictionary_version(
        {
            "target": target,
            "version": version,
            "value_is_literal": value_is_literal,
            "external_dictionary_type": external_dictionary_type,
        }
    )
    assert_series_equals(result, expected_result)
