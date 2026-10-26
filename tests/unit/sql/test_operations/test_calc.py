import pytest

from test_check_operators.helpers import CompiledOperators

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    ColumnNotFoundError,
    RuleExecutionError,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.calc import SqlCalcOperation
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

TEST_TABLE_NAME = "test_table"


def _build_calc(column_data, formula, previous_operations=None):
    data_service = PostgresQLDataService.instance()
    standards_context = DefaultStandardsContext()
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=TEST_TABLE_NAME,
        column_data=column_data,
        standards_context=standards_context,
    )
    params = SqlOperationParams(
        domain=TEST_TABLE_NAME,
        target="FAKEVARIABLE",
        standards_context=standards_context,
        value=formula,
        previous_operations=previous_operations or {},
    )
    result = SqlCalcOperation(params, data_service).execute()
    return data_service, result


def _eval_calc(data_service, calc_result):
    query = calc_result.query
    for placeholder, column_name in (calc_result.params or {}).items():
        column_hash = data_service.pgi.schema.get_column_hash(TEST_TABLE_NAME, column_name)
        query = query.replace(placeholder, f"co.{column_hash}")
    table_hash = data_service.pgi.schema.get_table_hash(TEST_TABLE_NAME)
    full_query = f"SELECT id, ({query}) AS v FROM {table_hash} AS co ORDER BY id ASC"
    data_service.pgi.execute_sql(full_query)
    rows = data_service.pgi.fetch_all()
    return [row["v"] for row in rows]


def _make_operators(data_service, calc_result):
    config = {
        "dataset_id": TEST_TABLE_NAME,
        "data_service": data_service,
        "operation_variables": {"$calc": calc_result},
        "dataset_metadata": data_service.get_dataset_metadata(TEST_TABLE_NAME),
    }
    return CompiledOperators(config)


def _approx(values):
    return [pytest.approx(v) if v is not None else None for v in values]


def test_calc_result_structure():
    column_data = {"AVAL": [10.0], "BASE": [5.0]}
    _, result = _build_calc(column_data, "((AVAL - BASE) / BASE) * 100")

    assert isinstance(result, SqlOperationResult)
    assert result.type == "constant"
    assert result.subtype == "Num"
    assert result.query.startswith("SELECT ")
    assert result.query.endswith(" AS value")
    assert "NULLIF" in result.query
    assert sorted(result.params.values()) == ["aval", "base"]


@pytest.mark.parametrize(
    "formula,expected",
    [
        ("AVAL / BASE", [2.0, 4.0, None]),
        ("AVAL - BASE", [5.0, 15.0, 8.0]),
        ("BASE - AVAL", [-5.0, -15.0, -8.0]),
        ("((AVAL - BASE) / BASE) * 100", [100.0, 300.0, None]),
        ("((BASE - AVAL) / AVAL) * 100", [-50.0, -75.0, -100.0]),
    ],
)
def test_calc_adam_formulas(formula, expected):
    column_data = {
        "AVAL": [10.0, 20.0, 8.0],
        "BASE": [5.0, 5.0, 0.0],
    }
    data_service, result = _build_calc(column_data, formula)
    assert _eval_calc(data_service, result) == _approx(expected)


def test_calc_operator_precedence_and_parentheses():
    column_data = {"AVAL": [1.0]}
    data_service, result = _build_calc(column_data, "2 + 3 * 4")
    assert _eval_calc(data_service, result) == _approx([14.0])

    data_service, result = _build_calc(column_data, "(2 + 3) * 4")
    assert _eval_calc(data_service, result) == _approx([20.0])


def test_calc_unary_minus_and_decimals():
    column_data = {"AVAL": [10.0]}
    data_service, result = _build_calc(column_data, "-AVAL + 2.5")
    assert _eval_calc(data_service, result) == _approx([-7.5])


def test_calc_division_by_zero_and_null_are_skipped():
    column_data = {
        "AVAL": [10.0, 10.0, None],
        "BASE": [0.0, 5.0, 5.0],
    }
    data_service, result = _build_calc(column_data, "AVAL / BASE")
    assert _eval_calc(data_service, result) == _approx([None, 2.0, None])


def test_calc_char_column_is_safely_cast():
    column_data = {"STRESC": ["10", "abc", "4"], "DIVISOR": [2.0, 2.0, 2.0]}
    data_service, result = _build_calc(column_data, "STRESC / DIVISOR")
    assert _eval_calc(data_service, result) == _approx([5.0, None, 2.0])


def test_calc_used_in_equal_to():
    column_data = {
        "AVAL": [10.0, 8.0, 5.0],
        "BASE": [5.0, 4.0, 5.0],
        "PCHG": [100.0, 100.0, 999.0],
    }
    data_service, result = _build_calc(column_data, "((AVAL - BASE) / BASE) * 100")
    operators = _make_operators(data_service, result)

    equal = operators.equal_to({"target": "$calc", "comparator": "PCHG"})
    assert list(equal) == [True, True, False]


def test_calc_used_in_not_equal_to():
    column_data = {
        "AVAL": [10.0, 8.0, 5.0],
        "BASE": [5.0, 4.0, 5.0],
        "PCHG": [100.0, 100.0, 999.0],
    }
    data_service, result = _build_calc(column_data, "((AVAL - BASE) / BASE) * 100")
    operators = _make_operators(data_service, result)

    not_equal = operators.not_equal_to({"target": "$calc", "comparator": "PCHG"})
    assert list(not_equal) == [False, False, True]


def test_calc_with_previous_constant_operation_reference():
    column_data = {"AVAL": [10.0, 20.0]}
    previous_operations = {
        "$offset": SqlOperationResult(query="SELECT 2.0", type="constant", subtype="Num"),
    }
    data_service, result = _build_calc(column_data, "AVAL - $offset", previous_operations)
    assert _eval_calc(data_service, result) == _approx([8.0, 18.0])


def test_calc_with_parameterized_previous_operation_reference():
    column_data = {"AVAL": [10.0, 20.0], "BASE": [3.0, 4.0]}
    previous_operations = {
        "$base_ref": SqlOperationResult(query="SELECT $1", type="constant", subtype="Num", params={"$1": "BASE"}),
    }
    data_service, result = _build_calc(column_data, "AVAL - $base_ref", previous_operations)
    assert "base" in [value.lower() for value in result.params.values()]
    assert _eval_calc(data_service, result) == _approx([7.0, 16.0])


def test_calc_empty_formula_raises():
    with pytest.raises(RuleExecutionError, match="non-empty"):
        _build_calc({"AVAL": [1.0]}, "   ")


def test_calc_unknown_column_raises():
    with pytest.raises(ColumnNotFoundError, match="not found in table"):
        _build_calc({"AVAL": [1.0]}, "AVAL + NOTACOLUMN")


def test_calc_unknown_operation_reference_raises():
    with pytest.raises(RuleExecutionError, match="not a known previous operation"):
        _build_calc({"AVAL": [1.0]}, "AVAL + $missing")


def test_calc_collection_operation_reference_raises():
    previous_operations = {
        "$list": SqlOperationResult(query="SELECT 1", type="collection", subtype="Char"),
    }
    with pytest.raises(RuleExecutionError, match="must be a constant"):
        _build_calc({"AVAL": [1.0]}, "AVAL + $list", previous_operations)


def test_calc_invalid_character_raises():
    with pytest.raises(RuleExecutionError, match="Invalid character"):
        _build_calc({"AVAL": [1.0]}, "AVAL % 2")


def test_calc_unresolved_placeholder_raises():
    with pytest.raises(RuleExecutionError, match="Invalid character"):
        _build_calc({"AVAL": [1.0]}, "AVAL / {{root}}")


def test_calc_unbalanced_parentheses_raises():
    with pytest.raises(RuleExecutionError, match="closing parenthesis"):
        _build_calc({"AVAL": [1.0]}, "(AVAL + 1")
