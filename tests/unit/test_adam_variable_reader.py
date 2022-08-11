import pytest

from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader


@pytest.mark.parametrize("columns", [["SITEGR7", "AFG", "AG3EGR", "AOCC02FL"]])
def test_extract_columns(columns):
    ad = AdamVariableReader()
    ad.extract_columns(columns)
    assert ad.alphanum_cols == ["SITEGR7", "AG3EGR", "AOCC02FL"]


@pytest.mark.parametrize("column_name", ["SITEGR7"])
def test_check_y_rule(column_name):
    ad = AdamVariableReader()
    ad.check_y(column_name)
    assert ad.categorization_scheme[column_name] == 7


@pytest.mark.parametrize("column_name", ["PH3ST"])
def test_check_w_rule(column_name):
    ad = AdamVariableReader()
    ad.check_w(column_name)
    assert ad.w_indexes["PH3ST"] == 3


@pytest.mark.parametrize("column_name", ["TRT02P"])
def test_check_xx_zz_rule(column_name):
    ad = AdamVariableReader()
    ad.check_xx_zz(column_name)
    assert ad.period["TRT02P"] == 2


@pytest.mark.parametrize("column_name", ["ANL23FL"])
def test_check_xx_zz_rule(column_name):
    ad = AdamVariableReader()
    ad.check_xx_zz(column_name)
    assert ad.selection_algorithm["ANL23FL"] == 23
