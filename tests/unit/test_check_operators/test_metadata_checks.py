from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType
import pytest
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_conformant_value_length(dataset_type):
    def filter_func(row):
        return row["IDVAR1"] == "TEST"

    def length_check(row):
        return len(row["IDVAR2"]) <= 4

    data = {
        "RDOMAIN": ["LB", "LB", "AE"],
        "IDVAR1": ["TEST", "TEST", "AETERM"],
        "IDVAR2": ["TEST", "TOOLONG", "AETERM"],
    }
    df = dataset_type.from_dict(data)

    vlm = [{"filter": filter_func, "length_check": length_check}]

    result = DataframeType(
        {"value": df, "value_level_metadata": vlm}
    ).conformant_value_length({})
    assert result.equals(df.convert_to_series([True, False, False]))


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_conformant_value_data_type(dataset_type):
    def filter_func(row):
        return row["IDVAR1"] == "TEST"

    def type_check(row):
        return isinstance(row["IDVAR2"], str)

    data = {
        "RDOMAIN": ["LB", "LB", "AE"],
        "IDVAR1": ["TEST", "TEST", "AETERM"],
        "IDVAR2": ["TEST", 1, "AETERM"],
    }

    vlm = [{"filter": filter_func, "type_check": type_check}]
    df = dataset_type.from_dict(data)
    result = DataframeType(
        {"value": df, "value_level_metadata": vlm}
    ).conformant_value_data_type({})
    assert result.equals(df.convert_to_series([True, False, False]))


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_variable_metadata_equal_to(dataset_type):
    data = {
        "STUDYID": [1, 1, 1, 1],
        "$CORE_VALUES": [
            {"STUDYID": "Req", "DOMAIN": "Req"},
            {"STUDYID": "Req", "DOMAIN": "Req"},
            {"STUDYID": "Req", "DOMAIN": "Req"},
            {"STUDYID": "Req", "DOMAIN": "Req"},
        ],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).variable_metadata_equal_to(
        {"target": "STUDYID", "comparator": "Exp", "metadata": "$CORE_VALUES"}
    )
    assert result.equals(df.convert_to_series([False, False, False, False]))
    result = DataframeType({"value": df}).variable_metadata_equal_to(
        {"target": "STUDYID", "comparator": "Req", "metadata": "$CORE_VALUES"}
    )
    assert result.equals(df.convert_to_series([True, True, True, True]))


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_variable_metadata_not_equal_to(dataset_type):
    data = {
        "STUDYID": [1, 1, 1, 1],
        "$CORE_VALUES": [
            {"STUDYID": "Req", "DOMAIN": "Req"},
            {"STUDYID": "Req", "DOMAIN": "Req"},
            {"STUDYID": "Req", "DOMAIN": "Req"},
            {"STUDYID": "Req", "DOMAIN": "Req"},
        ],
    }
    df = dataset_type.from_dict(data)
    result = DataframeType({"value": df}).variable_metadata_not_equal_to(
        {"target": "STUDYID", "comparator": "Exp", "metadata": "$CORE_VALUES"}
    )
    assert result.equals(df.convert_to_series([True, True, True, True]))
    result = DataframeType({"value": df}).variable_metadata_not_equal_to(
        {"target": "STUDYID", "comparator": "Req", "metadata": "$CORE_VALUES"}
    )
    assert result.equals(df.convert_to_series([False, False, False, False]))
