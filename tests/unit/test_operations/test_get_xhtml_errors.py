import pytest
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.get_xhtml_errors import GetXhtmlErrors
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.fixture()
def base_services():
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    return config, cache, data_service


def build_params(dataset, target="text"):
    return OperationParams(
        core_id="core",
        operation_id="$xhtml_errors",
        operation_name="get_xhtml_errors",
        dataframe=dataset,
        target=target,
        domain="AE",
        dataset_path="/tmp/ae.xpt",
        directory_path="/tmp",
        datasets=[],
        standard="sdtmig",
        standard_version="3.4",
    )


@pytest.mark.parametrize(
    "values, expected",
    [
        (["<p>Test</p>"], [[]]),  # valid single root
        (["<p>Hi</p><br/><span>Ok</span>"], [[]]),  # multi-root valid after wrapping
        (["<p><b>Bad</p>"], [["__INVALID__"]]),  # mismatched tags
        ([""], [[]]),  # empty string -> no errors
        ([None], [[]]),  # None treated as no error (ignored)
        ([123], [["Value is not a string"]]),  # non-string
        ([["p", "q"]], [["Value is not a string (got iterable)"]]),  # iterable
    ],
)
def test_get_xhtml_errors(values, expected, base_services):
    dataset = PandasDataset.from_dict({"text": values})
    params = build_params(dataset)
    _, cache, data_service = base_services
    result_dataset = GetXhtmlErrors(params, dataset, cache, data_service).execute()
    col = params.operation_id
    assert col in result_dataset
    for i, exp in enumerate(expected):
        cell = result_dataset[col].iloc[i]
        if exp and exp[0] == "__INVALID__":
            assert any("Invalid XHTML fragment" in msg for msg in cell)
        else:
            assert cell == exp


def test_get_xhtml_errors_missing_column(base_services):
    # нет столбца text
    dataset = PandasDataset.from_dict({"OTHER": ["<p>Ok</p>"]})
    params = build_params(dataset)
    _, cache, data_service = base_services
    result_dataset = GetXhtmlErrors(params, dataset, cache, data_service).execute()
    errors_col = params.operation_id
    assert errors_col in result_dataset
    val = result_dataset[errors_col].iloc[0]
    assert isinstance(val, list) and "Target column 'text' not found" in val[0]
