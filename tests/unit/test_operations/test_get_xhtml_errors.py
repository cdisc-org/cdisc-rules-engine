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


# expected_type legend:
#   ok -> []
#   not_html -> ["Not HTML fragment"]
#   invalid_html -> message starts with "Invalid HTML fragment:" (lenient HTML parse failed)
#   invalid_xhtml -> message starts with "Invalid XHTML fragment:" (HTML ok, XML invalid)


@pytest.mark.parametrize(
    "value, expected_type",
    [
        ("<p>Test</p>", "ok"),
        ("<p>A</p><p>B</p>", "ok"),  # multi-root
        (
            "<usdm:ref klass='klassName' id='idValue' attribute='attributeName'/>",
            "ok",
        ),  # custom prefix single
        (
            "<usdm:ref klass='klassName'/><p>Next</p>",
            "ok",
        ),  # custom prefix + another root
        ("<p><b>Bad</p>", "invalid_xhtml"),  # mismatched tags
        ("Plain text only", "not_html"),  # no tags
        ("", "ok"),  # empty -> no errors
        (None, "ok"),  # None -> no errors
        ("<div><", "invalid_xhtml"),  # broken html tolerated by HTML parser, fails XML
    ],
)
def test_get_xhtml_errors(value, expected_type, base_services):
    dataset = PandasDataset.from_dict({"text": [value]})
    params = build_params(dataset)
    _, cache, data_service = base_services
    result_dataset = GetXhtmlErrors(params, dataset, cache, data_service).execute()
    col = params.operation_id
    assert col in result_dataset
    cell = result_dataset[col].iloc[0]
    if expected_type == "ok":
        assert cell == []
    elif expected_type == "not_html":
        assert cell == ["Not HTML fragment"]
    elif expected_type == "invalid_html":
        assert len(cell) == 1 and cell[0].startswith("Invalid HTML fragment:")
    elif expected_type == "invalid_xhtml":
        assert len(cell) == 1 and cell[0].startswith("Invalid XHTML fragment:")
    else:
        pytest.fail(f"Unknown expected_type {expected_type}")


def test_get_xhtml_errors_missing_column(base_services):
    dataset = PandasDataset.from_dict({"OTHER": ["<p>Ok</p>"]})
    params = build_params(dataset)
    _, cache, data_service = base_services
    result_dataset = GetXhtmlErrors(params, dataset, cache, data_service).execute()
    errors_col = params.operation_id
    val = result_dataset[errors_col].iloc[0]
    assert isinstance(val, list) and "Target column 'text' not found" in val[0]
