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


# expected_type legend (updated for new logic):
#   ok -> []
#   invalid_html -> message starts with "Invalid HTML fragment:" (html5lib parse failed)
#   invalid_xml -> message starts with "Invalid XML fragment:" (HTML ok, XML well-formedness or DTD failed)


@pytest.mark.parametrize(
    "value, expected_type",
    [
        ("<p>Test</p>", "ok"),
        (
            '<div><p>The primary objectives of this study are</p>\r\n<ul>\r\n<li><usdm:ref attribute="text" '
            'id="Objective_1" klass="Objective"></usdm:ref></li>\r\n<li><usdm:ref attribute="text" id="Objective_2" '
            'klass="Objective"></usdm:ref></li>\r\n</ul></div>',
            "ok",
        ),  # with prefixed tags
        (
            "<usdm:ref klass='klassName' id='idValue' attribute='attributeName'/>",
            "ok",
        ),  # single custom prefix
        ("<p><b>Bad</p>", "invalid_xml"),  # mismatched tags => XML error
        ("Plain text only", "invalid_xml"),  # no tags => fails XML well-formedness
        ("", "ok"),  # empty
        (None, "ok"),  # None
        ("<div><", "invalid_xml"),  # broken XML
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
    elif expected_type == "invalid_html":
        assert len(cell) == 1 and cell[0].startswith("Invalid HTML fragment:")
    elif expected_type == "invalid_xml":
        assert len(cell) == 1 and cell[0].startswith("Invalid XML fragment:")
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
