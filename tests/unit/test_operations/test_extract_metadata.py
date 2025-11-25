import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.operations.extract_metadata import ExtractMetadata
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService
from unittest.mock import Mock, MagicMock
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_extract_metadata_get_dataset_name(
    operation_params: OperationParams, dataset_type
):
    mock_data_service = Mock(LocalDataService)
    mock_data_service.get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {"dataset_name": ["AE"]}
    )
    operation_params.dataframe = dataset_type.from_dict(
        {
            "STUDYID": [
                "TEST_STUDY",
                "TEST_STUDY",
                "TEST_STUDY",
            ],
            "AETERM": [
                "test",
                "test",
                "test",
            ],
        }
    )
    operation_params.target = "dataset_name"
    cache = InMemoryCacheService.get_instance()
    # execute operation
    operation = ExtractMetadata(
        operation_params, operation_params.dataframe, cache, mock_data_service
    )
    result: pd.DataFrame = operation.execute()
    assert operation_params.operation_id in result
    for item in result[operation_params.operation_id]:
        assert item == "AE"


def _create_mock_service(dataset_name, first_record=None):
    mock_service = Mock(LocalDataService)
    mock_service.get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {"dataset_name": [dataset_name]}
    )
    raw_metadata = SDTMDatasetMetadata(
        name=dataset_name,
        first_record=first_record,
    )
    mock_service.get_raw_dataset_metadata = MagicMock(return_value=raw_metadata)
    return mock_service


@pytest.mark.parametrize(
    "dataset_name, first_record, expected_suffix",
    [
        ("APFA", None, ""),
        ("APXX", None, ""),
        ("APLB", None, ""),
        ("", {"DOMAIN": "APFA"}, "FA"),
        ("AE", {"DOMAIN": "APFA"}, "FA"),
        ("AP", {"DOMAIN": "APFA"}, "FA"),
        ("APF", {"DOMAIN": "APFA"}, "FA"),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_extract_metadata_domain_suffix_valid_cases(
    operation_params: OperationParams,
    dataset_type,
    dataset_name,
    first_record,
    expected_suffix,
):
    mock_data_service = _create_mock_service(dataset_name, first_record)
    operation_params.dataframe = dataset_type.from_dict(
        {"STUDYID": ["TEST_STUDY"], "DOMAIN": ["APFA"]}
    )
    operation_params.target = "ap_suffix"
    cache = InMemoryCacheService.get_instance()
    operation = ExtractMetadata(
        operation_params, operation_params.dataframe, cache, mock_data_service
    )
    result = operation.execute()
    assert operation_params.operation_id in result
    assert all(
        item == expected_suffix for item in result[operation_params.operation_id]
    )


@pytest.mark.parametrize(
    "dataset_name, first_record",
    [
        ("AE", None),
        ("LB", None),
        ("AP", None),
        ("APF", None),
        ("AE", {"DOMAIN": "AE"}),
        ("AE", {"DOMAIN": "LB"}),
        ("AE", {"DOMAIN": "AP"}),
        ("AE", {"DOMAIN": ""}),
        ("AE", {"DOMAIN": None}),
        ("AE", None),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_extract_metadata_domain_suffix_returns_empty_for_invalid(
    operation_params: OperationParams,
    dataset_type,
    dataset_name,
    first_record,
):
    mock_data_service = _create_mock_service(dataset_name, first_record)
    operation_params.dataframe = dataset_type.from_dict(
        {"STUDYID": ["TEST_STUDY"], "DOMAIN": ["AE"]}
    )
    operation_params.target = "ap_suffix"
    cache = InMemoryCacheService.get_instance()
    operation = ExtractMetadata(
        operation_params, operation_params.dataframe, cache, mock_data_service
    )
    result = operation.execute()
    assert operation_params.operation_id in result
    assert all(item == "" for item in result[operation_params.operation_id])


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_extract_metadata_domain_suffix_uses_domain(
    operation_params: OperationParams, dataset_type
):
    mock_data_service = _create_mock_service("APFA", {"DOMAIN": "APXX"})
    operation_params.dataframe = dataset_type.from_dict(
        {"STUDYID": ["TEST_STUDY"], "DOMAIN": ["APXX"]}
    )
    operation_params.target = "ap_suffix"
    cache = InMemoryCacheService.get_instance()
    operation = ExtractMetadata(
        operation_params, operation_params.dataframe, cache, mock_data_service
    )
    result = operation.execute()
    assert operation_params.operation_id in result
    assert all(item == "XX" for item in result[operation_params.operation_id])


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_extract_metadata_domain_suffix_empty_metadata(
    operation_params: OperationParams, dataset_type
):
    mock_data_service = _create_mock_service("APFA", None)
    mock_data_service.get_dataset_metadata.return_value = pd.DataFrame()
    operation_params.dataframe = dataset_type.from_dict(
        {"STUDYID": ["TEST_STUDY"], "DOMAIN": ["APFA"]}
    )
    operation_params.target = "ap_suffix"
    cache = InMemoryCacheService.get_instance()
    operation = ExtractMetadata(
        operation_params, operation_params.dataframe, cache, mock_data_service
    )
    result = operation.execute()
    assert operation_params.operation_id in result
    assert all(item == "" for item in result[operation_params.operation_id])
