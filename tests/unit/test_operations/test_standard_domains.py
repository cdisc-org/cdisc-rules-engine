from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
import pytest
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.standard_domains import StandardDomains
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.services.data_services import LocalDataService


def _create_operation(operation_params, standard_metadata, dataset_type):
    operation_params.dataframe = dataset_type.from_dict(
        {"STUDYID": ["TEST_STUDY"], "AETERM": ["test"]}
    )
    operation_params.standard = "sdtmig"
    operation_params.standard_version = "3-4"
    cache = InMemoryCacheService.get_instance()
    library_metadata = LibraryMetadataContainer(standard_metadata=standard_metadata)
    data_service = LocalDataService.get_instance(
        cache_service=cache, config=ConfigService()
    )
    return StandardDomains(
        operation_params,
        operation_params.dataframe,
        cache,
        data_service,
        library_metadata,
    )


@pytest.mark.parametrize(
    "domains_input, expected_domains",
    [
        ({"AE", "FA", "LB", "QS", "CM", "DM"}, ["AE", "CM", "DM", "FA", "LB", "QS"]),
        (["AE", "FA", "LB"], ["AE", "FA", "LB"]),
        (("AE", "FA", "LB"), ["AE", "FA", "LB"]),
        (["QS", "AE", "FA", "LB", "CM"], ["AE", "CM", "FA", "LB", "QS"]),
        (set(), []),
        ([], []),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_standard_domains_returns_sorted_list(
    operation_params: OperationParams,
    dataset_type,
    domains_input,
    expected_domains,
):
    standard_metadata = {"domains": domains_input}
    operation = _create_operation(operation_params, standard_metadata, dataset_type)
    result = operation.execute()
    domain_list = result[operation_params.operation_id].iloc[0]
    assert domain_list == expected_domains


@pytest.mark.parametrize(
    "standard_metadata, expected_length",
    [
        ({}, 0),
        ({"domains": None}, 0),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_standard_domains_handles_missing_or_none_domains(
    operation_params: OperationParams,
    dataset_type,
    standard_metadata,
    expected_length,
):
    operation = _create_operation(operation_params, standard_metadata, dataset_type)
    result = operation.execute()
    domain_list = result[operation_params.operation_id].iloc[0]
    assert isinstance(domain_list, list)
    assert len(domain_list) == expected_length


@pytest.mark.parametrize(
    "standard_metadata",
    [
        ({"domains": {}}),
        ({"domains": 123}),
        ({"domains": "invalid"}),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_standard_domains_raises_error_for_invalid_type(
    operation_params: OperationParams,
    dataset_type,
    standard_metadata,
):
    operation = _create_operation(operation_params, standard_metadata, dataset_type)
    with pytest.raises(TypeError):
        operation.execute()
