# tests/unit/test_operations/test_referenced_domain_variable_names.py
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.referenced_domain_variable_names import (
    ReferencedDomainVariableNames,
)
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory

STUDY_DATASETS = {
    "AE": ("AE", {"STUDYID": ["S1"], "AESEQ": [1], "AETERM": ["HEADACHE"]}),
    "LB": ("LB", {"STUDYID": ["S1"], "LBSEQ": [1], "LBTESTCD": ["ALT"]}),
    "QS1": ("QS", {"STUDYID": ["S1"], "QSSEQ": [1], "QSTESTCD": ["Q1"]}),
    "QS2": ("QS", {"STUDYID": ["S1"], "QSSEQ": [2], "QSORRES": ["YES"]}),
    "CM": ("CM", {"STUDYID": ["S1"], "CMSEQ": [1], "CMTRT": ["ASPIRIN"]}),
}


def _mock_data_service():
    data_service = MagicMock()
    data_service.get_datasets.return_value = [
        SimpleNamespace(name=name, domain=domain)
        for name, (domain, _) in STUDY_DATASETS.items()
    ]
    data_service.get_dataset.side_effect = lambda dataset_name: PandasDataset.from_dict(
        STUDY_DATASETS[dataset_name][1]
    )
    return data_service


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_referenced_domain_variable_names(
    dataset_type, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = _mock_data_service()

    dataset = dataset_type.from_dict(
        {
            "USUBJID": ["01", "02", "03", "04", "05", "06"],
            "RDOMAIN": ["AE", "LB", None, "XX", "ae", "QS"],
        }
    )
    operation_params.dataframe = dataset
    operation_params.target = "RDOMAIN"

    result = ReferencedDomainVariableNames(
        operation_params, dataset, cache, data_service
    ).execute()

    assert operation_params.operation_id in result
    expected = [
        ["AESEQ", "AETERM", "STUDYID"],
        ["LBSEQ", "LBTESTCD", "STUDYID"],
        [],
        [],
        ["AESEQ", "AETERM", "STUDYID"],
        ["QSORRES", "QSSEQ", "QSTESTCD", "STUDYID"],
    ]
    for actual, exp in zip(result[operation_params.operation_id], expected):
        assert list(actual) == exp

    loaded = {
        call.kwargs["dataset_name"] for call in data_service.get_dataset.call_args_list
    }
    assert "cm.xpt" not in loaded
