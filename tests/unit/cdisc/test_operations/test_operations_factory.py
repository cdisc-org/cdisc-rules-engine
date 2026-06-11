from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.operations_factory import OperationsFactory
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
import pandas as pd
import pytest


def test_register_service(operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()

    class DummyOperation(BaseOperation):
        def _execute_operation(self):
            return 1

    factory = OperationsFactory()
    factory.register_service("dummy", DummyOperation)
    op = factory.get_service(
        "dummy",
        operation_params=operation_params,
        cache=cache,
        data_service=data_service,
        original_dataset=pd.DataFrame(),
        library_metadata=LibraryMetadataContainer(),
    )
    assert isinstance(op, DummyOperation)


def test_register_invalid_service():
    class DummyOperation:
        def _execute_operation(self):
            return 1

    factory = OperationsFactory()
    with pytest.raises(TypeError):
        factory.register_service("dummy", DummyOperation)
