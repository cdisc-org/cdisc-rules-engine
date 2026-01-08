import pandas as pd

from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.models.dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.split_by import SplitBy
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.data_services import DataServiceFactory


def test_split_by(operation_params: OperationParams):
    dataset = PandasDataset.from_dict(
        {
            "target": [
                "ABDOMINAL WALL",
                "ABDOMINAL WALL;ADIPOSE TISSUE, BROWN",
                "ABDOMINAL WALL;ADIPOSE TISSUE, BROWN;AIR SAC",
            ]
        }
    )
    config = ConfigService()
    cache_service = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache_service).get_data_service()
    operation_params.delimiter = ";"

    operation = SplitBy(
        operation_params,
        dataset,
        cache_service,
        data_service,
    )
    result = operation.execute()

    assert result[operation_params.operation_id].equals(
        pd.Series(
            [
                ["ABDOMINAL WALL"],
                ["ABDOMINAL WALL", "ADIPOSE TISSUE, BROWN"],
                ["ABDOMINAL WALL", "ADIPOSE TISSUE, BROWN", "AIR SAC"],
            ],
        )
    )
