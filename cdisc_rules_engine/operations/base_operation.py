import pandas as pd
from cdisc_rules_engine.models.operation_params import OperationParams
from abc import abstractmethod

from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
)


class BaseOperation:
    def __init__(
        self,
        params: OperationParams,
        original_dataset: pd.DataFrame,
        cache_service: CacheServiceInterface,
        data_service: DataServiceInterface,
    ):
        self.params = params
        self.cache = cache_service
        self.data_service = data_service
        self.evaluation_dataset = original_dataset

    @abstractmethod
    def _execute_operation(self):
        """Perform operation calculations."""
        pass

    def execute(self) -> pd.DataFrame:
        result = self._execute_operation()
        return self._handle_operation_result(result)

    def _handle_operation_result(self, result) -> pd.DataFrame:
        if self.params.grouping:
            return self._handle_grouped_result(result)
        elif isinstance(result, dict):
            return self._handle_dictionary_result(result)
        else:
            # Handle single results
            self.evaluation_dataset[self.params.operation_id] = result
            return self.evaluation_dataset

    def _handle_grouped_result(self, result):
        # Handle grouped results
        result = result.rename(columns={self.params.target: self.params.operation_id})
        target_columns = self.params.grouping + [self.params.operation_id]
        return self.evaluation_dataset.merge(
            result[target_columns], on=self.params.grouping, how="left"
        )

    def _handle_dictionary_result(self, result):
        self.evaluation_dataset[self.params.operation_id] = [result] * len(
            self.params.dataframe
        )
        return self.evaluation_dataset
