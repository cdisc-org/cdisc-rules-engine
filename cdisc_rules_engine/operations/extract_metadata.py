import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class ExtractMetadata(BaseOperation):
    def _execute_operation(self):
        # get metadata
        metadata: pd.DataFrame = self.data_service.get_dataset_metadata(
            dataset_name=self.params.dataset_path, datasets=self.params.datasets
        )

        # extract target value. Metadata df always has one row
        target_value = metadata.get(self.params.target, pd.Series())[0]
        return target_value
