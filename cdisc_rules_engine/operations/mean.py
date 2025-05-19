from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


class Mean(BaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].mean()
        else:
            if isinstance(self.params.dataframe, DaskDataset):
                dask_df = self.params.dataframe.data
                result_df = (
                    dask_df.groupby(self.params.grouping)[self.params.target]
                    .mean()
                    .compute()
                    .reset_index()
                )
                return result_df
            else:
                result = self.params.dataframe.groupby(
                    self.params.grouping, as_index=False
                )[self.params.target].mean()
        return result
