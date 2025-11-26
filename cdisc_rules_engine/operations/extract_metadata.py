import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class ExtractMetadata(BaseOperation):
    def _execute_operation(self):
        metadata: pd.DataFrame = self.data_service.get_dataset_metadata(
            dataset_name=self.params.dataset_path, datasets=self.params.datasets
        )

        if self.params.target in ("ap_suffix", "ap_domain_suffix", "domain_suffix"):
            series = metadata.get("ap_suffix", pd.Series())
            return series[0] if len(series) > 0 else ""

        target_value = metadata.get(self.params.target, pd.Series())
        return target_value[0] if len(target_value) > 0 else None
