import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class ExtractMetadata(BaseOperation):
    def _execute_operation(self):
        # get metadata
        metadata: pd.DataFrame = self.data_service.get_dataset_metadata(
            dataset_name=self.params.dataset_path, datasets=self.params.datasets
        )

        if self.params.target == "domain_suffix":
            return self._extract_domain_suffix(metadata)

        # extract target value. Metadata df always has one row
        target_value = metadata.get(self.params.target, pd.Series())[0]
        return target_value

    def _extract_domain_suffix(self, metadata: pd.DataFrame) -> str:
        dataset_name = metadata.get("dataset_name", pd.Series())
        if dataset_name.empty:
            return ""

        name = dataset_name.iloc[0]
        if isinstance(name, str) and len(name) >= 4 and name.startswith("AP"):
            return name[2:4]

        raw_metadata = self.data_service.get_raw_dataset_metadata(
            dataset_name=self.params.dataset_path, datasets=self.params.datasets
        )
        if raw_metadata and raw_metadata.first_record:
            domain = raw_metadata.first_record.get("DOMAIN", "")
            if isinstance(domain, str) and len(domain) >= 4 and domain.startswith("AP"):
                return domain[2:4]

        return ""
