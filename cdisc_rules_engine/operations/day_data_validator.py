from cdisc_rules_engine.operations.base_operation import BaseOperation
from datetime import datetime
import numpy as np


class DayDataValidator(BaseOperation):
    def _execute_operation(self):

        dtc_value = self.evaluation_dataset[self.params.target].map(
            self.parse_timestamp
        )

        # Always get RFSTDTC column from DM dataset.
        dm_datasets = [
            dataset for dataset in self.params.datasets if dataset["domain"] == "DM"
        ]
        if not dm_datasets:
            # Return none for all values if dm is not provided.
            return [0] * len(self.evaluation_dataset[self.params.target])
        if len(dm_datasets) > 1:
            files = [dataset["filename"] for dataset in dm_datasets]
            dm_data = self.data_service.join_split_datasets(files)
        else:
            dm_data = self.data_service.get_dataset(dm_datasets[0]["filename"])

        new_dataset = self.evaluation_dataset.merge(
            dm_data[["USUBJID", "RFSTDTC"]], on="USUBJID", suffixes=("", "_dm")
        )
        rfstdtc_value = "RFSTDTC"
        if "RFSTDTC_dm" in new_dataset:
            rfstdtc_value = "RFSTDTC_dm"
        delta = (dtc_value - new_dataset[rfstdtc_value].map(self.parse_timestamp)).map(
            self.get_day_difference
        )
        return delta.replace(np.nan, "")

    def parse_timestamp(self, timestamp: str) -> datetime:
        try:
            return datetime.fromisoformat(timestamp)
        except TypeError:
            # Null date time
            return None
        except ValueError:
            # Value is not iso format
            return None

    def get_day_difference(self, delta: datetime) -> int:
        # Return 1 if the --DTC value is the same as the DY
        return delta.days if delta.days < 0 else delta.days + 1
