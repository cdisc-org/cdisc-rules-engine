from cdisc_rules_engine.operations.base_operation import BaseOperation
from datetime import datetime
import numpy as np
from cdisc_rules_engine.services import logger


class DayDataValidator(BaseOperation):
    def _execute_operation(self):
        logger.info(
            f"trying to find '{self.params.target}' in the {self.evaluation_dataset['DOMAIN'].iloc[0]}."
        )
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
            dm_data = self.data_service.concat_split_datasets(
                self.data_service.get_dataset, files
            )
        else:
            dm_data = self.data_service.get_dataset(
                dataset_name=dm_datasets[0].get("full_path")
                or dm_datasets[0]["filename"]
            )

        new_dataset = self.evaluation_dataset.merge(
            dm_data[["USUBJID", "RFSTDTC"]], on="USUBJID", suffixes=("", "_dm")
        )
        rfstdtc_value = "RFSTDTC"
        if "RFSTDTC_dm" in new_dataset:
            rfstdtc_value = "RFSTDTC_dm"
        delta = (dtc_value - new_dataset[rfstdtc_value].map(self.parse_timestamp)).map(
            self.get_day_difference
        )
        return self.evaluation_dataset.convert_to_series(delta.replace(np.nan, ""))

    def parse_timestamp(self, timestamp: str) -> datetime:
        try:
            dt = datetime.fromisoformat(timestamp)
            return dt.date()
        except TypeError:
            # Null date time
            return None
        except ValueError:
            # Value is not iso format
            return None

    def get_day_difference(self, delta: datetime) -> int:
        # Return 1 if the --DTC value is the same as the DY
        return delta.days if delta.days < 0 else delta.days + 1
