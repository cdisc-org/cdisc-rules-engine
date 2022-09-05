import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface
from datetime import datetime


class DY(OperationInterface):
    def execute(self) -> pd.DataFrame:
        dtc_value = self.params.dataframe[self.params.target].map(
            datetime.fromisoformat
        )
        rfstdtc_value = self.params.dataframe["RFSTDTC"].map(datetime.fromisoformat)

        # + 1 if --DTC is on or after RFSTDTC
        delta = (dtc_value - rfstdtc_value).map(
            lambda x: x.days if x.days < 0 else x.days + 1
        )
        return self._handle_operation_result(delta)
