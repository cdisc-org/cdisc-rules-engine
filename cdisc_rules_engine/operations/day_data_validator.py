from cdisc_rules_engine.operations.base_operation import BaseOperation
from datetime import datetime


class DayDataValidator(BaseOperation):
    def _execute_operation(self):
        dtc_value = self.params.dataframe[self.params.target].map(
            datetime.fromisoformat
        )
        rfstdtc_value = self.params.dataframe["RFSTDTC"].map(datetime.fromisoformat)

        # + 1 if --DTC is on or after RFSTDTC
        delta = (dtc_value - rfstdtc_value).map(
            lambda x: x.days if x.days < 0 else x.days + 1
        )
        return delta
