from cdisc_rules_engine.operations.base_operation import BaseOperation
import pandas as pd


class LabelReferencedVariableMetadata(BaseOperation):
    def _execute_operation(self):
        """
        Generates a dataframe where each record in the dataframe
        is the variable metadata corresponding with the variable label
        found in the column provided in self.params.target.
        """
        variables_metadata = self._get_variables_metadata_from_standard()
        df = pd.DataFrame(variables_metadata).add_prefix(f"{self.params.operation_id}_")
        return (
            df.merge(
                self.evaluation_dataset,
                left_on=f"{self.params.operation_id}_label",
                right_on=self.params.target,
                how="right",
            )
            .filter(like=self.params.operation_id, axis=1)
            .fillna("")
        )
