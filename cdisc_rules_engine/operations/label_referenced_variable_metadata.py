from cdisc_rules_engine.operations.base_operation import BaseOperation


class LabelReferencedVariableMetadata(BaseOperation):
    def _execute_operation(self):
        """
        Generates a dataframe where each record in the dataframe
        is the variable metadata corresponding with the variable label
        found in the column provided in self.params.target.
        """
        variables_metadata = self._get_variables_metadata_from_standard()
        df = self.evaluation_dataset.__class__.from_records(variables_metadata)
        df.data = df.data.add_prefix(f"{self.params.operation_id}_")
        target_columns = df.columns
        return self.evaluation_dataset.__class__(
            df.merge(
                self.evaluation_dataset.data,
                left_on=f"{self.params.operation_id}_label",
                right_on=self.params.target,
                how="right",
            ).data.fillna("")[target_columns]
        )
