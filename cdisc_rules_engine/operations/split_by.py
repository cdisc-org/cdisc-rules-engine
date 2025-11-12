from cdisc_rules_engine.operations.base_operation import BaseOperation


class SplitBy(BaseOperation):
    def _execute_operation(self):
        if not all((self.params.target, self.params.delimiter)):
            raise ValueError(
                f"name and delimiter are required params for operation {self.params.operation_name}"
            )

        return self.evaluation_dataset[self.params.target].str.split(
            self.params.delimiter
        )
