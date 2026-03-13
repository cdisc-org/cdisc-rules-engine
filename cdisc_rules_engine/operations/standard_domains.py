from cdisc_rules_engine.operations.base_operation import BaseOperation


class StandardDomains(BaseOperation):
    def _execute_operation(self):
        dataset_names = (
            self.library_metadata.standard_metadata.get("dataset_names") or set()
        ) | (self.library_metadata.model_metadata.get("dataset_names") or set())
        return sorted(list(dataset_names))
