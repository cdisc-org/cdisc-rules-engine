# cdisc_rules_engine/operations/referenced_domain_variable_names.py
from collections import defaultdict

from pandas import Series

from cdisc_rules_engine.operations.base_operation import BaseOperation


class ReferencedDomainVariableNames(BaseOperation):
    def _execute_operation(self):
        """
        Returns, for each row, the variable (column) names of the dataset in the
        study whose domain matches the value of self.params.target on that row
        (e.g. RDOMAIN). Rows with no matching dataset get an empty list.
        """
        target_values = list(
            self.evaluation_dataset.get(
                self.params.target, [None] * len(self.evaluation_dataset)
            )
        )

        domain_to_datasets = defaultdict(list)
        for dataset in self.data_service.get_datasets():
            if dataset.domain:
                domain_to_datasets[dataset.domain.upper()].append(dataset)

        variables_by_domain = {}
        for value in set(target_values):
            key = self._normalize(value)
            if key is None or key in variables_by_domain:
                continue
            variables = set()
            for dataset in domain_to_datasets.get(key, []):
                data = self.data_service.get_dataset(dataset_name=dataset.name)
                variables.update(str(column).upper() for column in data.columns)
            variables_by_domain[key] = sorted(variables)

        return self.evaluation_dataset.convert_to_series(
            Series(
                [
                    variables_by_domain.get(self._normalize(value), [])
                    for value in target_values
                ]
            )
        )

    @staticmethod
    def _normalize(value):
        if value is None or (isinstance(value, float) and value != value):
            return None
        value = str(value).strip().upper()
        return value or None
