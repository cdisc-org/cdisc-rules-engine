from .base_operation import BaseOperation


class DatasetNames(BaseOperation):
    def _execute_operation(self):
        """
        Returns a list of the dataset names in the study
        """
        return list({dataset.name for dataset in self.params.datasets})
