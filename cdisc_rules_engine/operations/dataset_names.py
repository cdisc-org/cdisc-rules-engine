from .base_operation import BaseOperation


class DatasetNames(BaseOperation):
    def _execute_operation(self):
        """
        Returns a list of the domains in the study
        """
        return list({dataset.get("filename", "") for dataset in self.params.datasets})
