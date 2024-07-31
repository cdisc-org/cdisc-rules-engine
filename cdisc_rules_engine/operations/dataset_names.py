from .base_operation import BaseOperation


class DatasetNames(BaseOperation):
    def _execute_operation(self):
        """
        Returns a list of the dataset names in the study
        """
        return list(
            {
                dataset.get("filename", "").split(".")[0].upper()
                for dataset in self.params.datasets
            }
        )
