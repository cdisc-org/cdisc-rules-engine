from .base_operation import BaseOperation


class StudyDomains(BaseOperation):
    def _execute_operation(self):
        """
        Returns a list of the domains in the study
        """
        return list({(dataset.domain or "") for dataset in self.params.datasets})
