from cdisc_rules_engine.operations.base_operation import BaseOperation


class DatasetIsCustom(BaseOperation):
    def _execute_operation(self):
        """Return True if the current dataset (by dataset_path) is not in standard library datasets.

        self.params.datasets is expected to be a list of dataset objects with at least
        attributes 'name' and 'filename'. We find the dataset whose 'filename'
        matches params.dataset_path, take its name and check whether it is present
        among the datasets defined in the standard library model metadata.
        """
        model_data: dict = self.library_metadata.model_metadata or {}
        library_datasets = model_data.get("datasets", []) or []

        # Collect a set of dataset names defined in the library model metadata
        library_dataset_names = set(
            dataset.get("name")
            for dataset in library_datasets
            if isinstance(dataset, dict) and dataset.get("name")
        )

        datasets = getattr(self.params, "datasets", []) or []
        dataset_path = getattr(self.params, "dataset_path", None)
        ds_name = None
        if dataset_path:
            for ds in datasets:
                if getattr(ds, "filename", None) == dataset_path:
                    ds_name = getattr(ds, "name", None)
                    break

        # A dataset is considered custom if its name is not present in the library
        return ds_name not in library_dataset_names
