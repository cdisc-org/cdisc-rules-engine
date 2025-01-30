from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.models.dataset import DatasetInterface
from cdisc_rules_engine import config
from typing import List
from cdisc_rules_engine.utilities import sdtm_utilities


class VariablesMetadataWithLibraryMetadataDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the variable metadata from a given file.
        Returns a dataframe with the following columns:
        variable_name
        variable_order_number
        variable_label
        variable_size
        variable_data_type
        variable_has_empty_values
        library_variable_name,
        library_variable_label,
        library_variable_data_type,
        library_variable_role,
        library_variable_core,
        library_variable_order_number
        """
        # get dataset metadata and execute the rule
        content_variables_metadata: DatasetInterface = (
            self.data_service.get_variables_metadata(
                dataset_name=self.dataset_path,
                datasets=self.datasets,
                drop_duplicates=True,
            )
        )
        dataset_contents = self.get_dataset_contents()
        library_variables_metadata = self.get_library_variables_metadata()
        variables: List[dict] = sdtm_utilities.get_variables_metadata_from_standard(
            standard=self.standard,
            standard_version=self.standard_version,
            domain=self.domain,
            config=config,
            cache=self.cache,
            library_metadata=self.library_metadata,
        )
        # Rename columns:
        column_name_mapping = {
            # "ordinal": "order_number",
            "simpleDatatype": "data_type",
        }
        for var in variables:
            var["name"] = var["name"].replace("--", self.domain)
            for key, new_key in column_name_mapping.items():
                if key in var:
                    var[new_key] = var.pop(key)
        library_variables_metadata = self.dataset_implementation.from_records(variables)
        library_variables_metadata.data = library_variables_metadata.data.add_prefix(
            "library_variable_"
        )

        data = content_variables_metadata.merge(
            library_variables_metadata.data,
            how="outer",
            left_on="variable_name",
            right_on="library_variable_name",
        ).fillna("")

        data["variable_has_empty_values"] = data.apply(
            lambda row: self.variable_has_null_values(
                row["variable_name"], dataset_contents
            ),
            axis=1,
        )
        return data

    def variable_has_null_values(
        self, variable: str, content: DatasetInterface
    ) -> bool:
        if variable not in content:
            return True
        series = content[variable]
        return series.mask(series == "").isnull().any()
