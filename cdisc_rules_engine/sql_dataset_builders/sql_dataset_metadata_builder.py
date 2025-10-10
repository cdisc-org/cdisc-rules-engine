from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlDatasetMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Dataset Metadata Check rules.
    NOT YET IMPLEMENTED - placeholder for future implementation.
    """

    def build(self) -> str:
        """
        Raises NotImplementedError - this builder is not yet implemented.
        """
        raise NotImplementedError(
            "SqlDatasetMetadataBuilder is not yet implemented. "
            "Dataset Metadata Check rules are not currently supported in SQL engine."
        )
