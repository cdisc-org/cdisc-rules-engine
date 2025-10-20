from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlContentsDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Default builder for Record Data rules.
    Just returns the base table name (no transformation needed).
    """

    def build(self) -> str:
        """
        Return the appropriate table name for the rule.
        This will be the joined table if the rule specifies a join,
        otherwise it will be the base dataset table.
        """
        # This is the logic moved from your old execute_rule function
        dataset_id = self.data_service.get_dataset_for_rule(self.dataset_metadata, self.rule, self.standards_context)
        return dataset_id
