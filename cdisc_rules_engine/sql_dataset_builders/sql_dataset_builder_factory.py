from typing import Type
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_contents_dataset_builder import (
    SqlContentsDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_variables_metadata_builder import (
    SqlVariablesMetadataBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_domain_list_builder import (
    SqlDomainListDatasetBuilder,
)


class SqlDatasetBuilderFactory:
    """
    Factory to get the right SQL dataset builder based on rule type.
    """

    # Mapping of rule types to their SQL builder implementations
    _builders_map = {
        RuleTypes.VARIABLE_METADATA_CHECK.value: SqlVariablesMetadataBuilder,
        RuleTypes.DOMAIN_PRESENCE_CHECK.value: SqlDomainListDatasetBuilder,
    }

    # List of rule types that are not yet implemented
    _unimplemented_types = {
        RuleTypes.DATASET_METADATA_CHECK.value,
        RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value,
        RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE.value,
        RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value,
        RuleTypes.DEFINE_ITEM_GROUP_METADATA_CHECK.value,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK.value,
        RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_LIBRARY.value,
        RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VARIABLE.value,
        RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VLM.value,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK_AGAINST_LIBRARY.value,
        RuleTypes.VALUE_CHECK_WITH_DATASET_METADATA.value,
        RuleTypes.VALUE_CHECK_WITH_VARIABLE_METADATA.value,
    }

    @classmethod
    def register_service(cls, name: str, builder: Type[SqlBaseDatasetBuilder]) -> None:
        """
        Register a new SQL dataset builder for a specific rule type.
        """
        if not name:
            raise ValueError("Builder name must not be empty!")
        if not issubclass(builder, SqlBaseDatasetBuilder):
            raise TypeError("Implementation of SqlBaseDatasetBuilder required!")
        cls._builders_map[name] = builder

    def get_service(self, rule_type: str, **kwargs) -> SqlBaseDatasetBuilder:
        """
        Get instance of SQL dataset builder by rule type.

        Uses _builders_map to instantiate the appropriate builder.
        Falls back to SqlContentsDatasetBuilder (Record Data) as default.
        Raises NotImplementedError for explicitly unimplemented rule types.
        """

        # Get builder class from map, default to SqlContentsDatasetBuilder
        builder_class = self._builders_map.get(rule_type, SqlContentsDatasetBuilder)
        return builder_class(**kwargs)
