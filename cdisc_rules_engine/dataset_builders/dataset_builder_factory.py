# flake8: noqa
from typing import Type

from cdisc_rules_engine.interfaces import FactoryInterface
from cdisc_rules_engine.dataset_builders.contents_dataset_builder import (
    ContentsDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.content_metadata_dataset_builder import (
    ContentMetadataDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.variables_metadata_dataset_builder import (
    VariablesMetadataDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.domain_list_dataset_builder import (
    DomainListDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.define_variables_dataset_builder import (
    DefineVariablesDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.variables_metadata_with_define_dataset_builder import (
    VariablesMetadataWithDefineDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.define_item_group_dataset_builder import (
    DefineItemGroupDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.enums.rule_types import RuleTypes


class DatasetBuilderFactory(FactoryInterface):
    _builders_map = {
        RuleTypes.DATASET_METADATA_CHECK.value: ContentMetadataDatasetBuilder,
        RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value: ContentMetadataDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK.value: VariablesMetadataDatasetBuilder,
        RuleTypes.DOMAIN_PRESENCE_CHECK.value: DomainListDatasetBuilder,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK.value: DefineVariablesDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value: VariablesMetadataWithDefineDatasetBuilder,
        RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value: ContentsDatasetBuilder,
        RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value: ContentsDatasetBuilder,
        RuleTypes.DEFINE_ITEM_GROUP_METADATA_CHECK.value: DefineItemGroupDatasetBuilder,
    }

    @classmethod
    def register_service(cls, name: str, builder: Type[BaseDatasetBuilder]) -> None:
        """
        Save mapping of operation name and it's implementation
        """
        if not name:
            raise ValueError("Builder name must not be empty!")
        if not issubclass(builder, BaseDatasetBuilder):
            raise TypeError("Implementation of BaseDatasetBuilder required!")
        cls._operations_map[name] = builder

    def get_service(
        self,
        name,
        **kwargs,
    ) -> BaseDatasetBuilder:
        """
        Get instance of dataset builder by name.
        """
        builder = self._builders_map.get(name, ContentsDatasetBuilder)(
            kwargs.get("rule"),
            kwargs.get("data_service"),
            kwargs.get("cache_service"),
            kwargs.get("rule_processor"),
            kwargs.get("data_processor"),
            kwargs.get("dataset_path"),
            kwargs.get("datasets"),
            kwargs.get("domain"),
        )
        return builder
