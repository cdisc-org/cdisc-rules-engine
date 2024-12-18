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
from cdisc_rules_engine.dataset_builders.contents_define_variables_dataset_builder import (
    ContentsDefineVariablesDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.contents_define_dataset_builder import (
    ContentsDefineDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.contents_define_vlm_dataset_builder import (
    ContentsDefineVLMDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.variables_metadata_with_library_metadata import (
    VariablesMetadataWithLibraryMetadataDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.define_variables_with_library_metadata import (
    DefineVariablesWithLibraryMetadataDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.variables_metadata_with_define_and_library_dataset_builder import (
    VariablesMetadataWithDefineAndLibraryDatasetBuilder,
)
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.enums.rule_types import RuleTypes


class DatasetBuilderFactory(FactoryInterface):
    _builders_map = {
        RuleTypes.DATASET_METADATA_CHECK.value: ContentMetadataDatasetBuilder,
        RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value: ContentsDefineDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK.value: VariablesMetadataDatasetBuilder,
        RuleTypes.DOMAIN_PRESENCE_CHECK.value: DomainListDatasetBuilder,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK.value: DefineVariablesDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value: VariablesMetadataWithDefineDatasetBuilder,
        RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value: ContentsDatasetBuilder,
        RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value: ContentsDatasetBuilder,
        RuleTypes.DEFINE_ITEM_GROUP_METADATA_CHECK.value: DefineItemGroupDatasetBuilder,
        RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VARIABLE.value: ContentsDefineVariablesDatasetBuilder,
        RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VLM.value: ContentsDefineVLMDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_LIBRARY.value: VariablesMetadataWithLibraryMetadataDatasetBuilder,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK_AGAINST_LIBRARY.value: DefineVariablesWithLibraryMetadataDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value: VariablesMetadataWithDefineAndLibraryDatasetBuilder,
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
            kwargs.get("domain", ""),
            kwargs.get("define_xml_path"),
            kwargs.get("standard"),
            kwargs.get("standard_version"),
            kwargs.get("standard_substandard", None),
            kwargs.get("library_metadata"),
        )
        return builder
