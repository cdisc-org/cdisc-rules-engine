"""
Rule types.

Record Data rules just run against the dataset (merged with whatever the rule pulls in).
Every other rule type runs against rows describing the dataset.
One per variable, per value, per define item, etc.
Those are views, our view are:
- Metadata is read once per study in MetadataStore and written in as literal rows
- Value checks unpivot the dataset inside the view.
What's great about this is no table gets built or altered for a rule, saves us time on materialisation.

Layout:
    records.py      - checks over the dataset's own rows
    datasets.py     - one row per dataset
    variables.py    - one row per variable (dataset / define / library)
    values.py       - one row per value (the unpivots)
    value_level.py  - VLM
    study.py        - study-wide value checks
    lengths.py      - max-length summary used instead of a full unpivot when that's all a rule needs
"""

from typing import Callable, Dict

from cdisc_rules_engine.compiler.sources import (
    datasets,
    records,
    study,
    value_level,
    values,
    variables,
)
from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import rows_view
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.metadata.stf import STF_METADATA_TYPE, stf_rows


def stf_metadata(context: SourceContext) -> str:
    # Only rule type that reads the STF therefore it lives here rather than getting a module
    stf_file_path = getattr(context.data_service, "stf_file_path", None)
    if not stf_file_path:
        raise ValueError("No STF file path provided. Set stf_file_path or define_xml_path.")
    return rows_view(context, STF_METADATA_TYPE, stf_rows(context.data_service, stf_file_path))


RULE_TYPE_SOURCES: Dict[str, Callable[[SourceContext], str]] = {
    RuleTypes.DOMAIN_PRESENCE_CHECK.value: datasets.domain_presence,
    RuleTypes.DOMAIN_PRESENCE_CHECK_AGAINST_DEFINE.value: datasets.domain_presence_against_define,
    RuleTypes.DATASET_METADATA_CHECK.value: datasets.metadata,
    RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value: datasets.metadata_against_define,
    RuleTypes.DEFINE_ITEM_GROUP_METADATA_CHECK.value: datasets.define_item_group,
    RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE.value: records.contents_against_define,
    RuleTypes.VARIABLE_METADATA_CHECK.value: variables.metadata,
    RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_LIBRARY.value: variables.against_library,
    RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value: variables.against_define,
    RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value: variables.against_define_and_library,  # noqa: E501
    RuleTypes.DEFINE_ITEM_METADATA_CHECK.value: variables.define_items,
    RuleTypes.DEFINE_ITEM_METADATA_CHECK_AGAINST_LIBRARY.value: variables.define_items_against_library,
    RuleTypes.VALUE_CHECK_WITH_VARIABLE_METADATA.value: values.with_variable_metadata,
    RuleTypes.VALUE_CHECK_WITH_DATASET_METADATA.value: values.with_dataset_metadata,
    RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VARIABLE.value: values.against_define_variables,
    RuleTypes.VALUE_CHECK_AGAINST_LIBRARY.value: values.against_library,
    RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VLM.value: value_level.against_define_vlm,
    RuleTypes.GLOBAL_VALUE_CHECK_WITH_VARIABLE_METADATA.value: study.global_values,
    RuleTypes.GLOBAL_VARIABLE_METADATA_CHECK.value: study.global_variable_lengths,
    RuleTypes.STF_METADATA_CHECK.value: stf_metadata,
}


def rule_source(rule: dict, data_service, dataset_metadata, standards_context, datasets: list) -> str:
    """Table / view id the rule's checks should run against for this dataset."""
    context = SourceContext(rule, data_service, dataset_metadata, standards_context, datasets)
    # Record Data, and anything we don't have a builder for, just runs on the dataset itself
    build = RULE_TYPE_SOURCES.get(rule.get("rule_type"), records.record_data)
    return build(context)
