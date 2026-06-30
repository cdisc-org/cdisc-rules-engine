from unittest.mock import MagicMock

from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.sql_dataset_builders.sql_dataset_builder_factory import (
    SqlDatasetBuilderFactory,
)
from cdisc_rules_engine.sql_dataset_builders.sql_stf_dataset_builder import (
    SqlSTFDatasetBuilder,
)


def test_factory_returns_stf_builder_for_stf_metadata_check_rule_type():
    factory = SqlDatasetBuilderFactory()
    builder = factory.get_service(
        RuleTypes.STF_METADATA_CHECK.value,
        rule={},
        data_service=MagicMock(),
        dataset_metadata=MagicMock(),
        standards_context=MagicMock(),
    )

    assert isinstance(builder, SqlSTFDatasetBuilder)
