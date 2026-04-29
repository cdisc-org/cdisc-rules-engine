from unittest.mock import MagicMock, mock_open, patch

from cdisc_rules_engine.dataset_builders.jsonata_dataset_builder import (
    JSONataDatasetBuilder,
)


def test_get_dataset_uses_cache():
    metadata = MagicMock()
    metadata.full_path = "fake_path.json"

    data_service = MagicMock()
    data_service.read_data.return_value.__enter__.return_value = mock_open(
        read_data='{"a": {"b": 1}}'
    )()

    cache_service = MagicMock()
    cache_service.get.side_effect = [None, {"cached": True}]

    builder = JSONataDatasetBuilder(
        dataset_metadata=metadata,
        data_service=data_service,
        rule={},
        cache_service=cache_service,
        rule_processor=MagicMock(),
        data_processor=MagicMock(),
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
    )
    builder.cache_service = cache_service

    with patch(
        "cdisc_rules_engine.dataset_builders.jsonata_dataset_builder.load",
        return_value={"a": {"b": 1}},
    ) as mock_load:

        result1 = builder.get_dataset()
        result2 = builder.get_dataset()

        assert result1 == {"_path": "", "a": {"_path": "/a", "b": 1}}

        # second call gets from cache
        assert result2 == {"cached": True}

        assert mock_load.call_count == 1

        assert cache_service.get.call_count == 2
        assert cache_service.add.call_count == 1
