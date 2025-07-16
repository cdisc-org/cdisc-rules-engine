"""
Test to verify that cache failures don't break rule validation.
This tests the fix for the issue where CORE-000529 was failing due to cache errors.
"""

import pandas as pd
from unittest.mock import Mock, patch
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.interfaces.cache_service_interface import (
    CacheServiceInterface
)


class MockFailingCacheService(CacheServiceInterface):
    """Mock cache service that simulates cache failure on add operation."""

    def __init__(self):
        self.cache = {}

    def add(self, cache_key, data):
        # Simulate the multiprocessing cache failure
        raise ValueError("invalid option: reset(base=-11200)")

    def get(self, cache_key):
        return self.cache.get(cache_key)

    def exists(self, cache_key):
        return cache_key in self.cache

    def clear(self, cache_key):
        self.cache.pop(cache_key, None)

    def clear_all(self, prefix=None):
        self.cache.clear()

    @classmethod
    def get_instance(cls, **kwargs):
        return cls()


def test_cache_failure_does_not_break_rule_validation():
    """Test that cache failures don't prevent rule validation from completing."""

    # Create test data
    test_data = pd.DataFrame({
        'DOMAIN': ['AE', 'AE', 'AE'],
        'USUBJID': ['001', '002', '003'],
        'AESTDY': [1, 2, 3],
        'AEENDY': [5, 6, 7]
    })

    dataset = PandasDataset(test_data)

    # Create mock cache that fails on add
    failing_cache = MockFailingCacheService()

    # Mock data service
    mock_data_service = Mock()
    mock_data_service.dataset_implementation = PandasDataset

    # Mock is_dummy_data to return False (so cache is used)
    with patch('cdisc_rules_engine.utilities.rule_processor.'
               'DataProcessor.is_dummy_data', return_value=False):

        # Create rule processor with failing cache
        rule_processor = RuleProcessor(
            cache=failing_cache,
            data_service=mock_data_service,
            library_metadata=None
        )

        # Mock operation that returns a result
        mock_operation = Mock()
        mock_operation.execute.return_value = dataset

        # Mock the operations_factory to return our mock operation
        with patch('cdisc_rules_engine.utilities.rule_processor.'
                   'operations_factory.get_service',
                   return_value=mock_operation):

            # Create operation parameters
            operation_params = OperationParams(
                core_id="CORE-000529",
                operation_id="test_op",
                operation_name="test_operation",
                dataframe=dataset,
                target="AESTDY",
                original_target="AESTDY",
                domain="AE",
                dataset_path="/test/path",
                directory_path="/test",
                datasets=[],
                grouping=[],
                standard="SDTM",
                standard_version="3.1",
                standard_substandard="SDTM",
                external_dictionaries=None,
                ct_version=None,
                ct_package_type=None,
                ct_attribute=None,
                ct_package_types=[],
                ct_packages=None,
                ct_package=None,
                attribute_name="",
                key_name="",
                key_value="",
                case_sensitive=True,
                external_dictionary_type=None,
                external_dictionary_term_variable=None,
                dictionary_term_type=None,
                filter=None,
                grouping_aliases=None,
                level=None,
                returntype=None,
                codelists=None,
                codelist=None,
                codelist_code=None,
                map=None,
                term_code=None,
                term_value=None,
            )

            # Execute the operation - this should NOT fail even though
            # cache.add fails. This is the key test: before the fix,
            # this would raise the cache error. After the fix, it should
            # log a warning but continue successfully
            result = rule_processor._execute_operation(
                operation_params, dataset, []
            )

            # Verify the result is returned correctly
            assert result is not None
            assert isinstance(result, PandasDataset)
            assert len(result.data) == 3
            assert result.data['DOMAIN'].tolist() == ['AE', 'AE', 'AE']
            assert result.data['USUBJID'].tolist() == ['001', '002', '003']

            # Verify the operation was called
            mock_operation.execute.assert_called_once()


def test_cache_failure_logs_warning_message():
    """Test that cache failures log the expected warning message."""

    # Create test data
    test_data = pd.DataFrame({
        'DOMAIN': ['VS', 'VS'],
        'USUBJID': ['001', '002'],
        'VSTESTCD': ['SYSBP', 'DIABP'],
        'VSSTRESN': [120, 80]
    })

    dataset = PandasDataset(test_data)

    # Create mock cache that fails on add
    failing_cache = MockFailingCacheService()

    # Mock data service
    mock_data_service = Mock()
    mock_data_service.dataset_implementation = PandasDataset

    # Mock is_dummy_data to return False (so cache is used)
    with patch('cdisc_rules_engine.utilities.rule_processor.'
               'DataProcessor.is_dummy_data', return_value=False):

        # Create rule processor with failing cache
        rule_processor = RuleProcessor(
            cache=failing_cache,
            data_service=mock_data_service,
            library_metadata=None
        )

        # Mock operation that returns a result
        mock_operation = Mock()
        mock_operation.execute.return_value = dataset

        # Mock the logger to capture warning messages
        with patch('cdisc_rules_engine.utilities.rule_processor.'
                   'logger') as mock_logger:
            with patch('cdisc_rules_engine.utilities.rule_processor.'
                       'operations_factory.get_service',
                       return_value=mock_operation):
                with patch('cdisc_rules_engine.utilities.rule_processor.'
                           'get_operations_cache_key',
                           return_value="test_cache_key"):

                    # Create operation parameters
                    operation_params = OperationParams(
                        core_id="CORE-000529",
                        operation_id="test_op",
                        operation_name="test_operation",
                        dataframe=dataset,
                        target="VSSTRESN",
                        original_target="VSSTRESN",
                        domain="VS",
                        dataset_path="/test/path",
                        directory_path="/test",
                        datasets=[],
                        grouping=[],
                        standard="SDTM",
                        standard_version="3.1",
                        standard_substandard="SDTM",
                        external_dictionaries=None,
                        ct_version=None,
                        ct_package_type=None,
                        ct_attribute=None,
                        ct_package_types=[],
                        ct_packages=None,
                        ct_package=None,
                        attribute_name="",
                        key_name="",
                        key_value="",
                        case_sensitive=True,
                        external_dictionary_type=None,
                        external_dictionary_term_variable=None,
                        dictionary_term_type=None,
                        filter=None,
                        grouping_aliases=None,
                        level=None,
                        returntype=None,
                        codelists=None,
                        codelist=None,
                        codelist_code=None,
                        map=None,
                        term_code=None,
                        term_value=None,
                    )

                    # Execute the operation
                    result = rule_processor._execute_operation(
                        operation_params, dataset, []
                    )

                    # Verify result is still returned
                    assert result is not None
                    assert isinstance(result, PandasDataset)

                    # Verify the warning was logged
                    mock_logger.warning.assert_called_once()
                    call_args = mock_logger.warning.call_args[0][0]
                    assert "Failed to add result to cache" in call_args
                    assert "test_cache_key" in call_args
                    assert "invalid option: reset(base=-11200)" in call_args
                    assert "Continuing with operation result" in call_args
