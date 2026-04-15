from dataclasses import dataclass
from typing import List

from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult


@dataclass
class SqlOperationParams:
    """
    This class defines input parameters for rule operations.
    Rule operations are defined in DataProcessor class.
    """

    # Required parameters (no defaults) first
    domain: str
    target: str
    standards_context: BaseStandardsContext

    # Optional parameters with defaults
    name: str = None
    previous_operations: dict[str, SqlOperationResult] = None
    grouping: List[str] = None
    filter: dict = None
    key_name: str = None
    key_value: str = None
    ct_package_types: List[str] = None
    ct_version: str = None
    ct_attribute: str = None
    ct_conditions: dict = None
    attribute_name: str = None
    subtract: str = None
    external_dictionary_type: str = None
    filter_attribute: str = None
    filter_value: str = None

    # standard_substandard: str = None
    # attribute_name: str = None
    # case_sensitive: bool = True
    # codelist: str = None
    # codelist_code: str = None
    # codelists: list = None
    # ct_package: list = None
    # ct_packages: list = None
    # ct_package_type: str = None
    # term_code: str = None
    # term_value: str = None
    # dictionary_term_type: str = None
    # external_dictionaries: ExternalDictionariesContainer = None
    # external_dictionary_term_variable: str = None
    # grouping: List[str] = None
    # grouping_aliases: List[str] = None
    # level: str = None
    # map: List[dict] = None
    # original_target: str = None
    # returntype: str = None
    # target: str = None
