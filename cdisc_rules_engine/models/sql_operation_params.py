from dataclasses import dataclass


@dataclass
class SqlOperationParams:
    """
    This class defines input parameters for rule operations.
    Rule operations are defined in DataProcessor class.
    """

    # Required parameters (no defaults) first
    domain: str
    standard: str
    standard_version: str
    target: str
    grouping: bool = False  # Add later?

    # Optional parameters with defaults
    # standard_substandard: str = None
    # attribute_name: str = None
    # case_sensitive: bool = True
    # codelist: str = None
    # codelist_code: str = None
    # codelists: list = None
    # ct_attribute: str = None
    # ct_package_types: List[str] = None
    # ct_package: list = None
    # ct_packages: list = None
    # ct_version: str = None
    # ct_package_type: str = None
    # term_code: str = None
    # term_value: str = None
    # dictionary_term_type: str = None
    # external_dictionaries: ExternalDictionariesContainer = None
    # external_dictionary_term_variable: str = None
    # external_dictionary_type: str = None
    # filter: dict = None
    # grouping: List[str] = None
    # grouping_aliases: List[str] = None
    # key_name: str = None
    # key_value: str = None
    # level: str = None
    # map: List[dict] = None
    # original_target: str = None
    # returntype: str = None
    # target: str = None
