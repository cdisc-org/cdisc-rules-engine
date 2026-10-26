import re
import traceback

from cdisc_rules_engine.constants.metadata_columns import DATASET_NAME
from cdisc_rules_engine.exceptions.custom_exceptions import SqlOperatorError
from cdisc_rules_engine.services import logger

from cdisc_rules_engine.check_operators.sql.is_valid_whodrug_level_reference_operator import (
    ValidWHODrugLevelReferenceOperator,
)
from cdisc_rules_engine.check_operators.sql.not_operator import NotOperator
from cdisc_rules_engine.enums.static_tables import StaticTables

from .conformant_value_data_type_operator import ConformantValueDataTypeOperator
from .conformant_value_length_operator import ConformantValueLengthOperator
from .contains_all_operator import ContainsAllOperator
from .contains_operator import ContainsOperator
from .date_comparison_operator import DateComparisonOperator
from .empty_operator import EmptyOperator
from .empty_within_except_last_row_operator import EmptyWithinExceptLastRowOperator
from .ends_with_operator import EndsWithOperator
from .equal_to_operator import EqualToOperator
from .equals_string_part_operator import EqualsStringPartOperator
from .exists_operator import ExistsOperator
from .has_different_values_operator import HasDifferentValuesOperator
from .has_next_corresponding_record_operator import HasNextCorrespondingRecordOperator
from .inconsistent_enumerated_columns_operator import (
    InconsistentEnumeratedColumnsOperator,
)
from .invalid_date_operator import InvalidDateOperator
from .invalid_date_interval_operator import InvalidDateIntervalOperator
from .invalid_duration_operator import InvalidDurationOperator
from .is_complete_date_operator import IsCompleteDateOperator
from .is_contained_by_operator import IsContainedByOperator
from .is_inconsistent_across_dataset_operator import IsInconsistentAcrossDatasetOperator
from .is_not_unique_relationship_operator import IsNotUniqueRelationshipOperator
from .is_ordered_by_operator import IsOrderedByOperator
from .is_ordered_set_operator import IsOrderedSetOperator
from .is_ordered_subset_of_operator import IsOrderedSubsetOfOperator
from .is_substring_of_operator import IsSubstringOfOperator
from .is_unique_set_operator import IsUniqueSetOperator
from .is_valid_external_dict_term_reference_operator import (
    ValidExDictTermReferenceOperator,
)
from .is_valid_external_dict_code_reference_operator import (
    ValidExDictCodeReferenceOperator,
)
from .is_valid_external_dict_code_term_pair_operator import (
    ValidExDictCodeTermPairsOperator,
)
from .matches_regex_operator import MatchesRegexOperator
from .numeric_comparison_operator import NumericComparisonOperator
from .prefix_matches_regex_operator import PrefixMatchesRegexOperator
from .prefix_suffix_equal_to_operator import PrefixSuffixEqualToOperator
from .present_on_multiple_rows_within_operator import (
    PresentOnMultipleRowsWithinOperator,
)
from .references_correct_codelist_operator import ReferencesCorrectCodelistOperator
from .shares_elements_with_operator import SharesElementsWithOperator
from .starts_with_operator import StartsWithOperator
from .string_length_comparison_operator import StringLengthComparisonOperator
from .suffix_matches_regex_operator import SuffixMatchesRegexOperator
from .target_is_sorted_by_operator import TargetIsSortedByOperator
from .value_has_multiple_references_operator import ValueHasMultipleReferencesOperator
from .variable_metadata_equal_to_operator import VariableMetadataEqualToOperator
from .is_latest_available_exdict_version import IsLatestAvailableExDictVersionOperator
from .in_enumerated_columns_operator import InEnumeratedColumnsOperator
from .is_extensible_codelist_code_operator import IsExtensibleCodelistCodeOperator

MEDDRA_CODE_SUFFIX_MAP = {
    "BDSYCD": "SOC",
    "SOCCD": "SOC",
    "HLGTCD": "HLGT",
    "HLTCD": "HLT",
    "PTCD": "PT",
    "LLTCD": "LLT",
}
MEDDRA_TERM_SUFFIX_MAP = {
    "BODSYS": "SOC",
    "SOC": "SOC",
    "HLGT": "HLGT",
    "HLT": "HLT",
    "DECOD": "PT",
    "LLT": "LLT",
}
MEDDRA_PAIR_MAP = {
    "BODSYS": ("SOC", "SOCCD"),
    "BDSYCD": ("SOC", "SOC"),
    "SOC": ("SOC", "SOCCD"),
    "SOCCD": ("SOC", "SOC"),
    "LLT": ("LLT", "LLTCD"),
    "LLTCD": ("LLT", "LLT"),
    "HLGT": ("HLGT", "HLGTCD"),
    "HLGTCD": ("HLGT", "HLGT"),
    "HLT": ("HLT", "HLTCD"),
    "HLTCD": ("HLT", "HLT"),
    "PTCD": ("PT", "DECOD"),
    "DECOD": ("PT", "PTCD"),
}


class PostgresQLOperators:
    """
    Main SQL operators class with dynamic method registration.

    This class uses dynamic registration to combine functionality from individual
    operator classes, maintaining compatibility with the business rules framework
    while providing operations-like modularity.
    """

    name = "dataframe"

    _operator_map = {
        "exists": lambda data: ExistsOperator(data),
        "not_exists": lambda data: NotOperator(data, ExistsOperator),
        "equal_to": lambda data: EqualToOperator(data),
        "not_equal_to": lambda data: EqualToOperator(data, invert=True),
        "equal_to_case_insensitive": lambda data: EqualToOperator(data, case_insensitive=True),
        "not_equal_to_case_insensitive": lambda data: EqualToOperator(data, case_insensitive=True, invert=True),
        "empty": lambda data: EmptyOperator(data),
        "non_empty": lambda data: NotOperator(data, EmptyOperator),
        "less_than": lambda data: NumericComparisonOperator(data, operator="<"),
        "greater_than": lambda data: NumericComparisonOperator(data, operator=">"),
        "less_than_or_equal_to": lambda data: NumericComparisonOperator(data, operator="<="),
        "greater_than_or_equal_to": lambda data: NumericComparisonOperator(data, operator=">="),
        "is_contained_by": lambda data: IsContainedByOperator(data),
        "is_not_contained_by": lambda data: NotOperator(data, IsContainedByOperator),
        "is_contained_by_case_insensitive": lambda data: IsContainedByOperator(data, case_insensitive=True),
        "is_not_contained_by_case_insensitive": lambda data: NotOperator(
            data, lambda d: IsContainedByOperator(d, case_insensitive=True)
        ),
        "has_different_values": lambda data: HasDifferentValuesOperator(data),
        "has_same_values": lambda data: NotOperator(data, HasDifferentValuesOperator),
        "date_equal_to": lambda data: DateComparisonOperator(data, operator="="),
        "date_not_equal_to": lambda data: DateComparisonOperator(data, operator="!="),
        "date_less_than": lambda data: DateComparisonOperator(data, operator="<"),
        "date_less_than_or_equal_to": lambda data: DateComparisonOperator(data, operator="<="),
        "date_greater_than": lambda data: DateComparisonOperator(data, operator=">"),
        "date_greater_than_or_equal_to": lambda data: DateComparisonOperator(data, operator=">="),
        "is_not_unique_relationship": lambda data: IsNotUniqueRelationshipOperator(data),
        "is_unique_relationship": lambda data: NotOperator(data, IsNotUniqueRelationshipOperator),
        "present_on_multiple_rows_within": lambda data: PresentOnMultipleRowsWithinOperator(data),
        "not_present_on_multiple_rows_within": lambda data: NotOperator(data, PresentOnMultipleRowsWithinOperator),
        "prefix_is_contained_by": lambda data: IsContainedByOperator(data),
        "prefix_is_not_contained_by": lambda data: NotOperator(data, IsContainedByOperator),
        "prefix_is_contained_by_case_insensitive": lambda data: IsContainedByOperator(data, case_insensitive=True),
        "prefix_is_not_contained_by_case_insensitive": lambda data: NotOperator(
            data, lambda d: IsContainedByOperator(d, case_insensitive=True)
        ),
        "suffix_is_contained_by": lambda data: IsContainedByOperator(data),
        "suffix_is_not_contained_by": lambda data: NotOperator(data, IsContainedByOperator),
        "suffix_is_contained_by_case_insensitive": lambda data: IsContainedByOperator(data, case_insensitive=True),
        "suffix_is_not_contained_by_case_insensitive": lambda data: NotOperator(
            data, lambda d: IsContainedByOperator(d, case_insensitive=True)
        ),
        "contains": lambda data: ContainsOperator(data),
        "does_not_contain": lambda data: NotOperator(data, ContainsOperator),
        "contains_case_insensitive": lambda data: ContainsOperator(data, case_insensitive=True),
        "does_not_contain_case_insensitive": lambda data: NotOperator(
            data, lambda d: ContainsOperator(d, case_insensitive=True)
        ),
        "matches_regex": lambda data: MatchesRegexOperator(data),
        "not_matches_regex": lambda data: MatchesRegexOperator(data, invert=True),
        "matches_regex_case_insensitive": lambda data: MatchesRegexOperator(data, case_insensitive=True),
        "not_matches_regex_case_insensitive": lambda data: MatchesRegexOperator(
            data, invert=True, case_insensitive=True
        ),
        "prefix_matches_regex": lambda data: PrefixMatchesRegexOperator(data),
        "not_prefix_matches_regex": lambda data: PrefixMatchesRegexOperator(data, invert=True),
        "prefix_matches_regex_case_insensitive": lambda data: PrefixMatchesRegexOperator(data, case_insensitive=True),
        "not_prefix_matches_regex_case_insensitive": lambda data: PrefixMatchesRegexOperator(
            data, invert=True, case_insensitive=True
        ),
        "suffix_matches_regex": lambda data: SuffixMatchesRegexOperator(data),
        "not_suffix_matches_regex": lambda data: SuffixMatchesRegexOperator(data, invert=True),
        "suffix_matches_regex_case_insensitive": lambda data: SuffixMatchesRegexOperator(data, case_insensitive=True),
        "not_suffix_matches_regex_case_insensitive": lambda data: SuffixMatchesRegexOperator(
            data, invert=True, case_insensitive=True
        ),
        "starts_with": lambda data: StartsWithOperator(data),
        "not_starts_with": lambda data: NotOperator(data, StartsWithOperator),
        "ends_with": lambda data: EndsWithOperator(data),
        "not_ends_with": lambda data: NotOperator(data, EndsWithOperator),
        "equals_string_part": lambda data: EqualsStringPartOperator(data),
        "does_not_equal_string_part": lambda data: EqualsStringPartOperator(data, invert=True),
        "invalid_date": lambda data: InvalidDateOperator(data),
        "invalid_date_interval": lambda data: InvalidDateIntervalOperator(data),
        "invalid_duration": lambda data: InvalidDurationOperator(data),
        "is_complete_date": lambda data: IsCompleteDateOperator(data),
        "is_incomplete_date": lambda data: NotOperator(data, IsCompleteDateOperator),
        "is_unique_set": lambda data: IsUniqueSetOperator(data),
        "is_not_unique_set": lambda data: NotOperator(data, IsUniqueSetOperator),
        "is_ordered_set": lambda data: IsOrderedSetOperator(data),
        "is_not_ordered_set": lambda data: IsOrderedSetOperator(data, invert=True),
        "is_inconsistent_across_dataset": lambda data: IsInconsistentAcrossDatasetOperator(data),
        "conformant_value_data_type": lambda data: ConformantValueDataTypeOperator(data),
        "non_conformant_value_data_type": lambda data: NotOperator(data, ConformantValueDataTypeOperator),
        "conformant_value_length": lambda data: ConformantValueLengthOperator(data),
        "non_conformant_value_length": lambda data: NotOperator(data, ConformantValueLengthOperator),
        "suffix_equal_to": lambda data: PrefixSuffixEqualToOperator(data),
        "suffix_not_equal_to": lambda data: NotOperator(data, PrefixSuffixEqualToOperator),
        "prefix_equal_to": lambda data: PrefixSuffixEqualToOperator(data),
        "prefix_not_equal_to": lambda data: NotOperator(data, PrefixSuffixEqualToOperator),
        "has_equal_length": lambda data: StringLengthComparisonOperator(data, operator="="),
        "has_not_equal_length": lambda data: StringLengthComparisonOperator(data, operator="!="),
        "longer_than": lambda data: StringLengthComparisonOperator(data, operator=">"),
        "shorter_than_or_equal_to": lambda data: StringLengthComparisonOperator(data, operator="<="),
        "longer_than_or_equal_to": lambda data: StringLengthComparisonOperator(data, operator=">="),
        "shorter_than": lambda data: StringLengthComparisonOperator(data, operator="<"),
        "empty_within_except_last_row": lambda data: EmptyWithinExceptLastRowOperator(data),
        "non_empty_within_except_last_row": lambda data: NotOperator(data, EmptyWithinExceptLastRowOperator),
        "contains_all": lambda data: ContainsAllOperator(data),
        "not_contains_all": lambda data: NotOperator(data, ContainsAllOperator),
        "has_next_corresponding_record": lambda data: HasNextCorrespondingRecordOperator(data),
        "does_not_have_next_corresponding_record": lambda data: NotOperator(data, HasNextCorrespondingRecordOperator),
        "inconsistent_enumerated_columns": lambda data: InconsistentEnumeratedColumnsOperator(data),
        "in_enumerated_columns": lambda data: InEnumeratedColumnsOperator(data),
        "not_in_enumerated_columns": lambda data: NotOperator(data, InEnumeratedColumnsOperator),
        "references_correct_codelist": lambda data: ReferencesCorrectCodelistOperator(data),
        "does_not_reference_correct_codelist": lambda data: NotOperator(data, ReferencesCorrectCodelistOperator),
        "is_extensible_codelist_code": lambda data: IsExtensibleCodelistCodeOperator(data),
        "is_not_extensible_codelist_code": lambda data: NotOperator(data, IsExtensibleCodelistCodeOperator),
        "is_ordered_by": lambda data: IsOrderedByOperator(data),
        "is_not_ordered_by": lambda data: NotOperator(data, IsOrderedByOperator),
        "value_has_multiple_references": lambda data: ValueHasMultipleReferencesOperator(data),
        "value_does_not_have_multiple_references": lambda data: NotOperator(data, ValueHasMultipleReferencesOperator),
        "target_is_sorted_by": lambda data: TargetIsSortedByOperator(data),
        "target_is_not_sorted_by": lambda data: NotOperator(data, TargetIsSortedByOperator),
        "variable_metadata_equal_to": lambda data: VariableMetadataEqualToOperator(data),
        "variable_metadata_not_equal_to": lambda data: NotOperator(data, VariableMetadataEqualToOperator),
        "shares_at_least_one_element_with": lambda data: SharesElementsWithOperator(
            data, operation_type="at_least_one"
        ),
        "shares_exactly_one_element_with": lambda data: SharesElementsWithOperator(data, operation_type="exactly_one"),
        "shares_no_elements_with": lambda data: SharesElementsWithOperator(data, operation_type="no_elements"),
        "is_ordered_subset_of": lambda data: IsOrderedSubsetOfOperator(data),
        "is_not_ordered_subset_of": lambda data: NotOperator(data, IsOrderedSubsetOfOperator),
        "is_valid_whodrug_term_reference": lambda data: ValidExDictTermReferenceOperator(
            data, StaticTables.WHODRUG_TABLE_NAME.value
        ),
        "is_not_valid_whodrug_term_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictTermReferenceOperator(d, StaticTables.WHODRUG_TABLE_NAME.value),
        ),
        "is_valid_whodrug_code_reference": lambda data: ValidExDictCodeReferenceOperator(
            data, StaticTables.WHODRUG_TABLE_NAME.value
        ),
        "is_not_valid_whodrug_code_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeReferenceOperator(d, StaticTables.WHODRUG_TABLE_NAME.value),
        ),
        "is_valid_whodrug_level_reference": lambda data: ValidWHODrugLevelReferenceOperator(data),
        "is_not_valid_whodrug_level_reference": lambda data: NotOperator(data, ValidWHODrugLevelReferenceOperator),
        "is_valid_whodrug_code_term_pair": lambda data: ValidExDictCodeTermPairsOperator(
            data, StaticTables.WHODRUG_TABLE_NAME.value
        ),
        "is_not_valid_whodrug_code_term_pair": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeTermPairsOperator(d, StaticTables.WHODRUG_TABLE_NAME.value),
        ),
        "is_valid_meddra_term_reference": lambda data: ValidExDictTermReferenceOperator(
            data, StaticTables.MEDDRA_TABLE_NAME.value
        ),
        "is_not_valid_meddra_term_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictTermReferenceOperator(d, StaticTables.MEDDRA_TABLE_NAME.value),
        ),
        "is_valid_meddra_code_reference": lambda data: ValidExDictCodeReferenceOperator(
            data, StaticTables.MEDDRA_TABLE_NAME.value
        ),
        "is_not_valid_meddra_code_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeReferenceOperator(d, StaticTables.MEDDRA_TABLE_NAME.value),
        ),
        "is_valid_meddra_code_term_pair": lambda data: ValidExDictCodeTermPairsOperator(
            data, StaticTables.MEDDRA_TABLE_NAME.value
        ),
        "is_not_valid_meddra_code_term_pair": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeTermPairsOperator(d, StaticTables.MEDDRA_TABLE_NAME.value),
        ),
        "is_valid_medrt_term_reference": lambda data: ValidExDictTermReferenceOperator(
            data, StaticTables.MEDRT_TABLE_NAME.value
        ),
        "is_not_valid_medrt_term_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictTermReferenceOperator(d, StaticTables.MEDRT_TABLE_NAME.value),
        ),
        "is_valid_medrt_code_reference": lambda data: ValidExDictCodeReferenceOperator(
            data, StaticTables.MEDRT_TABLE_NAME.value
        ),
        "is_not_valid_medrt_code_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeReferenceOperator(d, StaticTables.MEDRT_TABLE_NAME.value),
        ),
        "is_valid_medrt_code_term_pair": lambda data: ValidExDictCodeTermPairsOperator(
            data, StaticTables.MEDRT_TABLE_NAME.value
        ),
        "is_not_valid_medrt_code_term_pair": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeTermPairsOperator(d, StaticTables.MEDRT_TABLE_NAME.value),
        ),
        "is_valid_unii_term_reference": lambda data: ValidExDictTermReferenceOperator(
            data, StaticTables.UNII_TABLE_NAME.value
        ),
        "is_not_valid_unii_term_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictTermReferenceOperator(d, StaticTables.UNII_TABLE_NAME.value),
        ),
        "is_valid_unii_code_reference": lambda data: ValidExDictCodeReferenceOperator(
            data, StaticTables.UNII_TABLE_NAME.value
        ),
        "is_not_valid_unii_code_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeReferenceOperator(d, StaticTables.UNII_TABLE_NAME.value),
        ),
        "is_valid_unii_code_term_pair": lambda data: ValidExDictCodeTermPairsOperator(
            data, StaticTables.UNII_TABLE_NAME.value
        ),
        "is_not_valid_unii_code_term_pair": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeTermPairsOperator(d, StaticTables.UNII_TABLE_NAME.value),
        ),
        "is_valid_loinc_term_reference": lambda data: ValidExDictTermReferenceOperator(
            data, StaticTables.LOINC_TABLE_NAME.value
        ),
        "is_not_valid_loinc_term_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictTermReferenceOperator(d, StaticTables.LOINC_TABLE_NAME.value),
        ),
        "is_valid_loinc_code_reference": lambda data: ValidExDictCodeReferenceOperator(
            data, StaticTables.LOINC_TABLE_NAME.value
        ),
        "is_not_valid_loinc_code_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeReferenceOperator(d, StaticTables.LOINC_TABLE_NAME.value),
        ),
        "is_valid_loinc_code_term_pair": lambda data: ValidExDictCodeTermPairsOperator(
            data, StaticTables.LOINC_TABLE_NAME.value
        ),
        "is_not_valid_loinc_code_term_pair": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeTermPairsOperator(d, StaticTables.LOINC_TABLE_NAME.value),
        ),
        "is_valid_snomed_term_reference": lambda data: ValidExDictTermReferenceOperator(
            data, StaticTables.SNOMED_TABLE_NAME.value
        ),
        "is_not_valid_snomed_term_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictTermReferenceOperator(d, StaticTables.SNOMED_TABLE_NAME.value),
        ),
        "is_valid_snomed_code_reference": lambda data: ValidExDictCodeReferenceOperator(
            data, StaticTables.SNOMED_TABLE_NAME.value
        ),
        "is_not_valid_snomed_code_reference": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeReferenceOperator(d, StaticTables.SNOMED_TABLE_NAME.value),
        ),
        "is_valid_snomed_code_term_pair": lambda data: ValidExDictCodeTermPairsOperator(
            data, StaticTables.SNOMED_TABLE_NAME.value
        ),
        "is_not_valid_snomed_code_term_pair": lambda data: NotOperator(
            data,
            lambda d: ValidExDictCodeTermPairsOperator(d, StaticTables.SNOMED_TABLE_NAME.value),
        ),
        "is_latest_available_external_dictionary_version": lambda data: IsLatestAvailableExDictVersionOperator(data),
        "is_not_latest_available_external_dictionary_version": lambda data: NotOperator(
            data, lambda d: IsLatestAvailableExDictVersionOperator(d)
        ),
        "is_substring_of": lambda data: IsSubstringOfOperator(data),
        "is_not_substring_of": lambda data: NotOperator(data, IsSubstringOfOperator),
    }

    def __init__(self, data):
        self.data = data

    @staticmethod
    def _is_missing_column_reference(operator_instance, value, variable_regex_pattern=False):
        if not isinstance(value, str) or value == "":
            return False

        if value == DATASET_NAME or value == "define.xml":
            return False

        if value in operator_instance.operation_variables:
            return False

        resolved_value = operator_instance.replace_prefix(value)
        if not isinstance(resolved_value, str) or resolved_value == "":
            return False

        if variable_regex_pattern:
            variables = getattr(operator_instance.dataset_metadata, "variables", [])
            return not any(re.fullmatch(resolved_value, variable.name) for variable in variables)

        return not operator_instance._exists(resolved_value)

    @classmethod
    def _missing_columns_for_operator(cls, operator_name, operator_instance, other_value):  # noqa: C901
        """
        Determines which columns referenced by a condition are missing from the
        dataset, so the operator can be skipped instead of producing a
        misleading result.

        `target` (and the structural grouping/ordering keys `within` and
        `ordering`) are always assumed to be column references.

        `comparator` (and `version`, used by the external-dictionary-version
        operators) is assumed to be a column reference by default,
        unless the condition sets `value_is_literal: true`,
        in which case it's treated as a literal value/collection
        and never checked for existence.

        The regex operators are excluded from the comparator check outright
        rather than requiring `value_is_literal: true` on every regex rule,
        as their `comparator` is always a raw regex pattern
        """
        if operator_name in {
            "exists",
            "not_exists",
            "is_unique_set",
            "is_not_unique_set",
            "is_unique_relationship",
            "is_not_unique_relationship",
            "is_ordered_subset_of",
            "is_not_ordered_subset_of",
        }:
            return []

        if not isinstance(other_value, dict):
            return []

        missing_columns = []
        variable_regex_pattern = other_value.get("variable_regex_pattern", False)

        def _check(value, regex=False):
            if isinstance(value, list):
                for item in value:
                    _check(item)
                return
            if cls._is_missing_column_reference(operator_instance, value, variable_regex_pattern=regex):
                missing_columns.append(value)

        _check(other_value.get("target"), regex=variable_regex_pattern)
        _check(other_value.get("within"))
        _check(other_value.get("ordering"))

        if operator_name in {"target_is_sorted_by", "target_is_not_sorted_by"}:
            # comparator is a list of {"name": ..., "sort_order": ..., "null_position": ...}
            comparator = other_value.get("comparator")
            if isinstance(comparator, list):
                for item in comparator:
                    _check(item.get("name") if isinstance(item, dict) else item)
            return missing_columns

        if operator_name == "is_inconsistent_across_dataset":
            return missing_columns

        regex_pattern_operators = {
            "matches_regex",
            "not_matches_regex",
            "matches_regex_case_insensitive",
            "not_matches_regex_case_insensitive",
            "prefix_matches_regex",
            "not_prefix_matches_regex",
            "prefix_matches_regex_case_insensitive",
            "not_prefix_matches_regex_case_insensitive",
            "suffix_matches_regex",
            "not_suffix_matches_regex",
            "suffix_matches_regex_case_insensitive",
            "not_suffix_matches_regex_case_insensitive",
        }
        if operator_name in regex_pattern_operators:
            return missing_columns

        value_is_literal = other_value.get("value_is_literal", False)
        comparator_is_column = other_value.get("value_is_reference", False) or not value_is_literal

        if comparator_is_column:
            _check(other_value.get("comparator"))
            _check(other_value.get("version"))

        return missing_columns

    def evaluate(self, operator_name: str, other_value) -> str:
        """
        SQL boolean condition for one check over the rule's source rows (aliased co).
        A missing value (NULL) counts as False.
        A check on a column the dataset doesn't have is False.
        """
        operator_factory = self._operator_map.get(operator_name)
        if operator_factory is None:
            raise ValueError(f"Unknown check operator: {operator_name}")

        operator = operator_factory(self.data)
        try:
            missing_columns = self._missing_columns_for_operator(operator_name, operator, other_value)
            if missing_columns:
                logger.info(
                    f"Operator '{operator_name}' cannot be executed because the following columns are missing: {missing_columns}"  # noqa
                )
                return "FALSE"
            condition = operator.execute_operator(other_value)
        except Exception as error:
            logger.error(f"Error in {operator_name}: {error}, traceback: {traceback.format_exc()}")
            raise SqlOperatorError(original_exception=error, operator_name=operator_name) from error

        if condition is None:
            return "FALSE"
        # SQL NULL here would otherwise poison the surrounding AND/OR/NOT.
        # e.g. NOT NULL is still NULL so a negated check on a missing value would never fire.
        # Hence... we coalesce the condition to FALSE to ensure missing values are treated as not met.
        return f"COALESCE(({condition}), FALSE)"

    def _assert_valid_value_and_cast(self, value):
        """Shared method for value validation and casting."""
        return value
