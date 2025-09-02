from business_rules.fields import FIELD_DATAFRAME
from business_rules.operators import BaseType, type_operator

from .base_sql_operator import log_operator_execution
from .exists_operator import ExistsOperator
from .not_exists_operator import NotExistsOperator
from .equal_to_operator import EqualToOperator
from .not_equal_to_operator import NotEqualToOperator
from .equal_to_case_insensitive_operator import EqualToCaseInsensitiveOperator
from .not_equal_to_case_insensitive_operator import NotEqualToCaseInsensitiveOperator
from .empty_operator import EmptyOperator
from .non_empty_operator import NonEmptyOperator
from .less_than_operator import LessThanOperator
from .greater_than_operator import GreaterThanOperator
from .less_than_or_equal_to_operator import LessThanOrEqualToOperator
from .greater_than_or_equal_to_operator import GreaterThanOrEqualToOperator
from .is_contained_by_operator import IsContainedByOperator
from .is_not_contained_by_operator import IsNotContainedByOperator
from .has_different_values_operator import HasDifferentValuesOperator
from .has_same_values_operator import HasSameValuesOperator
from .date_equal_to_operator import DateEqualToOperator
from .date_not_equal_to_operator import DateNotEqualToOperator
from .date_less_than_operator import DateLessThanOperator
from .date_less_than_or_equal_to_operator import DateLessThanOrEqualToOperator
from .date_greater_than_operator import DateGreaterThanOperator
from .date_greater_than_or_equal_to_operator import DateGreaterThanOrEqualToOperator
from .is_contained_by_case_insensitive_operator import IsContainedByCaseInsensitiveOperator
from .is_not_contained_by_case_insensitive_operator import IsNotContainedByCaseInsensitiveOperator
from .is_not_unique_relationship_operator import IsNotUniqueRelationshipOperator
from .is_unique_relationship_operator import IsUniqueRelationshipOperator
from .present_on_multiple_rows_within_operator import PresentOnMultipleRowsWithinOperator
from .not_present_on_multiple_rows_within_operator import NotPresentOnMultipleRowsWithinOperator
from .prefix_is_contained_by_operator import PrefixIsContainedByOperator
from .prefix_is_not_contained_by_operator import PrefixIsNotContainedByOperator
from .suffix_is_contained_by_operator import SuffixIsContainedByOperator
from .suffix_is_not_contained_by_operator import SuffixIsNotContainedByOperator
from .contains_operator import ContainsOperator
from .does_not_contain_operator import DoesNotContainOperator
from .contains_case_insensitive_operator import ContainsCaseInsensitiveOperator
from .does_not_contain_case_insensitive_operator import DoesNotContainCaseInsensitiveOperator
from .matches_regex_operator import MatchesRegexOperator
from .not_matches_regex_operator import NotMatchesRegexOperator
from .prefix_matches_regex_operator import PrefixMatchesRegexOperator
from .not_prefix_matches_regex_operator import NotPrefixMatchesRegexOperator
from .suffix_matches_regex_operator import SuffixMatchesRegexOperator
from .not_suffix_matches_regex_operator import NotSuffixMatchesRegexOperator
from .starts_with_operator import StartsWithOperator
from .ends_with_operator import EndsWithOperator
from .equals_string_part_operator import EqualsStringPartOperator
from .does_not_equal_string_part_operator import DoesNotEqualStringPartOperator
from .invalid_date_operator import InvalidDateOperator
from .invalid_duration_operator import InvalidDurationOperator
from .is_complete_date_operator import IsCompleteDateOperator
from .is_incomplete_date_operator import IsIncompleteDateOperator
from .is_unique_set_operator import IsUniqueSetOperator
from .is_not_unique_set_operator import IsNotUniqueSetOperator
from .is_ordered_set_operator import IsOrderedSetOperator
from .is_not_ordered_set_operator import IsNotOrderedSetOperator
from .is_inconsistent_across_dataset_operator import IsInconsistentAcrossDatasetOperator
from .conformant_value_data_type_operator import ConformantValueDataTypeOperator
from .non_conformant_value_data_type_operator import NonConformantValueDataTypeOperator
from .conformant_value_length_operator import ConformantValueLengthOperator
from .non_conformant_value_length_operator import NonConformantValueLengthOperator
from .suffix_equal_to_operator import SuffixEqualToOperator
from .suffix_not_equal_to_operator import SuffixNotEqualToOperator
from .prefix_equal_to_operator import PrefixEqualToOperator
from .prefix_not_equal_to_operator import PrefixNotEqualToOperator
from .has_equal_length_operator import HasEqualLengthOperator
from .has_not_equal_length_operator import HasNotEqualLengthOperator
from .longer_than_operator import LongerThanOperator
from .longer_than_or_equal_to_operator import LongerThanOrEqualToOperator
from .shorter_than_operator import ShorterThanOperator
from .shorter_than_or_equal_to_operator import ShorterThanOrEqualToOperator
from .empty_within_except_last_row_operator import EmptyWithinExceptLastRowOperator
from .non_empty_within_except_last_row_operator import NonEmptyWithinExceptLastRowOperator
from .contains_all_operator import ContainsAllOperator
from .not_contains_all_operator import NotContainsAllOperator
from .has_next_corresponding_record_operator import HasNextCorrespondingRecordOperator
from .does_not_have_next_corresponding_record_operator import DoesNotHaveNextCorrespondingRecordOperator
from .inconsistent_enumerated_columns_operator import InconsistentEnumeratedColumnsOperator
from .references_correct_codelist_operator import ReferencesCorrectCodelistOperator
from .does_not_reference_correct_codelist_operator import DoesNotReferenceCorrectCodelistOperator
from .is_ordered_by_operator import IsOrderedByOperator
from .is_not_ordered_by_operator import IsNotOrderedByOperator
from .value_has_multiple_references_operator import ValueHasMultipleReferencesOperator
from .value_does_not_have_multiple_references_operator import ValueDoesNotHaveMultipleReferencesOperator
from .target_is_sorted_by_operator import TargetIsSortedByOperator
from .target_is_not_sorted_by_operator import TargetIsNotSortedByOperator
from .variable_metadata_equal_to_operator import VariableMetadataEqualToOperator
from .variable_metadata_not_equal_to_operator import VariableMetadataNotEqualToOperator
from .shares_at_least_one_element_with_operator import SharesAtLeastOneElementWithOperator
from .shares_exactly_one_element_with_operator import SharesExactlyOneElementWithOperator
from .shares_no_elements_with_operator import SharesNoElementsWithOperator
from .is_ordered_subset_of_operator import IsOrderedSubsetOfOperator
from .is_not_ordered_subset_of_operator import IsNotOrderedSubsetOfOperator


class PostgresQLOperators(BaseType):
    """
    Main SQL operators class with dynamic method registration.

    This class uses dynamic registration to combine functionality from individual
    operator classes, maintaining compatibility with the business rules framework
    while providing operations-like modularity.
    """

    name = "dataframe"

    def __init__(self, data):
        self.data = data
        self._register_operators()

    def _register_operators(self):
        """Dynamically register all operator methods."""
        operator_map = {
            "exists": ExistsOperator,
            "not_exists": NotExistsOperator,
            "equal_to": EqualToOperator,
            "not_equal_to": NotEqualToOperator,
            "equal_to_case_insensitive": EqualToCaseInsensitiveOperator,
            "not_equal_to_case_insensitive": NotEqualToCaseInsensitiveOperator,
            "empty": EmptyOperator,
            "non_empty": NonEmptyOperator,
            "less_than": LessThanOperator,
            "greater_than": GreaterThanOperator,
            "less_than_or_equal_to": LessThanOrEqualToOperator,
            "greater_than_or_equal_to": GreaterThanOrEqualToOperator,
            "is_contained_by": IsContainedByOperator,
            "is_not_contained_by": IsNotContainedByOperator,
            "has_different_values": HasDifferentValuesOperator,
            "has_same_values": HasSameValuesOperator,
            "date_equal_to": DateEqualToOperator,
            "date_not_equal_to": DateNotEqualToOperator,
            "date_less_than": DateLessThanOperator,
            "date_less_than_or_equal_to": DateLessThanOrEqualToOperator,
            "date_greater_than": DateGreaterThanOperator,
            "date_greater_than_or_equal_to": DateGreaterThanOrEqualToOperator,
            "is_contained_by_case_insensitive": IsContainedByCaseInsensitiveOperator,
            "is_not_contained_by_case_insensitive": IsNotContainedByCaseInsensitiveOperator,
            "is_not_unique_relationship": IsNotUniqueRelationshipOperator,
            "is_unique_relationship": IsUniqueRelationshipOperator,
            "present_on_multiple_rows_within": PresentOnMultipleRowsWithinOperator,
            "not_present_on_multiple_rows_within": NotPresentOnMultipleRowsWithinOperator,
            "prefix_is_contained_by": PrefixIsContainedByOperator,
            "prefix_is_not_contained_by": PrefixIsNotContainedByOperator,
            "suffix_is_contained_by": SuffixIsContainedByOperator,
            "suffix_is_not_contained_by": SuffixIsNotContainedByOperator,
            "contains": ContainsOperator,
            "does_not_contain": DoesNotContainOperator,
            "contains_case_insensitive": ContainsCaseInsensitiveOperator,
            "does_not_contain_case_insensitive": DoesNotContainCaseInsensitiveOperator,
            "matches_regex": MatchesRegexOperator,
            "not_matches_regex": NotMatchesRegexOperator,
            "prefix_matches_regex": PrefixMatchesRegexOperator,
            "not_prefix_matches_regex": NotPrefixMatchesRegexOperator,
            "suffix_matches_regex": SuffixMatchesRegexOperator,
            "not_suffix_matches_regex": NotSuffixMatchesRegexOperator,
            "starts_with": StartsWithOperator,
            "ends_with": EndsWithOperator,
            "equals_string_part": EqualsStringPartOperator,
            "does_not_equal_string_part": DoesNotEqualStringPartOperator,
            "invalid_date": InvalidDateOperator,
            "invalid_duration": InvalidDurationOperator,
            "is_complete_date": IsCompleteDateOperator,
            "is_incomplete_date": IsIncompleteDateOperator,
            "is_unique_set": IsUniqueSetOperator,
            "is_not_unique_set": IsNotUniqueSetOperator,
            "is_ordered_set": IsOrderedSetOperator,
            "is_not_ordered_set": IsNotOrderedSetOperator,
            "is_inconsistent_across_dataset": IsInconsistentAcrossDatasetOperator,
            "conformant_value_data_type": ConformantValueDataTypeOperator,
            "non_conformant_value_data_type": NonConformantValueDataTypeOperator,
            "conformant_value_length": ConformantValueLengthOperator,
            "non_conformant_value_length": NonConformantValueLengthOperator,
            "suffix_equal_to": SuffixEqualToOperator,
            "suffix_not_equal_to": SuffixNotEqualToOperator,
            "prefix_equal_to": PrefixEqualToOperator,
            "prefix_not_equal_to": PrefixNotEqualToOperator,
            "has_equal_length": HasEqualLengthOperator,
            "has_not_equal_length": HasNotEqualLengthOperator,
            "longer_than": LongerThanOperator,
            "longer_than_or_equal_to": LongerThanOrEqualToOperator,
            "shorter_than": ShorterThanOperator,
            "shorter_than_or_equal_to": ShorterThanOrEqualToOperator,
            "empty_within_except_last_row": EmptyWithinExceptLastRowOperator,
            "non_empty_within_except_last_row": NonEmptyWithinExceptLastRowOperator,
            "contains_all": ContainsAllOperator,
            "not_contains_all": NotContainsAllOperator,
            "has_next_corresponding_record": HasNextCorrespondingRecordOperator,
            "does_not_have_next_corresponding_record": DoesNotHaveNextCorrespondingRecordOperator,
            "inconsistent_enumerated_columns": InconsistentEnumeratedColumnsOperator,
            "references_correct_codelist": ReferencesCorrectCodelistOperator,
            "does_not_reference_correct_codelist": DoesNotReferenceCorrectCodelistOperator,
            "is_ordered_by": IsOrderedByOperator,
            "is_not_ordered_by": IsNotOrderedByOperator,
            "value_has_multiple_references": ValueHasMultipleReferencesOperator,
            "value_does_not_have_multiple_references": ValueDoesNotHaveMultipleReferencesOperator,
            "target_is_sorted_by": TargetIsSortedByOperator,
            "target_is_not_sorted_by": TargetIsNotSortedByOperator,
            "variable_metadata_equal_to": VariableMetadataEqualToOperator,
            "variable_metadata_not_equal_to": VariableMetadataNotEqualToOperator,
            "shares_at_least_one_element_with": SharesAtLeastOneElementWithOperator,
            "shares_exactly_one_element_with": SharesExactlyOneElementWithOperator,
            "shares_no_elements_with": SharesNoElementsWithOperator,
            "is_ordered_subset_of": IsOrderedSubsetOfOperator,
            "is_not_ordered_subset_of": IsNotOrderedSubsetOfOperator,
        }

        for method_name, operator_class in operator_map.items():
            self._register_operator_method(method_name, operator_class)

    def _register_operator_method(self, method_name, operator_class):
        """Register a single operator method with proper decorators."""

        @log_operator_execution
        @type_operator(FIELD_DATAFRAME)
        def operator_method(self, other_value):
            operator_instance = operator_class(self.data)
            return operator_instance.execute_operator(other_value)

        # Properly bind the method to this instance
        setattr(self, method_name, operator_method.__get__(self, type(self)))

    # Add any shared methods that operators might need to access
    def _assert_valid_value_and_cast(self, value):
        """Shared method for value validation and casting."""
        return value
