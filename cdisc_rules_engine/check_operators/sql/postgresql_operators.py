from business_rules.fields import FIELD_DATAFRAME
from business_rules.operators import BaseType, type_operator

from cdisc_rules_engine.check_operators.sql.not_operator import NotOperator

from .base_sql_operator import log_operator_execution
from .exists_operator import ExistsOperator
from .equal_to_operator import EqualToOperator
from .empty_operator import EmptyOperator
from .numeric_comparison_operator import NumericComparisonOperator
from .is_contained_by_operator import IsContainedByOperator
from .has_different_values_operator import HasDifferentValuesOperator
from .date_comparison_operator import DateComparisonOperator
from .is_contained_by_case_insensitive_operator import IsContainedByCaseInsensitiveOperator
from .is_not_unique_relationship_operator import IsNotUniqueRelationshipOperator
from .present_on_multiple_rows_within_operator import PresentOnMultipleRowsWithinOperator
from .prefix_is_contained_by_operator import PrefixIsContainedByOperator
from .suffix_is_contained_by_operator import SuffixIsContainedByOperator
from .contains_operator import ContainsOperator
from .contains_case_insensitive_operator import ContainsCaseInsensitiveOperator
from .matches_regex_operator import MatchesRegexOperator
from .not_matches_regex_operator import NotMatchesRegexOperator
from .prefix_matches_regex_operator import PrefixMatchesRegexOperator
from .not_prefix_matches_regex_operator import NotPrefixMatchesRegexOperator
from .suffix_matches_regex_operator import SuffixMatchesRegexOperator
from .not_suffix_matches_regex_operator import NotSuffixMatchesRegexOperator
from .starts_with_operator import StartsWithOperator
from .ends_with_operator import EndsWithOperator
from .equals_string_part_operator import EqualsStringPartOperator
from .invalid_date_operator import InvalidDateOperator
from .invalid_duration_operator import InvalidDurationOperator
from .is_complete_date_operator import IsCompleteDateOperator
from .is_unique_set_operator import IsUniqueSetOperator
from .is_ordered_set_operator import IsOrderedSetOperator
from .is_inconsistent_across_dataset_operator import IsInconsistentAcrossDatasetOperator
from .conformant_value_data_type_operator import ConformantValueDataTypeOperator
from .conformant_value_length_operator import ConformantValueLengthOperator
from .suffix_equal_to_operator import SuffixEqualToOperator
from .prefix_equal_to_operator import PrefixEqualToOperator
from .has_equal_length_operator import HasEqualLengthOperator
from .longer_than_operator import LongerThanOperator
from .longer_than_or_equal_to_operator import LongerThanOrEqualToOperator
from .empty_within_except_last_row_operator import EmptyWithinExceptLastRowOperator
from .contains_all_operator import ContainsAllOperator
from .has_next_corresponding_record_operator import HasNextCorrespondingRecordOperator
from .inconsistent_enumerated_columns_operator import InconsistentEnumeratedColumnsOperator
from .references_correct_codelist_operator import ReferencesCorrectCodelistOperator
from .is_ordered_by_operator import IsOrderedByOperator
from .value_has_multiple_references_operator import ValueHasMultipleReferencesOperator
from .target_is_sorted_by_operator import TargetIsSortedByOperator
from .variable_metadata_equal_to_operator import VariableMetadataEqualToOperator
from .shares_at_least_one_element_with_operator import SharesAtLeastOneElementWithOperator
from .shares_exactly_one_element_with_operator import SharesExactlyOneElementWithOperator
from .shares_no_elements_with_operator import SharesNoElementsWithOperator
from .is_ordered_subset_of_operator import IsOrderedSubsetOfOperator


class PostgresQLOperators(BaseType):
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
        "is_contained_by_case_insensitive": lambda data: IsContainedByCaseInsensitiveOperator(data),
        "is_not_contained_by_case_insensitive": lambda data: NotOperator(data, IsContainedByCaseInsensitiveOperator),
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
        "prefix_is_contained_by": lambda data: PrefixIsContainedByOperator(data),
        "prefix_is_not_contained_by": lambda data: NotOperator(data, PrefixIsContainedByOperator),
        "suffix_is_contained_by": lambda data: SuffixIsContainedByOperator(data),
        "suffix_is_not_contained_by": lambda data: NotOperator(data, SuffixIsContainedByOperator),
        "contains": lambda data: ContainsOperator(data),
        "does_not_contain": lambda data: NotOperator(data, ContainsOperator),
        "contains_case_insensitive": lambda data: ContainsCaseInsensitiveOperator(data),
        "does_not_contain_case_insensitive": lambda data: NotOperator(data, ContainsCaseInsensitiveOperator),
        "matches_regex": lambda data: MatchesRegexOperator(data),
        "not_matches_regex": lambda data: NotMatchesRegexOperator(data),  # TODO check if this can use Not Operator
        "prefix_matches_regex": lambda data: PrefixMatchesRegexOperator(data),
        "not_prefix_matches_regex": lambda data: NotPrefixMatchesRegexOperator(data),
        "suffix_matches_regex": lambda data: SuffixMatchesRegexOperator(data),
        "not_suffix_matches_regex": lambda data: NotSuffixMatchesRegexOperator(data),
        "starts_with": lambda data: StartsWithOperator(data),
        "ends_with": lambda data: EndsWithOperator(data),
        "equals_string_part": lambda data: EqualsStringPartOperator(data),
        "does_not_equal_string_part": lambda data: NotOperator(data, EqualsStringPartOperator),
        "invalid_date": lambda data: InvalidDateOperator(data),
        "invalid_duration": lambda data: InvalidDurationOperator(data),
        "is_complete_date": lambda data: IsCompleteDateOperator(data),
        "is_incomplete_date": lambda data: NotOperator(data, IsCompleteDateOperator),
        "is_unique_set": lambda data: IsUniqueSetOperator(data),
        "is_not_unique_set": lambda data: NotOperator(data, IsUniqueSetOperator),
        "is_ordered_set": lambda data: IsOrderedSetOperator(data),
        "is_not_ordered_set": lambda data: NotOperator(data, IsOrderedByOperator),
        "is_inconsistent_across_dataset": lambda data: IsInconsistentAcrossDatasetOperator(data),
        "conformant_value_data_type": lambda data: ConformantValueDataTypeOperator(data),
        "non_conformant_value_data_type": lambda data: NotOperator(data, ConformantValueDataTypeOperator),
        "conformant_value_length": lambda data: ConformantValueLengthOperator(data),
        "non_conformant_value_length": lambda data: NotOperator(data, ConformantValueLengthOperator),
        "suffix_equal_to": lambda data: SuffixEqualToOperator(data),
        "suffix_not_equal_to": lambda data: NotOperator(data, SuffixEqualToOperator),
        "prefix_equal_to": lambda data: PrefixEqualToOperator(data),
        "prefix_not_equal_to": lambda data: NotOperator(data, PrefixEqualToOperator),
        "has_equal_length": lambda data: HasEqualLengthOperator(data),
        "has_not_equal_length": lambda data: NotOperator(data, HasEqualLengthOperator),
        "longer_than": lambda data: LongerThanOperator(data),
        "shorter_than_or_equal_to": lambda data: NotOperator(data, LongerThanOperator),
        "longer_than_or_equal_to": lambda data: LongerThanOrEqualToOperator(data),
        "shorter_than": lambda data: NotOperator(data, LongerThanOrEqualToOperator),
        "empty_within_except_last_row": lambda data: EmptyWithinExceptLastRowOperator(data),
        "non_empty_within_except_last_row": lambda data: NotOperator(data, EmptyWithinExceptLastRowOperator),
        "contains_all": lambda data: ContainsAllOperator(data),
        "not_contains_all": lambda data: NotOperator(data, ContainsAllOperator),
        "has_next_corresponding_record": lambda data: HasNextCorrespondingRecordOperator(data),
        "does_not_have_next_corresponding_record": lambda data: NotOperator(data, HasNextCorrespondingRecordOperator),
        "inconsistent_enumerated_columns": lambda data: InconsistentEnumeratedColumnsOperator(data),
        "references_correct_codelist": lambda data: ReferencesCorrectCodelistOperator(data),
        "does_not_reference_correct_codelist": lambda data: NotOperator(data, ReferencesCorrectCodelistOperator),
        "is_ordered_by": lambda data: IsOrderedByOperator(data),
        "is_not_ordered_by": lambda data: NotOperator(data, IsOrderedByOperator),
        "value_has_multiple_references": lambda data: ValueHasMultipleReferencesOperator(data),
        "value_does_not_have_multiple_references": lambda data: NotOperator(data, ValueHasMultipleReferencesOperator),
        "target_is_sorted_by": lambda data: TargetIsSortedByOperator(data),
        "target_is_not_sorted_by": lambda data: NotOperator(data, TargetIsSortedByOperator),
        "variable_metadata_equal_to": lambda data: VariableMetadataEqualToOperator(data),
        "variable_metadata_not_equal_to": lambda data: NotOperator(data, VariableMetadataEqualToOperator),
        "shares_at_least_one_element_with": lambda data: SharesAtLeastOneElementWithOperator(data),
        "shares_exactly_one_element_with": lambda data: SharesExactlyOneElementWithOperator(data),
        "shares_no_elements_with": lambda data: SharesNoElementsWithOperator(data),
        "is_ordered_subset_of": lambda data: IsOrderedSubsetOfOperator(data),
        "is_not_ordered_subset_of": lambda data: NotOperator(data, IsOrderedSubsetOfOperator),
    }

    def __init__(self, data):
        self.data = data

    def __getattr__(self, name):
        """
        Dynamically create and cache an operator method on its first access.
        Handles both simple operators and wrapped operators.
        """
        if name in self._operator_map:
            operator_factory = self._operator_map[name]
            operator_instance = operator_factory(self.data)

            # Define the method with the necessary decorators
            @log_operator_execution
            @type_operator(FIELD_DATAFRAME)
            def operator_method(self, other_value):
                return operator_instance.execute_operator(other_value)

            # Cache the new method on the instance
            bound_method = operator_method.__get__(self, type(self))
            setattr(self, name, bound_method)
            return bound_method

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def _assert_valid_value_and_cast(self, value):
        """Shared method for value validation and casting."""
        return value
