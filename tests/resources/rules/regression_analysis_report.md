# CDISC Rules Engine Regression Analysis

=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 30 (3.9%)
- **Clean rules**: 732 (96.1%)

**Error Breakdown by Category:**

- Rules with **operator errors**: 1
- Rules with **operation errors**: 13
- Rules with **other errors**: 16

## Missing Operators (1 operators, 4 total failures across 1 rule occurrences)

1.  **target_is_sorted_by**: 4 failures across 1 rules

## Missing Operations (10 operations, 79 total failures across 13 rule occurrences)

1.  **variable_count**: 20 failures across 2 rules
2.  **required_variables**: 16 failures across 2 rules
3.  **study_domains**: 8 failures across 2 rules
4.  **expected_variables**: 10 failures across 1 rules
5.  **extract_metadata**: 6 failures across 1 rules
6.  **get_model_column_order**: 6 failures across 1 rules
7.  **get_model_filtered_variables**: 6 failures across 1 rules
8.  **get_parent_model_column_order**: 4 failures across 1 rules
9.  **valid_codelist_dates**: 2 failures across 1 rules
10. **domain_is_custom**: 1 failures across 1 rules

## Execution Errors by Type (14 unique error types, 137 total failures across 30 rule occurrences)

1.  **An unknown exception has occurred**: 82 failures across 15 rules
2.  **SQL error in is_incomplete_date operator**: 12 failures across 2 rules
3.  **SQL error in does_not_contain operator**: 4 failures across 2 rules
4.  **Rule format error**: 15 failures across 1 rules
5.  **SQL error in not_equal_to operator**: 4 failures across 1 rules
6.  **SQL error in target_is_not_sorted_by operator**: 4 failures across 1 rules
7.  **SQL error in less_than_or_equal_to operator**: 3 failures across 1 rules
8.  **SQL error in not_matches_regex operator**: 2 failures across 1 rules
9.  **SQL error in is_not_contained_by operator**: 2 failures across 1 rules
10. **SQL error in sqldaydatavalidatoroperation operation**: 2 failures across 1 rules
11. **SQL error in date_less_than operator**: 2 failures across 1 rules
12. **SQL error in date_greater_than operator**: 2 failures across 1 rules
13. **SQL error in matches_regex operator**: 2 failures across 1 rules
14. **SQL error in is_not_unique_set operator**: 1 failures across 1 rules

## SQL vs Old Engine Discrepancies
