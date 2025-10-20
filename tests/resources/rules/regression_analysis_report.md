# CDISC Rules Engine Regression Analysis

=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 36 (4.7%)
- **Clean rules**: 726 (95.3%)

**Error Breakdown by Category:**

- Rules with **operator errors**: 0
- Rules with **operation errors**: 12
- Rules with **other errors**: 24

## Missing Operators

No missing operator errors found!

## Missing Operations (9 operations, 76 total failures across 12 rule occurrences)

1.  **variable_count**: 20 failures across 2 rules
2.  **required_variables**: 16 failures across 2 rules
3.  **study_domains**: 8 failures across 2 rules
4.  **expected_variables**: 10 failures across 1 rules
5.  **extract_metadata**: 6 failures across 1 rules
6.  **get_model_column_order**: 6 failures across 1 rules
7.  **domain_is_custom**: 4 failures across 1 rules
8.  **get_parent_model_column_order**: 4 failures across 1 rules
9.  **valid_codelist_dates**: 2 failures across 1 rules

## Execution Errors by Type (15 unique error types, 167 total failures across 37 rule occurrences)

1.  **An unknown exception has occurred**: 110 failures across 20 rules
2.  **SQL error in is_incomplete_date operator**: 12 failures across 2 rules
3.  **SQL error in not_equal_to operator**: 7 failures across 2 rules
4.  **SQL error in does_not_contain operator**: 4 failures across 2 rules
5.  **Rule format error**: 15 failures across 1 rules
6.  **SQL error in less_than_or_equal_to operator**: 3 failures across 1 rules
7.  **SQL error in not_matches_regex operator**: 2 failures across 1 rules
8.  **SQL error in prefix_matches_regex operator**: 2 failures across 1 rules
9.  **SQL error in is_not_contained_by operator**: 2 failures across 1 rules
10. **SQL error in sqldaydatavalidatoroperation operation**: 2 failures across 1 rules
11. **SQL error in date_less_than operator**: 2 failures across 1 rules
12. **SQL error in date_greater_than operator**: 2 failures across 1 rules
13. **SQL error in matches_regex operator**: 2 failures across 1 rules
14. **Column not found in data**: 1 failures across 1 rules
15. **SQL error in is_not_unique_set operator**: 1 failures across 1 rules

## SQL vs Old Engine Discrepancies
