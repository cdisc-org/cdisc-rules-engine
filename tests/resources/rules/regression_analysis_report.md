# CDISC Rules Engine Regression Analysis

=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 28 (3.7%)
- **Clean rules**: 734 (96.3%)

**Error Breakdown by Category:**

- Rules with **operator errors**: 0
- Rules with **operation errors**: 6
- Rules with **other errors**: 22

## Missing Operators

No missing operator errors found!

## Missing Operations (5 operations, 36 total failures across 6 rule occurrences)

1.  **variable_count**: 20 failures across 2 rules
2.  **get_model_column_order**: 6 failures across 1 rules
3.  **domain_is_custom**: 4 failures across 1 rules
4.  **get_parent_model_column_order**: 4 failures across 1 rules
5.  **valid_codelist_dates**: 2 failures across 1 rules

## Execution Errors by Type (20 unique error types, 126 total failures across 29 rule occurrences)

1.  **An unknown exception has occurred**: 36 failures across 6 rules
2.  **SQL error in not_contains_all operator**: 16 failures across 2 rules
3.  **SQL error in is_incomplete_date operator**: 12 failures across 2 rules
4.  **SQL error in not_equal_to operator**: 6 failures across 2 rules
5.  **SQL error in does_not_contain operator**: 4 failures across 2 rules
6.  **Rule format error**: 15 failures across 1 rules
7.  **SQL error in is_contained_by operator**: 8 failures across 1 rules
8.  **SQL error in does_not_equal_string_part operator**: 6 failures across 1 rules
9.  **SQL error in less_than_or_equal_to operator**: 3 failures across 1 rules
10. **SQL error in not_matches_regex operator**: 2 failures across 1 rules
11. **SQL error in prefix_matches_regex operator**: 2 failures across 1 rules
12. **SQL error in is_not_contained_by operator**: 2 failures across 1 rules
13. **SQL error in sqldatasetcolumnorderoperation operation**: 2 failures across 1 rules
14. **SQL error in sqldaydatavalidatoroperation operation**: 2 failures across 1 rules
15. **SQL error in date_less_than operator**: 2 failures across 1 rules

## SQL vs Old Engine Discrepancies
