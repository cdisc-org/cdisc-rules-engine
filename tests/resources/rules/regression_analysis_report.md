# CDISC Rules Engine Regression Analysis

=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 31 (4.1%)
- **Clean rules**: 731 (95.9%)

**Error Breakdown by Category:**

- Rules with **operator errors**: 0
- Rules with **operation errors**: 10
- Rules with **other errors**: 21

## Missing Operators

No missing operator errors found!

## Missing Operations (8 operations, 58 total failures across 10 rule occurrences)

1.  **variable_count**: 20 failures across 2 rules
2.  **required_variables**: 16 failures across 2 rules
3.  **get_column_order_from_dataset**: 6 failures across 1 rules
4.  **get_model_column_order**: 5 failures across 1 rules
5.  **extract_metadata**: 4 failures across 1 rules
6.  **get_parent_model_column_order**: 4 failures across 1 rules
7.  **valid_codelist_dates**: 2 failures across 1 rules
8.  **domain_is_custom**: 1 failures across 1 rules

## Execution Errors by Type (11 unique error types, 129 total failures across 31 rule occurrences)

1.  **An unknown exception has occurred**: 85 failures across 18 rules
2.  **SQL error in is_incomplete_date operator**: 10 failures across 2 rules
3.  **SQL error in not_matches_regex operator**: 4 failures across 2 rules
4.  **SQL error in does_not_contain operator**: 4 failures across 2 rules
5.  **Rule format error**: 15 failures across 1 rules
6.  **SQL error in sqldaydatavalidatoroperation operation**: 2 failures across 1 rules
7.  **SQL error in date_less_than operator**: 2 failures across 1 rules
8.  **SQL error in less_than_or_equal_to operator**: 2 failures across 1 rules
9.  **SQL error in date_greater_than operator**: 2 failures across 1 rules
10. **SQL error in matches_regex operator**: 2 failures across 1 rules
11. **SQL error in is_not_contained_by operator**: 1 failures across 1 rules

## SQL vs Old Engine Discrepancies
