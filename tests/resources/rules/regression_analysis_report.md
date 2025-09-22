# CDISC Rules Engine Regression Analysis

=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 36 (4.7%)
- **Clean rules**: 726 (95.3%)

**Error Breakdown by Category:**

- Rules with **operator errors**: 1
- Rules with **operation errors**: 14
- Rules with **other errors**: 21

## Missing Operators (1 operators, 2 total failures across 1 rule occurrences)

1.  **empty_within_except_last_row**: 2 failures across 1 rules

## Missing Operations (10 operations, 58 total failures across 14 rule occurrences)

1.  **variable_count**: 20 failures across 2 rules
2.  **domain_label**: 8 failures across 2 rules
3.  **max_date**: 5 failures across 2 rules
4.  **min_date**: 3 failures across 2 rules
5.  **get_column_order_from_dataset**: 6 failures across 1 rules
6.  **get_model_column_order**: 5 failures across 1 rules
7.  **extract_metadata**: 4 failures across 1 rules
8.  **get_parent_model_column_order**: 4 failures across 1 rules
9.  **valid_codelist_dates**: 2 failures across 1 rules
10. **domain_is_custom**: 1 failures across 1 rules

## Execution Errors by Type (13 unique error types, 147 total failures across 37 rule occurrences)

1.  **An unknown exception has occurred**: 80 failures across 19 rules
2.  **SQL error in not_equal_to operator**: 18 failures across 3 rules
3.  **SQL error in is_incomplete_date operator**: 10 failures across 2 rules
4.  **SQL error in not_matches_regex operator**: 4 failures across 2 rules
5.  **SQL error in does_not_contain operator**: 4 failures across 2 rules
6.  **SQL error in less_than_or_equal_to operator**: 4 failures across 2 rules
7.  **Rule format error**: 15 failures across 1 rules
8.  **SQL error in empty_within_except_last_row operator**: 2 failures across 1 rules
9.  **SQL error in sqldaydatavalidatoroperation operation**: 2 failures across 1 rules
10. **SQL error in date_less_than operator**: 2 failures across 1 rules
11. **SQL error in date_greater_than operator**: 2 failures across 1 rules
12. **SQL error in matches_regex operator**: 2 failures across 1 rules
13. **SQL error in sqldistinctoperation operation**: 2 failures across 1 rules

## SQL vs Old Engine Discrepancies
