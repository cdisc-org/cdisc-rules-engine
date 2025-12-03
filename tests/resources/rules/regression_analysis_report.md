# CDISC Rules Engine Regression Analysis

=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 15 (2.0%)
- **Clean rules**: 747 (98.0%)

**Error Breakdown by Category:**

- Rules with **operator errors**: 0
- Rules with **operation errors**: 0
- Rules with **other errors**: 15

## Missing Operators

No missing operator errors found!

## Missing Operations

No missing operation errors found!

## Execution Errors by Type (12 unique error types, 58 total failures across 15 rule occurrences)

1.  **SQL error in not_contains_all operator**: 18 failures across 2 rules
2.  **SQL error in not_equal_to operator**: 5 failures across 2 rules
3.  **Column not found in data**: 4 failures across 2 rules
4.  **Rule format error**: 15 failures across 1 rules
5.  **SQL error in less_than_or_equal_to operator**: 3 failures across 1 rules
6.  **SQL error in not_matches_regex operator**: 2 failures across 1 rules
7.  **SQL error in sqldaydatavalidatoroperation operation**: 2 failures across 1 rules
8.  **SQL error in date_less_than operator**: 2 failures across 1 rules
9.  **SQL error in date_greater_than operator**: 2 failures across 1 rules
10. **SQL error in matches_regex operator**: 2 failures across 1 rules
11. **Validation error**: 2 failures across 1 rules
12. **SQL error in is_not_unique_set operator**: 1 failures across 1 rules

## SQL vs Old Engine Discrepancies
