# CDISC Rules Engine Regression Analysis
=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 62 (8.1%)
- **Clean rules**: 700 (91.9%)

**Error Breakdown by Category:**
- Rules with **operator errors**: 29
- Rules with **operation errors**: 17
- Rules with **other errors**: 16

## Missing Operators (9 operators, 95 total failures across 29 rule occurrences)

 1. **not_matches_regex**: 32 failures across 13 rules
 2. **matches_regex**: 43 failures across 8 rules
 3. **invalid_date**: 4 failures across 2 rules
 4. **invalid_duration**: 4 failures across 1 rules
 5. **starts_with**: 3 failures across 1 rules
 6. **prefix_equal_to**: 3 failures across 1 rules
 7. **ends_with**: 2 failures across 1 rules
 8. **has_next_corresponding_record**: 2 failures across 1 rules
 9. **empty_within_except_last_row**: 2 failures across 1 rules

## Missing Operations (11 operations, 77 total failures across 17 rule occurrences)

 1. **dy**: 16 failures across 3 rules
 2. **variable_count**: 20 failures across 2 rules
 3. **domain_label**: 8 failures across 2 rules
 4. **max_date**: 5 failures across 2 rules
 5. **min_date**: 3 failures across 2 rules
 6. **get_column_order_from_dataset**: 6 failures across 1 rules
 7. **get_model_column_order**: 5 failures across 1 rules
 8. **domain_is_custom**: 4 failures across 1 rules
 9. **extract_metadata**: 4 failures across 1 rules
10. **get_parent_model_column_order**: 4 failures across 1 rules
11. **valid_codelist_dates**: 2 failures across 1 rules
## Execution Errors by Type (19 unique error types, 229 total failures across 62 rule occurrences)

 1. **An unknown exception has occurred**: 96 failures across 21 rules
 2. **SQL error in not_matches_regex operator**: 32 failures across 13 rules
 3. **SQL error in matches_regex operator**: 43 failures across 8 rules
 4. **SQL error in is_incomplete_date operator**: 10 failures across 2 rules
 5. **SQL error in does_not_contain operator**: 4 failures across 2 rules
 6. **SQL error in invalid_date operator**: 4 failures across 2 rules
 7. **SQL error in less_than_or_equal_to operator**: 3 failures across 2 rules
 8. **Rule format error**: 15 failures across 1 rules
 9. **SQL error in invalid_duration operator**: 4 failures across 1 rules
10. **SQL error in starts_with operator**: 3 failures across 1 rules
11. **SQL error in prefix_not_equal_to operator**: 3 failures across 1 rules
12. **SQL error in ends_with operator**: 2 failures across 1 rules
13. **SQL error in does_not_have_next_corresponding_record operator**: 2 failures across 1 rules
14. **SQL error in empty_within_except_last_row operator**: 2 failures across 1 rules
15. **SQL error in date_greater_than operator**: 2 failures across 1 rules


## SQL vs Old Engine Discrepancies

### SQL Errors where Old Engine Skipped (78 cases)
*Indicates SQL engine running rules it shouldn't*
- [15] not_matches_regex check_operator not implemented
- [9] matches_regex check_operator not implemented
- [5] Operation max_date is not implemented
- [4] '$ds_dsdecod'
- [4] invalid_duration check_operator not implemented
- [4] Operation extract_metadata is not implemented
- [4] Operation dy is not implemented
- [4] Rule contains invalid operator
- [4] invalid_date check_operator not implemented
- [3] Joins with relationship domains are not supported yet

### SQL Success where Old Engine Skipped (360 cases)
*Indicates SQL engine not respecting rule applicability*
**Skip Types:**
- Class Not Applicable: 258
- Domain Not Applicable: 102

**Examples:**
- [6] Rule skipped - doesn't apply to class for rule id=CORE-00047...
- [6] Rule skipped - doesn't apply to class for rule id=CORE-00053...
- [6] Rule skipped - doesn't apply to class for rule id=CORE-00058...
- [5] Rule skipped - doesn't apply to class for rule id=CORE-00014...
- [5] Rule skipped - doesn't apply to class for rule id=CORE-00056...

### SQL Errors where Old Engine Succeeded (57 cases)
*Indicates actual regressions in SQL implementation*
- [17] not_matches_regex check_operator not implemented
- [12] matches_regex check_operator not implemented
- [8] Operation variable_count is not implemented
- [6] Operation dy is not implemented
- [5] column or relation does not exist
- [3] prefix_equal_to check_operator not implemented
- [2] invalid input syntax
- [2] Joins with relationship domains are not supported yet
- [1] No valid columns found for uniqueness check.
- [1] 'NoneType' object has no attribute 'get_column_hash'
