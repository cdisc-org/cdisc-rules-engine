# CDISC Rules Engine Regression Analysis
=============================================

## Rule Error Summary (out of 762 total rules)

- **Rules with any errors**: 113 (14.8%)
- **Clean rules**: 649 (85.2%)

**Error Breakdown by Category:**
- Rules with **operator errors**: 56
- Rules with **operation errors**: 17
- Rules with **other errors**: 40

## Missing Operators (14 operators, 223 total failures across 56 rule occurrences)

 1. **longer_than**: 66 failures across 12 rules
 2. **matches_regex**: 43 failures across 8 rules
 3. **is_unique_set**: 43 failures across 11 rules
 4. **not_matches_regex**: 32 failures across 13 rules
 5. **longer_than_or_equal_to**: 9 failures across 1 rules
 6. **is_inconsistent_across_dataset**: 8 failures across 2 rules
 7. **invalid_duration**: 4 failures across 1 rules
 8. **invalid_date**: 4 failures across 2 rules
 9. **has_equal_length**: 3 failures across 1 rules
10. **starts_with**: 3 failures across 1 rules
11. **ends_with**: 2 failures across 1 rules
12. **has_next_corresponding_record**: 2 failures across 1 rules
13. **empty_within_except_last_row**: 2 failures across 1 rules
14. **prefix_equal_to**: 2 failures across 1 rules

## Missing Operations (11 operations, 77 total failures across 17 rule occurrences)

 1. **variable_count**: 20 failures across 2 rules
 2. **dy**: 16 failures across 3 rules
 3. **domain_label**: 8 failures across 2 rules
 4. **get_column_order_from_dataset**: 6 failures across 1 rules
 5. **max_date**: 5 failures across 2 rules
 6. **get_model_column_order**: 5 failures across 1 rules
 7. **domain_is_custom**: 4 failures across 1 rules
 8. **extract_metadata**: 4 failures across 1 rules
 9. **get_parent_model_column_order**: 4 failures across 1 rules
10. **min_date**: 3 failures across 2 rules
11. **valid_codelist_dates**: 2 failures across 1 rules
## Other Execution Errors (20 unique messages, 149 total failures across 45 rule occurrences)

 1. **A postgres SQL error occurred**: 94 failures across 23 rules
 2. **Rule contains invalid operator**: 15 failures across 1 rules
 3. **'NoneType' object has no attribute 'get_column_hash'**: 12 failures across 2 rules
 4. **invalid input syntax for type double precision: "TV.VISITDY"...**: 4 failures across 1 rules
 5. **Variable $ds_dsdecod is not a constant.**: 4 failures across 2 rules
 6. **invalid input syntax for type timestamp: "2018-04"
**: 3 failures across 2 rules
 7. **invalid input syntax for type numeric: "redacted"
**: 2 failures across 1 rules
 8. **invalid input syntax for type timestamp: "2019-03"
**: 2 failures across 1 rules
 9. **invalid input syntax for type double precision: "SE.TAETORD"...**: 2 failures across 1 rules
10. **invalid input syntax for type double precision: "TV.VISITDY"...**: 1 failures across 1 rules
11. **Column visitnum or visitnum not found in the respective sche...**: 1 failures across 1 rules
12. **invalid input syntax for type timestamp: "cmdtc"
LINE 4: ......**: 1 failures across 1 rules
13. **invalid input syntax for type timestamp: "2012-08"
**: 1 failures across 1 rules
14. **invalid input syntax for type timestamp: "cmdtc"
LINE 4: ......**: 1 failures across 1 rules
15. **invalid input syntax for type timestamp: "2006-03"
**: 1 failures across 1 rules

## SQL vs Old Engine Discrepancies

### SQL Errors where Old Engine Skipped (190 cases)
*Indicates SQL engine running rules it shouldn't*
- [54] A postgres SQL error occurred
- [22] is_unique_set check_operator not implemented
- [18] longer_than check_operator not implemented
- [15] not_matches_regex check_operator not implemented
- [9] matches_regex check_operator not implemented
- [5] Operation max_date is not implemented
- [4] Variable $ds_dsdecod is not a constant.
- [4] invalid_duration check_operator not implemented
- [4] longer_than_or_equal_to check_operator not implemented
- [4] Operation extract_metadata is not implemented

### SQL Success where Old Engine Skipped (252 cases)
*Indicates SQL engine not respecting rule applicability*
**Skip Types:**
- Class Not Applicable: 179
- Domain Not Applicable: 73

**Examples:**
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00012...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00033...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00033...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00046...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00056...

### SQL Errors where Old Engine Succeeded (99 cases)
*Indicates actual regressions in SQL implementation*
- [17] not_matches_regex check_operator not implemented
- [14] A postgres SQL error occurred
- [13] longer_than check_operator not implemented
- [13] is_unique_set check_operator not implemented
- [12] matches_regex check_operator not implemented
- [8] is_inconsistent_across_dataset check_operator not implemente...
- [8] Operation variable_count is not implemented
- [6] Operation dy is not implemented
- [3] longer_than_or_equal_to check_operator not implemented
- [2] prefix_equal_to check_operator not implemented
