# CDISC Rules Engine Regression Analysis


## Missing Operators (14 operators, 199 total failures)

 1. **longer_than**: 66 failures
 2. **is_unique_set**: 47 failures
 3. **not_matches_regex**: 30 failures
 4. **matches_regex**: 21 failures
 5. **longer_than_or_equal_to**: 9 failures
 6. **invalid_duration**: 4 failures
 7. **is_inconsistent_across_dataset**: 4 failures
 8. **invalid_date**: 4 failures
 9. **has_equal_length**: 3 failures
10. **starts_with**: 3 failures
11. **ends_with**: 2 failures
12. **has_next_corresponding_record**: 2 failures
13. **empty_within_except_last_row**: 2 failures
14. **prefix_equal_to**: 2 failures

## Missing Operations (10 operations, 87 total failures)

1.  **variable_count**: 20 failures
2.  **dataset_names**: 20 failures
3.  **dy**: 16 failures
4.  **domain_label**: 8 failures
5.  **max_date**: 5 failures
6.  **get_model_column_order**: 5 failures
7.  **domain_is_custom**: 4 failures
8.  **extract_metadata**: 4 failures
9.  **min_date**: 3 failures
10. **valid_codelist_dates**: 2 failures

## SQL vs Old Engine Discrepancies

### SQL Errors where Old Engine Skipped (189 cases)
*Indicates SQL engine running rules it shouldn't*
- [61] A postgres SQL error occurred
- [22] is_unique_set check_operator not implemented
- [18] longer_than check_operator not implemented
- [13] not_matches_regex check_operator not implemented
- [7] Rule contains invalid operator
- [6] matches_regex check_operator not implemented
- [5] Operation max_date is not implemented
- [4] Variable $ds_dsdecod is not a constant.
- [4] Operation variable_exists is not implemented
- [4] invalid_duration check_operator not implemented
- [4] longer_than_or_equal_to check_operator not implemented

### SQL Success where Old Engine Skipped (230 cases)

### SQL Success where Old Engine Skipped (228 cases)
*Indicates SQL engine not respecting rule applicability*
**Skip Types:**
- Class Not Applicable: 171
- Domain Not Applicable: 57

- [4] Rule skipped - doesn't apply to class for rule id=CORE-00012...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00033...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00033...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00046...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00056...

### SQL Errors where Old Engine Succeeded (98 cases)

_Indicates actual regressions in SQL implementation_

- [17] is_unique_set check_operator not implemented
- [17] not_matches_regex check_operator not implemented
- [16] A postgres SQL error occurred
- [13] longer_than check_operator not implemented
- [9] Operation dataset_names is not implemented
- [8] Operation variable_count is not implemented
- [6] Operation dy is not implemented
- [4] is_inconsistent_across_dataset check_operator not implemente...
- [3] matches_regex check_operator not implemented
- [3] longer_than_or_equal_to check_operator not implemented

## Other Execution Errors (15 unique messages, 155 total)

- [108] A postgres SQL error occurred
- [ 26] Rule contains invalid operator
- [ 4] invalid input syntax for type double precision: "TV.VISITDY"
  LINE 6: ...
- [ 4] Variable $ds_dsdecod is not a constant.
- [ 2] invalid input syntax for type numeric: "redacted"

- [ 2] invalid input syntax for type timestamp: "2019-03"

- [ 1] invalid input syntax for type double precision: "TV.VISITDY"
  LINE 6: ....
- [ 1] Column visitnum or visitnum not found in the respective schemas.
- [ 1] invalid input syntax for type timestamp: "2012-08"

- [ 1] invalid input syntax for type timestamp: "2006-03"

- [ 1] invalid input syntax for type timestamp: "2018-05"

- [ 1] invalid input syntax for type timestamp: "2018-04"

- [ 1] invalid input syntax for type timestamp: "2018-04-17T09"

- [ 1] invalid input syntax for type timestamp: " 2018-07"


## Other Execution Errors (15 unique messages, 153 total)

- [106] A postgres SQL error occurred
- [ 26] Rule contains invalid operator
- [  4] invalid input syntax for type double precision: "TV.VISITDY"
LINE 6:  ...
- [  4] Variable $ds_dsdecod is not a constant.
- [  2] invalid input syntax for type numeric: "redacted"

- [  2] invalid input syntax for type timestamp: "2019-03"

- [  1] invalid input syntax for type double precision: "TV.VISITDY"
LINE 6: ....
- [  1] Column visitnum or visitnum not found in the respective schemas.
- [  1] invalid input syntax for type timestamp: "2012-08"

- [  1] invalid input syntax for type timestamp: "2006-03"

- [  1] invalid input syntax for type timestamp: "2018-05"

- [  1] invalid input syntax for type timestamp: "2018-04"

- [  1] invalid input syntax for type timestamp: "2018-04-17T09"

- [  1] invalid input syntax for type timestamp: "	2018-07"

- [  1] invalid input syntax for type timestamp: "2019"
