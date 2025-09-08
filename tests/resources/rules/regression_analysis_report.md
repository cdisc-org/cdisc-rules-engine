# CDISC Rules Engine Regression Analysis

=============================================

## Missing Operators (15 operators, 198 total failures)

1.  **longer_than**: 66 failures
2.  **is_unique_set**: 41 failures
3.  **not_matches_regex**: 30 failures
4.  **matches_regex**: 23 failures
5.  **longer_than_or_equal_to**: 9 failures
6.  **invalid_duration**: 4 failures
7.  **is_inconsistent_across_dataset**: 4 failures
8.  **invalid_date**: 4 failures
9.  **has_equal_length**: 3 failures
10. **starts_with**: 3 failures
11. **contains_all**: 3 failures
12. **ends_with**: 2 failures
13. **has_next_corresponding_record**: 2 failures
14. **empty_within_except_last_row**: 2 failures
15. **prefix_equal_to**: 2 failures

## Missing Operations (12 operations, 364 total failures)

1.  **variable_exists**: 227 failures
2.  **record_count**: 50 failures
3.  **variable_count**: 20 failures
4.  **dataset_names**: 20 failures
5.  **dy**: 16 failures
6.  **domain_label**: 8 failures
7.  **max_date**: 5 failures
8.  **get_model_column_order**: 5 failures
9.  **domain_is_custom**: 4 failures
10. **extract_metadata**: 4 failures
11. **min_date**: 3 failures
12. **valid_codelist_dates**: 2 failures

## SQL vs Old Engine Discrepancies

### SQL Errors where Old Engine Skipped (246 cases)

_Indicates SQL engine running rules it shouldn't_

- [45] Operation record_count is not implemented
- [22] A postgres SQL error occurred
- [20] is_unique_set check_operator not implemented
- [18] longer_than check_operator not implemented
- [13] not_matches_regex check_operator not implemented
- [7] Variable $tv_visitnum is not a constant.
- [6] matches_regex check_operator not implemented
- [6] Variable $ta_arm is not a constant.
- [6] Variable $ta_armcd is not a constant.
- [5] Operation max_date is not implemented

### SQL Success where Old Engine Skipped (175 cases)

_Indicates SQL engine not respecting rule applicability_
**Skip Types:**

- Class Not Applicable: 129
- Domain Not Applicable: 46

**Examples:**

- [4] Rule skipped - doesn't apply to class for rule id=CORE-00033...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00046...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00056...
- [3] Rule skipped - doesn't apply to class for rule id=CORE-00017...
- [3] Rule skipped - doesn't apply to class for rule id=CORE-00025...

### SQL Errors where Old Engine Succeeded (152 cases)

_Indicates actual regressions in SQL implementation_

- [19] Operation variable_exists is not implemented
- [17] is_unique_set check_operator not implemented
- [17] not_matches_regex check_operator not implemented
- [13] longer_than check_operator not implemented
- [10] A postgres SQL error occurred
- [9] Operation dataset_names is not implemented
- [8] Variable $dm_studyid is not a constant.
- [8] Operation variable_count is not implemented
- [7] Variable $tv_visitnum is not a constant.
- [6] Operation dy is not implemented

## Other Execution Errors (67 unique messages, 273 total)

- [ 60] A postgres SQL error occurred
- [ 23] Variable $tv_visitnum is not a constant.
- [ 21] Rule contains invalid operator
- [ 16] Variable $dm_studyid is not a constant.
- [ 16] Variable $ta_epoch is not a constant.
- [ 11] Variable $dm_usubjid is not a constant.
- [ 9] Variable $TV_VISITNUM is not a constant.
- [ 6] Variable $ta_arm is not a constant.
- [ 6] Variable $ta_armcd is not a constant.
- [ 5] Variable $tv_visit is not a constant.
- [ 4] Column AGETXT does not exist in the table dm.
- [ 4] Variable $sv_visitnum is not a constant.
- [ 4] Variable $te_etcd is not a constant.
- [ 2] Column ecstat does not exist in the table ec.
- [ 2] Column ECSTAT does not exist in the table ec.
