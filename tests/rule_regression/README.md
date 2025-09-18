# Regression tests:

## Sharepoint Quality Control

- download a copy of the sharepoint unitTesting folder
- run the sharepoint structure validation script against that folder (sp_structure_validation.py)
- fix structural issues

## Rules preparation

- go to CDISC rule editor (https://rule-editor.cdisc.org/)
- remove all filters and get an export
- put it into: `home + "/data/CORE/rules_dump_20250806.csv`
- run all_rules_prep.ipynb notebook, using the repo's pyenv as the kernel

## Run regression

- run `test_regression::test_regression_all_rules` to update the rules.json
- Set up the two folders `tests/resources/rules/dev/test_case_results_old` and `tests/resources/rules/dev/test_case_results_sql`
- run `test_regression::test_regression_single_rule_DEV` to run local regression on a specific rule, as set in .env under `CURRENT_RULE_DEV`.

## IGNORE ATM

- `remove_xlsx_formatting.py`
