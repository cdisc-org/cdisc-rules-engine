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

- run `test_regression.py` to update the rules.json

## IGNORE ATM

- `remove_xlsx_formatting.py`
