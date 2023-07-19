## define_variable_metadata

If a target variable `name` is specified, returns the specified metadata in the define for the specified target variable.

- Input

  ```yaml
  - operation: define_variable_metadata
    attribute_name: define_variable_label
    name: LBTESTCD
    id: $LBTESTCD_VARIABLE_LABEL
  ```

- Output

  `Laboratory Test Code`

If no target variable `name` specified, returns a dictionary containing the specified metadata in the define for all variables.

- Input

  ```yaml
  - operation: define_variable_metadata`
    attribute_name: define_variable_label`
    id: $VARIABLE_LABEL`
  ```

- Output

  ```json
  {
    "STUDYID": "Study Identifier",
    "USUBJID": "Unique Subject Identifier",
    "LBTESTCD": "Laboratory Test Code",
    "...": "..."
  }
  ```

## distinct

Get a distinct list of values for the given `name`. If a `group` list is specified, the distinct value list will be grouped by the variables within `group`.

```yaml
Check:
  all:
    - name: SSSTRESC
      operator: equal_to
      value: DEAD
      value_is_literal: true
    - name: $ds_dsdecod
      operator: does_not_contain
      value: DEATH
      value_is_literal: true
  Operations:
    - operator: distinct
      domain: DS
      name: DSDECOD
      id: $ds_dsdecod
      group:
        - USUBJID
```

## domain_is_custom

Checks whether the domain is in the set of domains within the provided standard.

- Input

  Target Domain: `XY`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  Operations:
    - operator: domain_is_custom
      id: $domain_is_custom
  ```

- Output

  `true`

## domain_label

Returns the label for the domain the operation is executing on within the provided standard.

- Input.

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  Operations:
    - operator: domain_label
      id: $domain_label
  ```

- Output

  `Laboratory Test Results`

## dy

Calculates the number of days between the DTC and RFSTDTC. The Study Day value is incremented by 1 for each date following RFSTDTC. Dates prior to RFSTDTC are decreased by 1, with the date preceding RFSTDTC designated as Study Day -1 (there is no Study Day 0). . . . All Study Day values are integers. Thus, to calculate Study Day:

- `--DY = (date portion of --DTC) - (date portion of RFSTDTC) + 1 if --DTC is on or after RFSTDTC`
- `--DY = (date portion of --DTC) - (date portion of RFSTDTC) if --DTC precedes RFSTDTC`

This algorithm should be used across all domains.

```yaml
Check:
  all:
    - name: --DY
      operator: non_empty
    - name: --DTC
      operator: is_complete_date
    - name: RFSTDTC
      operator: is_complete_date
    - name: --DY
      operator: not_equal_to
      value: $dy`
Operations:
  - name: --DTC
    operator: dy
    id: $dy
Match Datasets:
  - Name: DM
    Keys:
      - USUBJID
```

## expected_variables

Returns the expected variables for the domain in the current standard.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operation: expected_variables`
    id: $expected_variables`
  ```

- Output:

  ```json
  ["LBCAT", "LBORRES", "LBORRESU", "..."]
  ```

## extract_metadata

Returns the requested dataset level metadata value for the current dataset. Possible `name` values are:

- `dataset_size`
- `dataset_location`
- `dataset_name`
- `dataset_label`

Example

- Input:

  Target domain: `LB`

  ```yaml
  - name: dataset_label
    operator: extract_metadata
    id: $dataset_label
  ```

- Output:

  `Laboratory Test Results`

## get_codelist_attributes

Fetches attribute values for a codelist specified in a dataset (like TS)

```yaml
- id: $TERM_CCODES
  name: TSVCDREF
  operation: get_codelist_attributes
  ct_attribute: Term CCODE
  ct_version: TSVCDVER
  ct_packages:
    - sdtmct-2020-03-27
```

## get_column_order_from_dataset

Returns list of dataset columns in order

```yaml
Check:
  all:
    - name: $column_order_from_dataset
      operator: is_not_ordered_by
      value: $column_order_from_library
Operations:
  - id: $column_order_from_library
    operator: get_column_order_from_library
  - id: $column_order_from_dataset
    operator: get_column_order_from_dataset
```

## get_column_order_from_library

Fetches column order for a given domain from the CDISC library. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Rule Type: Variable Metadata Check
Check:
  all:
    - name: variable_name
      operator: is_not_contained_by
      value: $ig_variables
Operations:
  - id: $ig_variables
    operator: get_column_order_from_library
```

## get_model_column_order

Fetches column order for a given model class from the CDISC library. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Rule Type: Variable Metadata Check
Check:
  all:
    - name: variable_name
      operator: is_not_contained_by
      value: $model_variables
Operations:
  - id: $model_variables
    operator: get_model_column_order
```

## get_model_filtered_variables

Fetches variable level library model properties filtered by the provided `key_name` and `key_value`

Example

- Input

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operation: get_model_filtered_variables`
    id: $model_filtered_variables`
    key_name: "role"
    key_value: "Timing"
  ```

- Output

  ```json
  ["VISITNUM", "VISIT", "VISITDY", "TAETORD", "..."]
  ```

## get_parent_model_column_order

Fetches column order for a given SUPP's parent model class from the CDISC library. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Check:
  all:
    - operator: is_not_contained_by
      value: $parent_model_variables
Operations:
  - id: $parent_model_variables
    operator: get_parent_model_column_order
```

## label_referenced_variable_metadata

Generates a dataframe where each record in the dataframe is the library ig variable metadata corresponding with the variable label found in the column provided in `name`

- Input

  Target Dataset: `SUPPLB`

  Product: `sdtmig`

  Version: `3-4`

  Dataset:

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QLABEL": ["Toxicity", "Viscosity", "Analysis Method"]
  }
  ```

  Rule:

  ```yaml
  - operation: label_referenced_variable_metadata`
    id: $label_referenced_variable_metadata`
    name: "QLABEL"
  ```

- Output

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QLABEL": ["Toxicity", "Viscosity", "Analysis Method"],
    "$label_referenced_variable_name": ["LBTOX", null, "LBANMETH"],
    "$label_referenced_variable_role": [
      "Variable Qualifier",
      null,
      "Record Qualifier"
    ],
    "$label_referenced_variable_ordinal": [44, null, 38],
    "$label_referenced_variable_label": ["Toxicity", null, "Analysis Method"]
  }
  ```

## max

If no `group` is provided, returns the max value in `name`. If `group` is provided, returns the max value in `name`, within each unique set of the grouping variables.

```yaml
Check:
  all:
    - name: "$max_age"
      operator: "greater_than"
      value: "MAXAGE"
Operations:
  - operator: "max"
    domain: "DM"
    name: "AGE"
    id: "$max_age"
```

## max_date

If no `group` is provided, returns the max date value in `name`. If `group` is provided, returns the max date value in `name`, within each unique set of the grouping variables.

```yaml
Check:
  all:
    - name: USUBJID
      operator: is_contained_by
      value: $ex_usubjid
    - name: RFXENDTC
      operator: not_equal_to
      value: $max_ex_exstdtc
    - name: RFXENDTC
      operator: not_equal_to
      value: $max_ex_exendtc
Operations:
  - operator: distinct
    domain: EX
    name: USUBJID
    id: $ex_usubjid
  - operator: max_date
    domain: EX
    name: EXSTDTC
    id: $max_ex_exstdtc
    group:
      - USUBJID
  - operator: max_date
    domain: EX
    name: EXENDTC
    id: $max_ex_exendtc
    group:
      - USUBJID
```

## mean

Example: AAGE > mean(DM.AGE), where AAGE is a fictitious NSV

```yaml
Check:
  all:
    - name: "AAGE"
      operator: "greater_than"
      value: "$average_age"
Operations:
  - operator: "mean"
    domain: "DM"
    name: "AGE"
    id: "$average_age"
```

## min

If no `group` is provided, returns the min value in `name`. If `group` is provided, returns the min value in `name`, within each unique set of the grouping variables.

```yaml
Check:
  all:
    - name: "$min_age"
      operator: "less_than"
      value: "MINAGE"
Operations:
  - operator: "min"
    domain: "DM"
    name: "AGE"
    id: "$min_age"
```

## min_date

If no `group` is provided, returns the min date value in `name`. If `group` is provided, returns the min date value in `name`, within each unique set of the grouping variables.

Example: RFSTDTC is greater than min AE.AESTDTC for the current USUBJID

```yaml
Check:
  all:
    - name: "RFSTDTC"
      operator: "date_greater_than"
      value: "$ae_aestdtc"
Operations:
  - operator: "min_date"
    domain: "AE"
    name: "AESTDTC"
    id: "$ae_aestdtc"
    group:
      - USUBJID
```

## name_referenced_variable_metadata

Generates a dataframe where each record in the dataframe is the library ig variable metadata corresponding with the variable name found in the column provided in `name`

- Input

  Target Dataset: `SUPPLB`

  Product: `sdtmig`

  Version: `3-4`

  Dataset:

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QNAM": ["Toxicity", "LBVISCOS", "Analysis Method"]
  }
  ```

  Rule:

  ```yaml
  - operation: name_referenced_variable_metadata`
    id: $name_referenced_variable_metadata`
    name: "QNAM"
  ```

- Output

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QNAM": ["LBTOX", "LBVISCOS", "LBANMETH"],
    "$label_referenced_variable_name": ["LBTOX", null, "LBANMETH"],
    "$label_referenced_variable_role": [
      "Variable Qualifier",
      null,
      "Record Qualifier"
    ],
    "$label_referenced_variable_ordinal": [44, null, 38],
    "$label_referenced_variable_label": ["Toxicity", null, "Analysis Method"]
  }
  ```

## permissible_variables

Returns the permissible variables for a given domain and standard

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operation: permissible_variables`
    id: $permissible_variables`
  ```

- Output:

  ```json
  ["LBGRPID", "LBREFID", "LBSPID", "..."]
  ```

## record_count

Returns the number of records in the dataset

## required_variables

Returns the required variables for a given domain and standard

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operation: required_variables`
    id: $required_variables`
  ```

- Output:

  ```json
  ["STUDYID", "DOMAIN", "USUBJID", "LBSEQ", "LBTESTCD", "LBTEST"]
  ```

## study_domains

Returns a list of the domains in the study

## valid_codelist_dates

Returns the valid codelist dates for a given standard

Given a list of codelists:

```json
[
  "sdtmct-2023-10-26",
  "sdtmct-2023-12-13",
  "adamct-2023-12-13",
  "cdashct-2023-05-19"
]
```

and standard: `sdtmig`

the operation will return

```json
["2023-10-26", "2023-12-13"]
```

## valid_meddra_code_references

Determines whether the values are valid in the following variables:

- `--SOCCD`
- `--HLGTCD`
- `--HLTCD`
- `--PTCD`
- `--LLTCD`

## valid_meddra_code_term_pairs

Determines whether the values are valid in the following variable pairs:

- `--SOCCD`, `--SOC`
- `--HLGTCD`, `--HLGT`
- `--HLTCD`, `--HLT`
- `--PTCD`, `--DECOD`
- `--LLTCD`, `--LLT`

## valid_meddra_term_references

Determines whether the values are valid in the following variables:

- `--SOC`
- `--HLGT`
- `--HLT`
- `--DECOD`
- `--LLT`

## valid_whodrug_references

Checks if a reference to whodrug term in `name` points to the existing code in Atc Text (INA) file.

## variable_count

Returns a mapping of variable names to the number of times that variable appears in a domain within the study.

- Input

  ```json
  {
    "AE": ["STUDYID", "DOMAIN", "USUBJID", "AETERM", "AEENDTC"],
    "LB": ["STUDYID", "DOMAIN", "USUBJID", "LBTESTCD", "LBENDTC"]
  }
  ```

- Output

  ```json
  {
    "STUDYID": 2,
    "DOMAIN": 2,
    "USUBJID": 2,
    "--TERM": 1,
    "--TESTCD": 1,
    "--ENDTC": 2
  }
  ```

## variable_exists

Flag an error if MIDS is in the dataset currently being evaluated and the TM domain is not present in the study

```yaml
Rule Type: Domain Presence Check
Check:
  all:
    - name: $MIDS_EXISTS
      operator: equal_to
      value: true
    - name: TM
      operator: not_exists
Operations:
  - id: $MIDS_EXISTS
    name: MIDS
    operator: variable_exists
```

## variable_is_null

True if variable is missing or if all values within a variable are null or empty string

## variable_names

Return the set of variable names from the library for the given standard

## variable_library_metadata

Get the metadata value from the library for all variables in the current dataset. Metadata attribute is specified by the `name`.

Result

```json
{
  "STUDYID": "Req",
  "DOMAIN": "Req",
  "AEGRPID": "Perm",
  "AETERM": "Req",
  "AELLT": "Exp",
  "...": "..."
}
```

> Condition: Variable Core Status = Required

> Rule: Variable ^= null

```yaml
Check:
  any:
    - all:
        - operator: variable_metadata_equal_to
          value: Req
          metadata: $var_perm
        - operator: empty
Operations:
  - id: $var_perm
    operator: variable_library_metadata
    name: core
```

## variable_value_count

Given a variable `name`, returns a mapping of variable values to the number of times that value appears in the variable within all datasets in the study.

## whodrug_code_hierarchy

Determines whether the values are valid in the following variables:

- `--DECOD`
- `--CLAS`
- `--CLASCD`
