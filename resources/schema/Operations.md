## codelist_terms

Returns a list of valid codelist/term values. Used for evaluating whether NCI code or submission values are valid based on controlled terminology. Expects three parameters: `codelists` which is a list of the codelist submission value(s) to retrieve, `level` which is the level of data (either "codelist" or "term") at which to return data from, and `returntype` which is the type of values to return, either "code" for NCI Code(s) or "value" for submission value(s)

```yaml
- Check:
    - all:
        - name: PPSTRESU
          operator: is_not_contained_by
          value: $terms
        - name: $extensible
          operator: equal_to
          value: true
- Operations:
    - id: $terms
      operator: codelist_terms
      codelists:
        - PKUDUG
      level: term
      returntype: value
    - id: $extensible
      codelist: PKUDUG
      operator: codelist_extensible
```

If `ct_package_type`, `version`, and `codelist_code` parameters are provided, it will instead attach a new column containing the term for each combination provided in the source dataset. If `term_code` is provided, it will find term values using the term codes. If `term_value` is provided, it will find term codes using the term values.Only one of `term_code` or `term_value` can be provided.

For example, given the current dataset:

| id  | codeSystemVersion | $codelist_code | code    | decode     |
| --- | ----------------- | -------------- | ------- | ---------- |
| 1   | 2024-09-27        | C201264        | C201356 | After      |
| 2   | 2024-09-27        | C201265        | C201352 | End to End |
| 3   | 2023-03-29        | C127262        | C51282  | CLINIC     |

and the following operation:

```yaml
- id: $found_term_value
  operator: codelist_terms
  ct_package_type: DDF
  version: codeSystemVersion
  codelist_code: $codelist_code
  term_code: code
```

This will result in the following dataset:

| id  | codeSystemVersion | $codelist_code | code    | decode     | $found_term_value |
| --- | ----------------- | -------------- | ------- | ---------- | ----------------- |
| 1   | 2024-09-27        | C201264        | C201356 | After      | After             |
| 2   | 2024-09-27        | C201265        | C201352 | End to End | End to End        |
| 3   | 2023-03-29        | C127262        | C51282  | CLINIC     | CLINIC            |

Conversely, if given the same dataset, and the following operation:

```yaml
- id: $found_term_code
  operator: codelist_terms
  ct_package_type: DDF
  version: codeSystemVersion
  codelist_code: $codelist_code
  term_value: decode
```

This will result in the following dataset:

| id  | codeSystemVersion | $codelist_code | code    | decode     | $found_term_code |
| --- | ----------------- | -------------- | ------- | ---------- | ---------------- |
| 1   | 2024-09-27        | C201264        | C201356 | After      | C201356          |
| 2   | 2024-09-27        | C201265        | C201352 | End to End | C201352          |
| 3   | 2023-03-29        | C127262        | C51282  | CLINIC     | C51282           |

## codelist_extensible

Returns a Series indicating whether a specified `codelist` is extensible. Used in conjunction with `codelist_terms` to determine if values outside the codelist are acceptable. From the above example, `$extensible` will contain a bool if the codelist PKUDUG is extensible in all rows of the column.

If `ct_package_type`, `version`, and `codelist_code` parameters are provided, it will instead attach a new column containing the extensible value for each combination provided in the source dataset.

For example, given the current dataset:

| id  | codeSystemVersion | $codelist_code |
| --- | ----------------- | -------------- |
| 1   | 2024-09-27        | C201264        |
| 2   | 2024-09-27        | C201265        |
| 3   | 2023-03-29        | C127262        |

and the following operation:

```yaml
- id: $codelist_extensible
  operator: codelist_extensible
  ct_package_type: DDF
  version: codeSystemVersion
  codelist_code: $codelist_code
```

This will result in the following dataset:

| id  | codeSystemVersion | $codelist_code | $codelist_extensible |
| --- | ----------------- | -------------- | -------------------- |
| 1   | 2024-09-27        | C201264        | false                |
| 2   | 2024-09-27        | C201265        | false                |
| 3   | 2023-03-29        | C127262        | true                 |

## define_extensible_codelists

Returns a list of valid extensible codelist term's submission values. Used for evaluating whether submission values are valid based on controlled terminology. Expects the parameter `codelists` which is a list of the codelist submission value(s) to retrieve. If the codelist argument is `["All"]` will return all extensible terms for the CT in a list.

```yaml
    {
      "id": "$ext_value",
      "codelist": ["ALL"],
      "operator": "define_extensible_codelists"
    },
```

## define_variable_metadata

If a target variable `name` is specified, returns the specified metadata in the define for the specified target variable.

- Input

  ```yaml
  - operator: define_variable_metadata
    attribute_name: define_variable_label
    name: LBTESTCD
    id: $LBTESTCD_VARIABLE_LABEL
  ```

- Output

  `Laboratory Test Code`

If no target variable `name` specified, returns a dictionary containing the specified metadata in the define for all variables.

- Input

  ```yaml
  - operator: define_variable_metadata`
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

## dataset_names

Returns a list of the submitted dataset filenames in all uppercase

ex. if TS.xpt, AE.xpt, EC.xpt, and SUPPEC.xpt are submitted -> [TS, AE, EC, SUPPEC] will be returned

## distinct

Get a distinct list of values for the given `name`.

- If a `group` list is specified, the distinct value list will be grouped by the variables within `group`.
- If a `filter` object is provided, only values for records that match the filter criteria are included in the distinct values.

If `group` is provided, `group_aliases` may also be provided to assign new grouping variable names so that results grouped by the values in one set of grouping variables can be merged onto a dataset according to the same grouping value(s) stored in different set of grouping variables. When both `group` and `group_aliases` are provided, columns are renamed according to corresponding list position (i.e., the 1st column in `group` is renamed to the 1st column in `group_aliases`, etc.). If there are more columns listed in `group` than in `group_aliases`, only the `group` columns with corresponding `group_aliases` columns will be renamed. If there are more columns listed in `group_aliases` than in `group`, the extra column names in `group_aliases` will be ignored. See [record_count](#record_count) for an example of the use of `group_aliases`.

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
    filter:
      CAT: "CATEGORY 1"
      SCAT: "SUBCATEGORY A"
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

Returns the expected ("Core" = Exp ) variables for the domain in the current standard
Variable Metadata for custom domains will pull from the model while non-custom domains will be from the IG and Model.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: expected_variables`
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
  operator: get_codelist_attributes
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

## get_dataset_filtered_variables

Filters variables from the dataset based on specified metadata criteria. Returns a list of variable names that exist in the dataset and match the filter criteria.

```yaml
- operation: get_dataset_filtered_variables
  id: $timing_variables
  key_name: "role"
  key_value: "Timing"
```

## get_model_filtered_variables

Fetches variable level library model properties filtered by the provided `key_name` and `key_value`

Example

- Input

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: get_model_filtered_variables`
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
  - operator: label_referenced_variable_metadata
    id: $label_referenced_variable_metadata
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

## map

Allows the creation of a lookup table to take the values from multiple input columns and map them to values in an output column. The `map` parameter contains a list of objects. Each dictionary contains column names as properties that match the column names in the source dataset and an `output` property that will be returned as a result.

If `map` has a single object and `output` is the only property specified on that object, this will function as a direct assignment.

For example, given the following current dataset:

| id  | parent_entity | parent_rel     |
| --- | ------------- | -------------- |
| 1   | Timing        | relativeToFrom |
| 2   | Something     | relativeToFrom |
| 3   | Timing        | type           |

and the following operation:

```yaml
Operations:
  - id: $codelist_code
    operator: map
    map:
      - parent_entity: Timing
        parent_rel: type
        output: C201264
      - parent_entity: Timing
        parent_rel: relativeToFrom
        output: C201265
```

This will result in the following dataset:

| id  | parent_entity | parent_rel     | $codelist_code |
| --- | ------------- | -------------- | -------------- |
| 1   | Timing        | relativeToFrom | C201265        |
| 2   | Something     | relativeToFrom | None           |
| 3   | Timing        | type           | C201264        |

The following operation:

```yaml
Operations:
  - id: $codelist_code
    operator: map
    map:
      - output: C201264
```

Will result in the following dataset:

| id  | parent_entity | parent_rel     | $codelist_code |
| --- | ------------- | -------------- | -------------- |
| 1   | Timing        | relativeToFrom | C201264        |
| 2   | Something     | relativeToFrom | C201264        |
| 3   | Timing        | type           | C201264        |

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
  - operator: name_referenced_variable_metadata`
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

Returns the permissible variables ("Core" = Perm ) for a given domain and standard
Variable Metadata for custom domains will pull from the model while non-custom domains will be from the IG and Model.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: permissible_variables`
    id: $permissible_variables`
  ```

- Output:

  ```json
  ["LBGRPID", "LBREFID", "LBSPID", "..."]
  ```

## record_count

If no `filter` or `group` is provided, returns the number of records in the dataset. If `filter` is provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter. If `group` is provided, returns the number of rows matching each unique set of the grouping variables. These can be static column name(s) or can be derived from other operations like `get_dataset_filtered_variables`. │ │If both `filter` and `group` are provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter that also match each unique set of the grouping variables.

**Wildcard Filtering**: Filter values ending with `%` will match any records where the column value starts with the specified prefix. For example, `RACE%` will match `RACE1`, `RACE2`, `RACE3`, etc. This is useful for matching related variables with numeric or alphabetic suffixes.

If `group` is provided, `group_aliases` may also be provided to assign new grouping variable names so that results grouped by the values in one set of grouping variables can be merged onto a dataset according to the same grouping value(s) stored in different set of grouping variables. When both `group` and `group_aliases` are provided, columns are renamed according to corresponding list position (i.e., the 1st column in `group` is renamed to the 1st column in `group_aliases`, etc.). If there are more columns listed in `group` than in `group_aliases`, only the `group` columns with corresponding `group_aliases` columns will be renamed. If there are more columns listed in `group_aliases` than in `group`, the extra column names in `group_aliases` will be ignored.

Example: return the number of records in a dataset.

```yaml
- operator: record_count
  id: $records_in_dataset
```

Example: return the number of records where STUDYID = "CDISC01" and FLAGVAR = "Y".

```yaml
- operator: record_count
  id: $flagged_cdisc01_records_in_dataset
  filter:
    STUDYID: "CDISC01"
    FLAGVAR: "Y"
```

Example: return the number of records where QNAM starts with "RACE" (matches RACE1, RACE2, RACE3, etc.) per USUBJID.

```yaml
- operation: record_count
  id: $race_records_in_dataset
  filter:
    QNAM: "RACE%"
  group:
    - "USUBJID"
```

Example: return the number of records grouped by USUBJID.

```yaml
- operator: record_count
  id: $records_per_usubjid
  group:
    - USUBJID
```

Example: return the number of records grouped by USUBJID where FLAGVAR = "Y".

```yaml
- operator: record_count
  id: $flagged_records_per_usubjid
  group:
    - USUBJID
  filter:
    FLAGVAR: "Y"
```

Example: return the number of records grouped by USUBJID and IDVARVAL where QNAM = "TEST1" and IDVAR = "GROUPID", renaming the IDVARVAL column to GROUPID for subsequent merging.

```yaml
- operator: record_count
  id: $test1_records_per_usubjid_groupid
  group:
    - USUBJID
    - IDVARVAL
  filter:
    QNAM: "TEST1"
    IDVAR: "GROUPID"
  group_aliases:
    - USUBJID
    - GROUPID
```

Example: Group the `StudyIdentifier` dataset by `parent_id` and merge the result back to the context dataset `StudyVersion` using `StudyVersion.id == StudyIdentifier.parent_id`

```yaml
Scope:
  Entities:
    Include:
      - StudyVersion
Operations:
  - domain: StudyIdentifier
    filter:
      parent_entity: "StudyVersion"
      parent_rel: "studyIdentifiers"
      rel_type: "definition"
      studyIdentifierScope.organizationType.code: "C70793"
      studyIdentifierScope.organizationType.codeSystem: "http://www.cdisc.org"
    group:
      - parent_id
    group_aliases:
      - id
    id: $num_sponsor_ids
    operator: record_count
```

## required_variables

Returns the required variables ( "Core" = Req ) for a given domain and standard
Variable Metadata for custom domains will pull from the model while non-custom domains will be from the IG and Model.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: required_variables
    id: $required_variables
  ```

- Output:

  ```json
  ["STUDYID", "DOMAIN", "USUBJID", "LBSEQ", "LBTESTCD", "LBTEST"]
  ```

## study_domains

Returns a list of the domains in the study

## valid_codelist_dates

Returns the valid terminology package dates for a given standard.

Given a list of terminology packages:

```json
[
  "sdtmct-2023-10-26",
  "sdtmct-2023-12-13",
  "adamct-2023-12-13",
  "cdashct-2023-05-19"
]
```

and standard: `sdtmig`

the operation will return:

```json
["2023-10-26", "2023-12-13"]
```

By default, the standard is as specified when running validation - as the validation runtime parameter and/or as specified in the rule header - and the list of terminology packages is obtained from the current cache. If required, the default standard may be overridden using the optional `ct_package_types` parameter. For example, given the same list of terminology packages, the following operation:

```yaml
Operations:
  - operator: valid_codelist_dates
    id: $valid_dates
    ct_package_types:
      - SDTM
      - CDASH
```

will return:

```json
["2023-05-19", "2023-10-26", "2023-12-13"]
```

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

## variable_value_count

Given a variable `name`, returns a mapping of variable values to the number of times that value appears in the variable within all datasets in the study.

# External Dictionary Validation Operations

## Supported External Dictionary Types

```
MEDDRA = "meddra"
WHODRUG = "whodrug"
LOINC = "loinc"
MEDRT = "medrt"
UNII = "unii"
SNOMED = "snomed"
```

## Generic External Dictionary Operations

## valid_define_external_dictionary_version

Returns true if the version of an external dictionary provided in the define.xml file matches
the version parsed from the dictionary files.

Input:

```yaml
Operations:
  - operator: valid_define_external_dictionary_version
    id: $is_valid_loinc_version
    external_dictionary_type: loinc
```

Output:

```json
[true, true, true, true]
```

## valid_external_dictionary_value

Returns true if the target variable contains a valid external dictionary value, otherwise false

Can be case insensitive by setting `case_sensitive` attribute to false. It is true by default.

Input:

```yaml
Operations:
  - operator: valid_external_dictionary_value
    name: --DECOD
    id: $is_valid_decod_value
    external_dictionary_type: meddra
    dictionary_term_type: PT
    case_sensitive: false
```

Output:

```json
[true, false, false, true]
```

## valid_external_dictionary_code

Returns true if the target variable contains a valid external dictionary code, otherwise false

Input:

```yaml
Operations:
  - operator: valid_external_dictionary_code
    name: --COD
    id: $is_valid_cod_code
    external_dictionary_type: meddra
    dictionary_term_type: PT
```

Output:

```json
[true, false, false, true]
```

## valid_external_dictionary_code_term_pair

Returns true if the row in the dataset contains a matching pair of code and term, otherwise false

For this operator, the name parameter should contain the name of the variable containing the code, and the
external_dictionary_term_variable parameter should contain the name of the variable containing the term
Input:

```yaml
Operations:
  - operator: valid_external_dictionary_code_term_pair
    name: --COD
    id: $is_valid_loinc_code_term_pair
    external_dictionary_type: loinc
    external_dictionary_term_variable: --DECOD
```

Output:

```json
[true, false, false, true]
```

## MedDRA-Specific Operations

## valid_meddra_code_references

Determines whether the values are valid in the following variables:

- `--SOCCD` (System Organ Class Code)
- `--HLGTCD` (High Level Group Term Code)
- `--HLTCD` (High Level Term Code)
- `--PTCD` (Preferred Term Code)
- `--LLTCD` (Lowest Level Term Code)

**Input:**

```yaml
Operations:
  - id: $is_valid_meddra_codes
    operator: valid_meddra_code_references
```

**Output:**

```json
[true, false, true, true]
```

## valid_meddra_code_term_pairs

Determines whether the values are valid in the following variable pairs:

- `--SOCCD`, `--SOC` (System Organ Class Code and Term)
- `--HLGTCD`, `--HLGT` (High Level Group Term Code and Term)
- `--HLTCD`, `--HLT` (High Level Term Code and Term)
- `--PTCD`, `--DECOD` (Preferred Term Code and Dictionary-Derived Term)
- `--LLTCD`, `--LLT` (Lowest Level Term Code and Term)

**Input:**

```yaml
Operations:
  - id: $is_valid_meddra_pairs
    operator: valid_meddra_code_term_pairs
```

**Output:**

```json
[true, true, false, true, true]
```

## valid_meddra_term_references

Determines whether the values are valid in the following variables:

- `--SOC` (System Organ Class)
- `--HLGT` (High Level Group Term)
- `--HLT` (High Level Term)
- `--DECOD` (Dictionary-Derived Term)
- `--LLT` (Lowest Level Term)

**Input:**

```yaml
Operations:
  - id: $is_valid_meddra_terms
    operator: valid_meddra_term_references
```

**Output:**

```json
[true, true, false, true, true]
```

## WHODrug-Specific Operations

## valid_whodrug_references

Checks if a reference to whodrug term in `name` points to the existing code in Atc Text (INA) file.

**Input:**

```yaml
Operations:
  - id: $whodrug_refs_valid
    operator: valid_whodrug_references
```

**Output:**

```json
[true, false, true, true]
```

## whodrug_code_hierarchy

Determines whether the values are valid and in the correct hierarchical structure in the following variables:

- `--DECOD`
- `--CLAS`
- `--CLASCD`

**Input:**

```yaml
Operations:
  - id: $valid_whodrug_codes
    operator: whodrug_code_hierarchy
```
