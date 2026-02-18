# Check Parameters

## Overview

Check parameters are configuration elements that define how validation rules are applied within the CDISC rules engine. These parameters control the behavior, scope, and criteria for data validation checks across clinical trial datasets. Each parameter serves a specific purpose in customizing rule logic to ensure data integrity and compliance with CDISC standards.

The rules engine uses these parameters to construct validation logic that can be applied to various datasets to identify data inconsistencies, missing values, invalid formats, and other quality issues.

## Parameter Definitions

### comparator

Specifies the column/variable name that provides the comparison values for validation rules. In the rules engine implementation, this parameter is processed through `replace_prefix()` and used to extract values for comparison operations. For example, in the `has_next_corresponding_record` operator, the comparator column's next row value is compared against the target column's current row value.

```yaml
- name: "VSTESTCD"
  operator: "has_next_corresponding_record"
  target: "VSTESTCD"
  comparator: "VSTESTCD" # Column providing comparison values
  within: "USUBJID"
  ordering: "VISITNUM"
```

### context

### date_component

Specifies which component of a date/datetime variable should be validated. Available options include:

- year
- month
- day
- hour
- minute
- second
- microsecond

### name

Identifies the specific variable or field name that the validation rule targets. This parameter links the rule to the appropriate data element within the dataset.

### negative

Boolean parameter used with the `invalid_duration` operator to specify whether negative durations should be considered valid (True) or invalid (False).

```yaml
- name: "BRTHDTC"
  operator: "invalid_duration"
  negative: False
```

In this example, the rule will flag any negative durations in the DURVAR variable as invalid. If `negative` were set to `true`, negative durations would be considered valid and not raise issues.

### operator

Defines the specific validation operation to be performed. This parameter determines the type of check the engine will execute (e.g., equal_to, not_null, within_range, invalid_duration).

### order

Specifies the sort order for validation results or data processing. Available options:

- asc (ascending)
- dsc (descending)

This parameter helps organize validation outputs and can influence how rules are applied to ordered data.

### ordering

Specifies the column name used to sort data before applying validation rules. In the rules engine, this parameter is processed through `replace_prefix()` and used in `sort_values()` operations to establish the correct sequence for row-by-row comparisons. Critical for operators that depend on data order, such as `has_next_corresponding_record`.

Example usage:

```yaml
ordering: "VISITNUM" # Sort by visit number to ensure proper sequence
```

### separator

Specifies the delimiter character(s) used to split string values into parts for comparison. Used by operators that validate paired data formats to ensure both parts have equal precision or completeness. Default value is "/" (forward slash) if not specified.

Example usage:

```yaml
- name: "--DTC"
  operator: "split_parts_have_equal_length"
  separator: "/" # Split by forward slash for date intervals
```

Used with operators:

- `split_parts_have_equal_length` - Validates that both parts of a split string have equal length
- `split_parts_have_unequal_length` - Validates that parts have different lengths (complement)

### prefix

Specifies a string prefix that should be present at the beginning of a variable's value. Used for format validation and standardization checks.

### suffix

Specifies a string suffix that should be present at the end of a variable's value. Complements prefix validation for comprehensive format checking.

### regex

Specifies a regular expression pattern used to extract portions of string values before performing validation operations.

### target

Specifies the primary column/variable name that the validation rule evaluates. In the rules engine implementation, this parameter is processed through `replace_prefix()` and represents the column whose values are being validated. The target column typically contains the data being checked for compliance or consistency. The results of validation rules are typically reported for the target variable.

### value

Contains the reference value or criteria against which the validation check is performed. The interpretation of this parameter depends on the `value_is_literal` setting.

### value_is_literal

Boolean parameter that signifies whether the string in the value key should be treated as a literal string. When value_is_literal is false or not specified, the string in the value key will be interpreted as a variable name in the dataset.

> IDVAR = "VISIT" as a value, not IDVAR = VISIT as a variable in the dataset

```yaml
- "name": "IDVAR"
  "operator": "equal_to"
  "value": "VISIT"
  "value_is_literal": true
```

# Operation Parameters

The rules engine uses operation parameters defined in the JSON schema to configure how operations execute. These parameters are specified by users in YAML operation definitions and work in conjunction with check parameters to provide comprehensive validation capabilities.

### attribute_name

Specifies the metadata attribute name to extract from variable definitions. Used in operations like `define_variable_metadata` to retrieve specific metadata properties such as variable labels, core status, or other attributes.

### case_sensitive

Boolean flag that controls whether string comparisons in operations should be case-sensitive. Default is `True`. If not explicitly specified, string comparisons will be case-sensitive. Used in external dictionary validation operations to allow flexible matching.

Examples:

Default behavior (case-sensitive):

```yaml
- operator: valid_external_dictionary_value
  # case_sensitive is not specified, so it defaults to True
  case_sensitive: false # Enable case-insensitive matching
```

### codelist

Specifies the name of a controlled terminology codelist for validation operations. Used with codelist-related operations to determine valid values.

### codelist_code

Contains the specific code value within a codelist. Used in operations that need to reference particular codelist entries.

### codelists

List of multiple codelist names for operations that work with multiple controlled terminology codelists simultaneously.

### ct_attribute

Specifies the controlled terminology attribute to retrieve, such as "Term CCODE" or other CT-specific attributes.

### ct_package_type

Single CT package type identifier. Valid values include: "ADAM", "CDASH", "COA", "DDF", "DEFINE-XML", "GLOSSARY", "MRCT", "PROTOCOL", "QRS", "QS-FT", "SDTM", "SEND", "TMF".

### ct_package_types

List of CT package types to include in operations. References the `ct_package_type` enumeration.

### ct_packages

List of multiple controlled terminology packages. Used when operations need to work across multiple CT packages.

### ct_version

Specifies the version of controlled terminology to use in validation operations.

### dictionary_term_type

Classification of external dictionary terms. Valid values include: "LLT", "PT", "HLT", "HLGT", "SOC". Used in MedDRA and other external dictionary operations.

### domain

Specifies the domain or data structure context for the operation. References Dataset or DataStructure definitions.

### external_dictionary_type

Type of external dictionary to use. Currently supports "meddra" for MedDRA validation operations.

### filter

Dictionary containing filter criteria for conditional validation. When specified, operations work only on data subsets that meet the filter criteria.

```yaml
filter:
  AESEV: "SEVERE" # Only process severe adverse events
  AEOUT: "FATAL" # Only process fatal outcomes
```

### filter_key

String parameter used for filtering operations. Works with filter-related functionality.

### filter_value

String parameter that specifies the value to filter by. Used in conjunction with filter operations.

### group

List of variable names used for grouping data before applying operations. Used to ensure operations are applied within appropriate data boundaries.

### group_aliases

Alternative names for grouping variables that allow grouped results to be merged back to datasets using different column names.

```yaml
group:
  - USUBJID
  - IDVARVAL
group_aliases:
  - USUBJID
  - GROUPID # Rename IDVARVAL to GROUPID for merging
```

### id

String identifier for the operation result. Used to reference operation outputs in subsequent rule logic.

### key_name

Specifies the metadata attribute name for filtering operations. Valid values include: "definition", "examples", "label", "name", "notes", "ordinal", "role", "simpleDatatype", "variableCcode".

### key_value

Works with `key_name` to specify the metadata value to filter by. Returns variables that match the specified key-value pair.

```yaml
- operator: get_model_filtered_variables
  key_name: "role"
  key_value: "Timing" # Find variables with role = "Timing"
```

### level

Specifies the level of data to retrieve from controlled terminology operations. Valid values are "codelist" or "term" to determine whether to return codelist-level or term-level data.

### map

List of mapping dictionaries for value transformations. Each dictionary contains input column names as properties and an `output` property specifying the result value.

### name

Variable name that the operation targets. Used to specify which column or variable the operation should process.

### operator

String that specifies the operation type to execute. Must match one of the defined operation types in the schema.

### returntype

Expected return type for operation results. Valid values are "code" (for NCI codes) or "value" (for submission values) in controlled terminology operations.

### source

Either "submission" or "evaluation" for which dataset to check the variable_is_null from. Evaluation is the dataset constructed by
the rule type while submission is the raw dataset submitted that is being evaluated.

### term_code

Terminology code value used in controlled terminology operations for code-based lookups.

### term_value

Terminology term value used in controlled terminology operations for value-based lookups.

### version

Version parameter used in codelist operations that require version-specific processing or validation.

### within

Specifies the column name used for grouping data before applying validation rules. In the rules engine implementation, this parameter is processed through `replace_prefix()` and used in `groupby()` operations to ensure validation logic is applied within appropriate data boundaries (e.g., by subject, by study).

Example usage:

```yaml
within: "USUBJID" # Group by subject ID to keep validations within subject data
```

This parameter is essential for maintaining data integrity boundaries and preventing inappropriate cross-subject or cross-study comparisons.
