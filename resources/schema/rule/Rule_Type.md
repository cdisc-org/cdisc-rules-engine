# Rule Types

Rule Types determine how the engine builds the dataset that rule conditions are evaluated against. Each Rule Type produces a different dataset structure by controlling which data sources are included (submission contents, Define-XML, CDISC Library, or combinations thereof) and at what grain (record, variable, dataset, or value level) you want to check data. Selecting the correct Rule Type is essential — it determines which columns are available to reference in rule conditions.

## Rule Type Descriptions

### Columns

The columns made by the rule type that can have check logic performed on.

### Rule Macro

The combination of data in each rule type.

# Rule Type Groupings

| Group                 | Rule Type                                                                                                                             |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Dataset**           | [Record Data](#record-data)                                                                                                           |
|                       | [Dataset Metadata Check](#dataset-metadata-check)                                                                                     |
|                       | [Dataset Metadata Check against Define XML](#dataset-metadata-check-against-define-xml)                                               |
|                       | [Dataset Contents Check against Define XML](#dataset-contents-check-against-define-xml)                                               |
| **Domain**            | [Domain Presence Check](#domain-presence-check)                                                                                       |
|                       | [Domain Presence Check against Define XML](#domain-presence-check-against-define-xml)                                                 |
| **Define XML**        | [Define Item Metadata Check against Library Metadata](#define-item-metadata-check-against-library-metadata)                           |
| **Value Check**       | [Value Check with Dataset Metadata](#value-check-with-dataset-metadata)                                                               |
|                       | [Value Check with Variable Metadata](#value-check-with-variable-metadata)                                                             |
|                       | [Value Check against Define XML Variable](#value-check-against-define-xml-variable)                                                   |
|                       | [Value Check against Define XML VLM](#value-check-against-define-xml-vlm)                                                             |
| **Variable Metadata** | [Variable Metadata Check](#variable-metadata-check)                                                                                   |
|                       | [Variable Metadata Check against Define XML](#variable-metadata-check-against-define-xml)                                             |
|                       | [Variable Metadata Check against Library Metadata](#variable-metadata-check-against-library-metadata)                                 |
|                       | [Variables Metadata Check against Define XML and Library Metadata](#variables-metadata-check-against-define-xml-and-library-metadata) |
|                       | [Define Item Metadata Check against Library Metadata](#define-item-metadata-check-against-library-metadata)                           |
| **JSON**              | [JSONata](#jsonata)                                                                                                                   |
|                       | [JSON Schema Check](#json-schema-check)                                                                                               |

# Dataset Rule Types

## Record Data

#### Columns

Columns are the columns within the original dataset

#### Rule Macro

Checks record-level data values sourced directly from submission dataset contents.

#### Example

```yaml
all:
  - name: --SCAT
    operator: non_empty
  - name: --SCAT
    operator: equal_to
    value: --CAT
```

## Dataset Metadata Check

#### Columns

- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`

#### Rule Macro

Pairs dataset metadata sourced from the submission contents.

#### Example

```yaml
- name: dataset_name
  operator: longer_than
  value: 6
```

## Dataset Contents Check against Define XML

#### Columns

Columns are the columns within the original dataset along with the following columns:

- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`
- `dataset_domain`
- `define_dataset_class`
- `define_dataset_domain`
- `define_dataset_has_no_data`
- `define_dataset_is_non_standard`
- `define_dataset_key_sequence`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_name`
- `define_dataset_structure`
- `define_dataset_variables`
- `define_dataset_variable_order`

## Dataset Metadata Check against Define XML

Returns a dataset where each dataset is a row in the new dataset. The define xml dataset metadata is attached to each row.

#### Columns

- `dataset_size`
- `dataset_location`
- `dataset_name`
- `dataset_label`
- `dataset_domain`
- `dataset_columns`
- `define_dataset_class`
- `define_dataset_domain`
- `define_dataset_has_no_data`
- `define_dataset_is_non_standard`
- `define_dataset_key_sequence`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_name`
- `define_dataset_structure`
- `define_dataset_variables`
- `define_dataset_variable_order`

#### Rule Macro

Allows comparing dataset metadata against define xml dataset metadata.

#### Example

```yaml
all:
  - name: dataset_name
    operator: not_equal_to
    value: define_dataset_name
```

Or

```yaml
any:
  - name: dataset_name
    operator: empty
  - name: define_dataset_name
    operator: empty
```

## Dataset Contents Check against Define XML

#### Columns

Columns are the columns within the original dataset along with the following columns:

- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`
- `dataset_domain`
- `define_dataset_class`
- `define_dataset_domain`
- `define_dataset_has_no_data`
- `define_dataset_is_non_standard`
- `define_dataset_key_sequence`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_name`
- `define_dataset_structure`
- `define_dataset_variables`

## Define Item Metadata Check

#### Columns

- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_role`
- `define_variable_size`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_is_collected`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_values`
- `define_variable_has_comment`
- `define_variable_has_method`

#### Rule Macro

Pairs record-level data values from the submission datasets with dataset metadata and Define-XML dataset-level (ItemGroup) metadata broadcast across every row. Each row represents one record from the original dataset, preserving all original columns, with the dataset's metadata and Define-XML dataset metadata repeated on each row.

# Variable Metadata Rule Types

## Variable Metadata Check

#### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_size`
- `variable_data_type`
- `variable_format`

#### Rule Macro

Checks variable-level metadata sourced from the submission dataset contents.

#### Example

```yaml
- name: variable_label
  operator: longer_than
  value: 40
```

# Domain Rule Types

## Domain Presence Check

#### Columns

Single row contains a column for each domain and the value of that column is the domain's file name

| AE     | EC     |
| ------ | ------ |
| ae.xpt | ec.xpt |

#### Rule Macro

Checks which dataset files are physically present in the submission contents. Each column represents one domain that exists in the submission, holding the filename as its value.

#### Example

```yaml
all:
  - name: PP
    operator: exists
  - name: PC
    operator: not_exists
```

## Domain Presence Check against Define XML

#### Columns

One row per dataset defined in Define-XML:

- `domain` - The domain if the dataset exists, null otherwise
- `filename` - The file name if dataset exists, null otherwise
- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_domain`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_has_no_data`
- `define_dataset_key_sequence`
- `define_dataset_variables`

#### Rule Macro

Reconciles dataset file presence in the submission against Define-XML dataset declarations. Each row represents one dataset defined in Define-XML, with the corresponding submission filename joined if the file exists.

#### Example

Check if SE domain is defined in Define-XML without HasNoData="Yes" but the dataset file doesn't exist:

```yaml
all:
  - name: define_dataset_name
    operator: equal_to
    value: "SE"
  - name: define_dataset_has_no_data
    operator: equal_to
    value: False
  - name: filename
    operator: not_exists
```

# Define XML Rule Types

## Define Item Metadata Check against Library Metadata

#### Columns

- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_role`
- `define_variable_size`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_is_collected`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_codes`
- `define_variable_mandatory`
- `define_variable_has_comment`
- `define_variable_has_method`
- `library_variable_name`
- `library_variable_order_number`
- `library_variable_label`
- `library_variable_data_type`
- `library_variable_role`
- `library_variable_core`
- `library_variable_has_codelist`
- `library_variable_ccode`

#### Rule Macro

Checks variable-level metadata, codelists, and codelist terms from Define-XML against the corresponding standard variable definitions from the CDISC Library.

# Value Check Rule Types

## Value Check with Dataset Metadata

#### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`

#### Rule Macro

Checks individual cell values from submission dataset contents pivoted to a long format, with dataset-level metadata attached to each row.

#### Example

```yaml
all:
  - name: variable_name
    operator: starts_with
    value: "DM"
  - name: dataset_name
    operator: not_equal_to
    value: "DM"
```

## Value Check with Variable Metadata

#### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `variable_order_number`
- `variable_label`
- `variable_size`
- `variable_data_type`
- `variable_format`
- `variable_value_length`

#### Rule Macro

Checks individual cell values from submission dataset contents pivoted to a long format, with variable-level metadata from the submission dataset attached to each row.

#### Example

```yaml
all:
  - name: variable_data_type
    operator: equal_to
    value: char
  - name: variable_value
    operator: longer_than
    value: 200
```

## Value Check against Define XML Variable

#### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_`...

#### Rule Macro

Checks individual cell values from submission dataset contents pivoted to a long format, with the matching Define-XML variable-level metadata joined to each row.

#### Example

```yaml
all:
  - name: define_variable_ccode
    operator: empty
  - name: variable_value
    operator: non_empty
  - name: define_variable_has_codelist
    operator: equal_to
    value: true
  - name: variable_value
    operator: is_not_contained_by
    value: define_variable_codelist_coded_values
```

## Value Check against Define XML VLM

#### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `define_vlm_name`
- `define_vlm_label`
- `define_vlm_data_type`
- `define_vlm_is_collected`
- `define_vlm_role`
- `define_vlm_size`
- `define_vlm_ccode`
- `define_vlm_format`
- `define_vlm_allowed_terms`
- `define_vlm_origin_type`
- `define_vlm_has_no_data`
- `define_vlm_order_number`
- `define_vlm_length`
- `define_vlm_has_codelist`
- `define_vlm_codelist_coded_values`
- `define_vlm_mandatory`
- `define_variable_name`
- `type_check`
- `length_check`
- `variable_value_length`

#### Rule Macro

Checks individual cell values from submission dataset contents pivoted to a long format, with the matching Define-XML Value Level Metadata (VLM) joined to each row. Only rows where VLM exists for that variable are produced — records without matching VLM are excluded.

#### Example

```yaml
all:
  - name: define_vlm_ccode
    operator: empty
  - name: variable_value
    operator: non_empty
  - name: define_vlm_has_codelist
    operator: equal_to
    value: true
  - name: variable_value
    operator: is_not_contained_by
    value: define_vlm_codelist_coded_values
```

```yaml
all:
  - name: variable_value
    operator: empty
  - name: define_vlm_mandatory
    operator: equal_to
    value: Yes
```

## Variable Metadata Check against Define XML

#### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_size`
- `variable_data_type`
- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_is_collected`
- `define_variable_role`
- `define_variable_size`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_length`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_codes`
- `define_variable_mandatory`
- `define_variable_has_comment`
- `define_variable_has_method`

#### Rule Macro

Combines variable-level metadata from submission dataset contents against the matching variable definitions in Define-XML.

#### Example

```yaml
- name: variable_name
  operator: not_equal_to
  value: define_variable_name
```

## Variable Metadata Check against Library Metadata

#### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_size`
- `variable_data_type`
- `variable_format`
- `variable_has_empty_values`
- `library_variable_name`
- `library_variable_role`
- `library_variable_label`
- `library_variable_core`
- `library_variable_order_number`
- `library_variable_data_type`
- `library_variable_ccode`

#### Rule Macro

Combines variable-level metadata from submission dataset contents against the corresponding CDISC Library standard variable metadata.

## Variables Metadata Check against Define XML and Library Metadata

#### Columns

- `variable_name`
- `variable_label`
- `variable_size`
- `variable_order_number`
- `variable_data_type`
- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_is_collected`
- `define_variable_role`
- `define_variable_size`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_length`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_codes`
- `define_variable_mandatory`
- `define_variable_has_comment`
- `define_variable_has_method`
- `library_variable_name`
- `library_variable_role`
- `library_variable_label`
- `library_variable_core`
- `library_variable_order_number`
- `library_variable_data_type`
- `library_variable_ccode`
- `variable_has_empty_values`

#### Rule Macro

Combines variable-level metadata from submission dataset contents against both Define-XML variable metadata and CDISC Library standard variable metadata simultaneously.

# JSON Rule Types

## JSONata

Apply a JSONata query to a JSON file. [JSONata documentation](https://docs.jsonata.org)

### Example

#### Rule

```yaml
Check: |
  **.$filter($, $myutils.equals).{"row":_path, "A":A, "B":B}
Core:
  Id: JSONATA Test
  Status: Draft
Outcome:
  Message: "A equals B"
  Output Variables:
    - row
    - A
    - B
Rule Type: JSONata
Scope:
  Entities:
    Include:
      - ALL
Sensitivity: Record
```

#### Custom user function contained in external file "equals.jsonata"

\* Note that in the CLI, you can pass a variable name and directory of such files using `-jcf` or `--jsonata-custom-functions`. The engine's built-in JSONata functions are accessible from the `$utils` variable (see [JSONata Functions](JSONata_Functions.md)). For example to load two more directories containing functions into `$myutils` and `$yourutils`, add the options:
`-jcf myutils path/to/myutils -jcf yourutils path/to/yourutils`

```yaml
{
  "equals": function($v){ $v.A=$v.B }
}
```

#### JSON Data

```json
{
  "A": "same value 1",
  "B": "same value 1",
  "C": {
    "A": "different value 1",
    "B": "different value 2",
    "C": { "A": "same value 2", "B": "same value 2" }
  }
}
```

#### Result

```json
[
  {
    "executionStatus": "success",
    "dataset": "",
    "domain": "",
    "variables": ["A", "B", "row"],
    "message": "A equals B",
    "errors": [
      {
        "value": { "row": "", "A": "same value 1", "B": "same value 1" },
        "dataset": "",
        "row": ""
      },
      {
        "value": {
          "row": "/C/C",
          "A": "same value 2",
          "B": "same value 2"
        },
        "dataset": "",
        "row": "/C/C"
      }
    ]
  }
]
```

### Preprocessing

When the JSONata Rule Type is used, the input JSON file will be preprocessed to assign a `_path` attribute to each node in the JSON tree. The syntax for this path value will use the [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) syntax. This `_path` attribute can be referenced throughout the JSONata query.

### Output Variables and Report column mapping

You can use `Outcome.Output Variables` to specify which properties to display from the result JSON. The following result property names will map to the column names in the Excel output report.

Mapping of Result property names to Report Issue Details Column Names:

| JSONata Result Name | JSON report property | Excel Column |
| ------------------- | -------------------- | ------------ |
| dataset             | dataset              | Dataset      |
| row                 | row                  | Record       |
| SEQ                 | SEQ                  | Sequence     |
| USUBJID             | USUBJID              | USUBJID      |
| entity              | entity               | Entity       |
| instance_id         | instance_id          | Instance ID  |
| path                | path                 | Path         |

### Scope

A JSONata rule will always run once for the entire JSON file, regardless of the Scope. The `Entity` determination must come from the rule's JSONata result property.

## JSON Schema Check

#### Columns

- `json_path`
- `error_attribute`
- `error_value`
- `validator`
- `validator_value`
- `message`
- `dataset`
- `id`
- `_path`
