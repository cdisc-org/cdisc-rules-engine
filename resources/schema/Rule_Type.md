## Dataset Contents Check against Define XML and Library Metadata

#### Columns

Columns are the columns within the original dataset

#### Rule Macro

Attach define xml metadata at variable level and library metadata at variable level

## Dataset Metadata Check

#### Columns

- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`

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
- `define_dataset_is_non_standard`
- `define_dataset_key_sequence`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_name`
- `define_dataset_structure`
- `define_dataset_variables`

## Dataset Metadata Check against Define XML

#### Columns

- `dataset_size`
- `dataset_location`
- `dataset_name`
- `dataset_label`
- `dataset_domain`
- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_domain`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_variables`
- `define_dataset_key_sequence`

#### Rule Macro

Allows comparing content metadata to define xml metadata of the same name.

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

## Define Item Group Metadata Check

#### Columns

- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_variables`
- `define_dataset_key_sequence`

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

#### Rule Macro

Attach variable codelist and codelist terms

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
- `library_variable_name`
- `library_variable_order_number`
- `library_variable_label`
- `library_variable_data_type`
- `library_variable_role`
- `library_variable_core`
- `library_variable_ccode`

#### Rule Macro

Attach variable codelist and codelist terms

## Domain Presence Check

#### Columns

Single row contains a column for each domain and the value of that column is the domain's file name

| AE     | EC     |
| ------ | ------ |
| ae.xpt | ec.xpt |

#### Example

```yaml
all:
  - name: PP
    operator: exists
  - name: PC
    operator: not_exists
```

## JSONata

Apply a JSONata query to a JSON file. [JSONata documentation](https://docs.jsonata.org)

### Example

#### Rule

```yaml
Check: |
  **.$filter($, $myutils.equals).{"row":path, "A":A, "B":B}
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

\* Note that in the CLI, you can pass a variable name and directory of such files using `-jcf` or `--jsonata-custom-functions`. The engine's built-in JSONata functions are accessible from the `$utils` variable. For example to load two more directories containing functions into `$myutils` and `$yourutils`, add the options:
`-jcf myutils path/to/myutils -jcf yourutils path/to/yourutils`

```yaml
{
  "equals": function($v){ $v.A=$v.B }
}
```

#### JSON Data

```json
{
  "path": "",
  "A": "same value 1",
  "B": "same value 1",
  "C": {
    "path": "C",
    "A": "different value 1",
    "B": "different value 2",
    "C": { "path": "C.C", "A": "same value 2", "B": "same value 2" }
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
          "row": "C.C",
          "A": "same value 2",
          "B": "same value 2"
        },
        "dataset": "",
        "row": "C.C"
      }
    ]
  }
]
```

### Output Variables and Report column mapping

You can use `Outcome.Output Variables` to specify which properties to display from the result JSON. The following result property names will map to the column names in the Excel output report.

Mapping of Result property names to Report Issue Details Column Names:

| JSONata Result Name | JSON report property | Excel Column |
| ------------------- | -------------------- | ------------ |
| dataset             | dataset              | Dataset      |
| row                 | row                  | Record       |
| SEQ                 | SEQ                  | Sequence     |
| USUBJID             | USUBJID              | USUBJID      |

### Scope

`Scope` should always `Include` `ALL` to ensure the rule will be run. The rule is only run once for the entire JSON file. The `Dataset` determination must come from the rule's jsonata result property.

## Record Data

#### Columns

Columns are the columns within the original dataset

#### Example

```yaml
all:
  - name: --SCAT
    operator: non_empty
  - name: --SCAT
    operator: equal_to
    value: --CAT
```

## Value Check with Dataset Metadata

#### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`

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
    value: define_variable_codelist_coded_values`
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

## Value Level Metadata Check against Define XML

#### Columns

Columns are the columns within the original dataset

#### Rule Macro

Attach define xml metadata at value level

## Variable Metadata Check

#### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_size`
- `variable_data_type`
- `variable_format`

#### Example

```yaml
- name: variable_label
  operator: longer_than
  value: 40
```

## Variable Metadata Check against Define XML

#### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_`...
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

#### Rule Macro

Attach define xml metadata at variable level

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

## Variables Metadata Check against Define XML and Library Metadata

#### Columns:

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
- `library_variable_name`
- `library_variable_role`
- `library_variable_label`
- `library_variable_core`
- `library_variable_order_number`
- `library_variable_data_type`
- `library_variable_ccode`
- `variable_has_empty_values`
