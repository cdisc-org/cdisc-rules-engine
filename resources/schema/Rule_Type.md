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

## Dataset Metadata Check against Define XML

#### Columns

- `dataset_size`
- `dataset_location`
- `dataset_name`
- `dataset_label`
- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_variables`

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
- `define_variable_`...

#### Rule Macro

Attach define xml metadata at variable level

#### Example

```yaml
- name: variable_name
  operator: not_equal_to
  value: define_variable_name
```
