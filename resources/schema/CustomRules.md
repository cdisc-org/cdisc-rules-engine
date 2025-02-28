# Custom Rules

## Custom Columns in Editor Syntax

## Overview and Basic Syntax

Custom columns allow you to query nested rule structures, including arrays, using a simple path syntax. All custom column paths use `.` to separate each nested object property of the rule and use `@` to denote array elements.

## Identifying Arrays in YAML

When viewing a YAML document, you can identify arrays by looking for these indicators:

1. **Hyphen (-) at the Start**: Arrays elements are marked with a leading hyphen

```yaml
Authorities:
  - Organization: "Org1" # This is an array element
    Standards:
      - Name: "Standard1" # This is also an array element
      - Name: "Standard2" # Another array element
```

2. **No Hyphen**: Regular object properties don't have a hyphen

```yaml
Core:
  Id: "123"
  Status: "Active"
```

### Example YAML with Path Mapping

```yaml
Authorities:
  - Organization: "Org1" # @Authorities.Organization
    Standards:
      - Name: "Standard1" # @Authorities.@Standards.Name
Core:
  Id: "123" # Core.Id
  Status: "Active" # Core.Status
```

The presence of hyphens (-) is your key indicator that you need to use @ notation in your custom path.

## Data Structure Examples

You can also inspect the rule json looking for `[` square brackets to denote arrays.

### Single Array Example

For data structured like:

```json
{
  "Check": {
    "all": [{ "operator": "equal_to" }, { "operator": "not_equal_to" }]
  }
}
```

Use: `Check.@all.operator`

### Nested Arrays Example

For data structured like:

```json
{
  "Authorities": [
    {
      "Organization": "Org1",
      "Standards": [{ "Name": "Standard1" }, { "Name": "Standard2" }]
    }
  ]
}
```

Use: `@Authorities.@Standards.Name`

## Rules

1. The path is **case-sensitive**
2. The `@` symbol must come directly before the array name
3. Array notation must be used to access array elements
4. The path must reflect the exact structure of your data

#Custom Rule Schema

coming soon...
