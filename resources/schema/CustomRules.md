# Custom Editor Columns and Custom Rules

This guide explains how to extend CDISC rule definitions with custom attributes and how to access these attributes using the custom columns editor syntax.

## Table of Contents

- [Custom Columns in Editor Syntax](#custom-columns-in-editor-syntax)
  - [Overview and Basic Syntax](#overview-and-basic-syntax)
  - [Identifying Arrays in YAML](#identifying-arrays-in-yaml)
  - [Identifying Arrays in JSON](#identifying-arrays-in-json)
- [Custom Attributes Overview](#overview)
- [What Can and Cannot Be Changed](#what-can-and-cannot-be-changed)
- [Custom Attributes](#custom-attributes)
- [Custom Columns in Editor Syntax](#custom-columns-in-editor-syntax)
- [Adding a Custom Organization](#adding-a-custom-organization)
- [Example Rule with Custom Attributes](#example-rule-with-custom-attributes)
- [Filtering Rules with Custom Columns](#filtering-rules-with-custom-columns)
- [Validation](#validation)
- [Best Practices](#best-practices)
- [FAQ](#faq)

## Custom Columns in Editor Syntax

Custom columns allow you to query nested rule structures, including arrays, using a simple path syntax. This allows for better access and filtering for both standard and custom attributes.

## Overview and Basic Syntax

Custom columns allow you to query nested rule structures, including arrays, using a simple path syntax. All custom column paths use `.` to separate each nested object property of the rule and use `@` to denote array elements.

The general rules for constructing paths:

1. The path is **case-sensitive**
2. The `@` symbol must come directly before the array name
3. Array notation must be used to access array elements
4. The path must reflect the exact structure of your data

### Identifying Arrays in YAML

When viewing a YAML document, you can identify arrays by looking for these indicators:

1. **Hyphen (-) at the Start**: Arrays elements are marked with a leading hyphen

```yaml
Authorities: #This is an array
  - Organization: "Org1" # This is an element within the array
    Standards: #This is an array (within the Authorities Array)
      - Name: "Standard1" # This is also an element within the array
      - Name: "Standard2" # Another element within the array
```

This means the Authorities array contains elements with Organization and Standards Properties on them. Similarly, Standards is an array with each element in the array having a Name property.

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

The presence of hyphens (-) is your key indicator that you need to use @ notation in your custom path

### Identifying Arrays in JSON

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

Use: `Check.@all.operator` All is an array, containing objects with operator keys

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

# CDISC Custom Rule Extensions

This guide explains how to extend CDISC rule definitions with your own custom attributes while maintaining compatibility with the core CDISC schema structure, and how to access these attributes using the custom columns editor syntax.

## Overview

The CDISC Rules Engine schema supports custom extensions to help organizations better categorize, manage, and filter rules. These extensions maintain compatibility with standard CDISC rule definitions while adding organizational metadata tailored to your specific needs. The custom columns editor syntax allows you to query and filter these properties using a simple path notation.

## What Can and Cannot Be Changed

### Required Structure (Cannot Be Changed)

The following core components of the CDISC schema **must be preserved**:

- **Authorities structure** - The basic structure of the Authorities array
- **Core CDISC properties** - Required properties like Check, Core, Description, Outcome, etc.
- **Data types** - Retain the data types of core properties
- **Required fields** - All required fields of a valid rule must remain present. These are Authorities, Check, Core, Description, Outcome, Executability, Rule Type, Scope, Sensitivity

### Customizable Elements (Can Be Added)

You can extend the schema by adding:

- **Custom root-level properties** - Add new properties for categorization and filtering
- **Custom organization** - Define rules under your own organization name
- **Custom fields object** - Add flexible metadata within the CustomFields property

## Custom Attributes

You can add custom attributes to custom rules. Some Example of properties added are:

| Attribute         | Type             | Description                                                     |
| ----------------- | ---------------- | --------------------------------------------------------------- |
| `Sponsor`         | String           | The sponsor organization for the rule                           |
| `Vendors`         | Array of strings | Vendors associated with the rule                                |
| `TherapeuticArea` | String or Array  | Therapeutic area(s) the rule applies to                         |
| `Trial`           | String or Array  | Specific trial(s) where the rule applies                        |
| `Purpose`         | String           | The business purpose of the rule                                |
| `Keywords`        | Array of strings | Custom keywords for filtering and searching                     |
| `Categories`      | Array of strings | Categorize rules (e.g., "Data Structure", "Required Variables") |
| `CustomFields`    | Object           | Flexible key-value pairs for additional metadata                |

### Example of Custom Attributes

```json
{
  "Sponsor": "Pharma Company XYZ",
  "Vendors": ["CRO Alpha", "Data Management Beta"],
  "TherapeuticArea": ["Oncology", "Immunology"],
  "Trial": ["ABC-123", "DEF-456"],
  "Purpose": "Ensure compliance with internal data standards",
  "Keywords": ["STUDYID", "required", "core", "identification"],
  "Categories": [
    "Data Structure",
    "Required Variables",
    "Study Identification"
  ],
  "CustomFields": {
    "TeamResponsible": "Data Standards Team",
    "LastReviewDate": "2025-02-01",
    "JiraTicket": "ID-245"
  }
}
```

## Example Rule with Custom Attributes

Here's a complete example of a rule with custom attributes:

```json
{
  "Authorities": [
    {
      "Organization": "Pharma XYZ",
      "Standards": [
        {
          "Name": "Internal Standard",
          "Version": "2.1",
          "References": [
            {
              "Origin": "Internal Conformance Rules",
              "Rule Identifier": {
                "Id": "INT001",
                "Version": "1"
              },
              "Version": "1.0",
              "Criteria": {
                "Type": "Success",
                "Plain Language Expression": "Dataset must include a variable named STUDYID"
              }
            }
          ]
        }
      ]
    }
  ],
  "Check": {
    "all": [
      {
        "name": "checkStudyId",
        "type": "existence_check",
        "variable_name": "STUDYID"
      }
    ]
  },
  "Core": {
    "Status": "Draft",
    "Version": "1"
  },
  "Description": "Custom rule to verify STUDYID exists in datasets",
  "Outcome": {
    "Message": "Dataset must include STUDYID variable"
  },
  "Executability": "Fully Executable",
  "Rule Type": {
    "Authority": "Non-Regulatory",
    "Category": "Metadata",
    "Subcategory": "Variable Presence"
  },
  "Scope": {
    "Classes": {
      "Include": ["ALL"]
    },
    "Domains": {
      "Include": ["ALL"]
    }
  },
  "Sensitivity": {
    "Severity": "Error"
  },

  // Custom attributes
  "Sponsor": "Pharma XYZ",
  "Vendors": ["CRO Alpha", "Data Management Beta"],
  "TherapeuticArea": ["Oncology"],
  "Trial": ["ONC-2025-01", "ONC-2025-02"],
  "Purpose": "Ensure study identification is consistent across datasets",
  "Keywords": ["core", "identification", "required", "STUDYID"],
  "Categories": ["Data Structure", "Required Variables"],
  "CustomFields": {
    "TeamResponsible": "Data Standards Team",
    "LastReviewDate": "2025-02-01",
    "ReviewCycle": "Annual",
    "ApplicablePhases": ["Phase 1", "Phase 2", "Phase 3", "Phase 4"]
  }
}
```

## Filtering Rules with Custom Columns

The custom columns editor syntax allows you to create powerful filters based on your custom attributes. Here are some common filtering scenarios:

### Setting Up Custom Columns

1. Add a custom column using the column selector
2. Enter the path to the attribute you want to display/filter
3. Set the column name

### Filter Examples

| To Filter By           | Custom Column Path                                       | Filter Value Example  |
| ---------------------- | -------------------------------------------------------- | --------------------- |
| Sponsor                | `Sponsor`                                                | "Pharma XYZ"          |
| Specific vendor        | `@Vendors`                                               | "CRO Alpha"           |
| Therapeutic area       | `@TherapeuticArea`                                       | "Oncology"            |
| Trial                  | `@Trial`                                                 | "ONC-2025-01"         |
| Category               | `@Categories`                                            | "Required Variables"  |
| Keyword                | `@Keywords`                                              | "STUDYID"             |
| Team responsible       | `CustomFields.TeamResponsible`                           | "Data Standards Team" |
| Authority organization | `@Authorities.Organization`                              | "Pharma XYZ"          |
| Standard name          | `@Authorities.@Standards.Name`                           | "Internal Standard"   |
| Rule ID                | `@Authorities.@Standards.@References.Rule_Identifier.Id` | "INT001"              |

### Complex Filtering

Combine multiple custom columns to create complex filters:

1. Add column for `@TherapeuticArea` and filter for "Oncology"
2. Add column for `@Categories` and filter for "Data Structure"
3. Add column for `Sponsor` and filter for "Pharma XYZ"

This will show all oncology data structure rules from the specific sponsor.

## Validation

The schema validation system has been updated to accept these custom attributes. To validate your rules:

1. Use the schema endpoint to get the latest schema: `/api/schema`
2. Validate your rules against the returned schema
3. Make sure all required core fields are still present

## Best Practices

When adding custom attributes to rules:

1. **Be consistent** - Use the same attribute names and values across rules
2. **Document conventions** - Create internal documentation for your custom attributes
3. **Use controlled vocabularies** - For fields like TherapeuticArea, maintain a list of standard terms
4. **Consider hierarchies** - Categorize using hierarchical structures when appropriate
5. **Avoid redundancy** - Don't duplicate information already in core CDISC properties
6. **Path naming** - When adding custom columns, use clear names that indicate the data being displayed

## FAQ

### Will custom attributes affect rule execution?

No, custom attributes are purely for organizational purposes and do not affect rule execution or validation results.

### Can I use custom attributes with standard CDISC/FDA rules?

Yes, you can add custom attributes to any rule, including standard CDISC and FDA rules.

### How do I access arrays of values in the custom columns?

Use the `@` symbol before the array property name, e.g., `@Vendors` or `@TherapeuticArea`.

### Can I modify the core structure of a CDISC rule?

No, the core structure and required fields of CDISC rules must be preserved. You can only add new properties, not modify or remove existing ones.

### What if I need to track additional information not listed in the examples?

Use the `CustomFields` object for any organization-specific metadata that doesn't fit into the predefined custom attributes. Access these fields with dot notation, e.g., `CustomFields.YourCustomProperty`.

### How should I handle rule versioning with custom attributes?

Custom attributes should follow the same versioning strategy as the rule itself. When a rule is updated, ensure all custom attributes are also reviewed and updated.

### Can I search for multiple values within an array property?

Yes, when filtering in the editor you can search for any value that appears in the array. For example, filtering for "Oncology" in `@TherapeuticArea` will find all rules that include Oncology in their therapeutic areas.
