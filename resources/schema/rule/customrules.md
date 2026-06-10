# CDISC Custom Rule Extensions

This guide explains how to extend CDISC rule definitions with your own custom attributes while maintaining compatibility with the core CDISC schema structure.

## Table of Contents

- [Custom Attributes Overview](#overview)
- [What Can and Cannot Be Changed](#what-can-and-cannot-be-changed)
- [Custom Schema Attributes](#custom-attributes)
- [Adding a Custom Organization](#adding-a-custom-organization)
- [Example Rule with Custom Attributes](#example-rule-with-custom-attributes)
- [Validation](#validation)
- [Best Practices](#best-practices)
- [FAQ](#faq)

## Overview

The CDISC Rules Engine schema supports custom extensions to help organizations better categorize, manage, and filter rules. These extensions maintain compatibility with standard CDISC rule definitions while adding organizational metadata tailored to your specific needs.

## What Can and Cannot Be Changed

### Required Structure (Cannot Be Changed)

The following core components of the CDISC schema **must be preserved**:

- **Authorities structure** - The basic structure of the Authorities array
- **Core CDISC properties** - Required properties like Check, Core, Description, Outcome, etc.
- **Data types** - Retain the data types of core properties
- **Required fields** - All required fields of a valid rule must remain present. These are Authorities, Check, Core, Description, Outcome, Executability, Rule Type, Scope, Sensitivity

### Customizable Elements (Can Be Added)

You can extend the schema by adding:

- **Custom organization** - Define rules under your own organization name
- **Custom Category object** - Add metadata within the Category property in the Authorities section
- **Custom attributes** - Define your own properties within the Category object

## Custom Attributes

### Predefined Category Properties

The Category object in the custom organization schema includes these predefined properties (which can be removed at your discretion):

| Attribute            | Type             | Description                                        |
| -------------------- | ---------------- | -------------------------------------------------- |
| `Sponsors`           | Array of strings | The sponsor organizations for the rule             |
| `Vendors`            | Array of strings | Vendors associated with the rule                   |
| `TherapeuticAreas`   | Array of strings | Therapeutic area(s) the rule applies to            |
| `Trials`             | Array of strings | Specific trial(s) where the rule applies           |
| `Purpose`            | String           | The business purpose of the rule                   |
| `CompanyRuleLibrary` | Boolean          | Whether the rule is part of a company rule library |
| `OutputType`         | String           | Output type (Check/Listing)                        |
| `Keywords`           | Array of strings | Custom keywords for filtering and searching        |

### Adding Your Own Custom Properties

#### Extending the Category Object in YAML Files

You can extend the Category object with your own custom properties in your rule YAML files. The schema uses `additionalProperties: true` to allow any additional properties you need:

1. **Simple Properties**: Add any string, number, boolean, or array property:

```yaml
Category:
  MyCustomProperty: "Custom value"
  PriorityLevel: 1
  IsRequired: true
```

2. **Complex Properties**: Add nested objects as needed:

```yaml
Category:
  ReviewInfo:
    LastReviewer: "John Doe"
    ReviewDate: "2025-02-15"
    Comments: "Approved with minor changes"
```

3. **Arrays of Complex Objects**: For more structured data:

```yaml
Category:
  ValidationHistory:
    - Date: "2024-12-01"
      System: "Test Environment"
      Result: "Pass"
    - Date: "2025-01-15"
      System: "Production"
      Result: "Pass"
```

#### Modifying the Schema to Support Custom Properties

If you want to formally define custom properties in your schema (recommended for validation and documentation purposes), follow these steps:

1. **Extend the Organization_Custom.json Schema**:

```json
{
  "$id": "Organization_Custom.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "Organization": {
      "type": "string",
      "description": "Name of your custom organization"
    },
    "Standards": {
      // existing standards definitions...
    },
    "Category": {
      "type": "object",
      "description": "Custom categorization for rule governance",
      "properties": {
        // Standard properties
        "Sponsors": {
          "type": "array",
          "description": "List of sponsors the rule applies to",
          "items": {
            "type": "string"
          }
        },
        // ... other standard Category properties ...

        // Your custom properties - add formal definitions here
        "Department": {
          "type": "string",
          "description": "Department responsible for the rule",
          "enum": ["Clinical", "Data Management", "Biostatistics", "Regulatory"]
        },
        "ReviewCycle": {
          "type": "string",
          "description": "Frequency of rule review",
          "enum": ["Monthly", "Quarterly", "Biannually", "Annually"]
        },
        "BusinessImpact": {
          "type": "string",
          "description": "Business impact of rule failures",
          "enum": ["Low", "Medium", "High", "Critical"]
        },
        "ValidationHistory": {
          "type": "array",
          "description": "History of validation events",
          "items": {
            "type": "object",
            "properties": {
              "Date": { "type": "string", "format": "date" },
              "Validator": { "type": "string" },
              "Status": {
                "type": "string",
                "enum": ["Pending", "In Review", "Approved", "Rejected"]
              }
            },
            "required": ["Date", "Status"]
          }
        }
      },
      // Still allow additional properties beyond those defined
      "additionalProperties": true
    }
  },
  "required": ["Organization", "Standards", "Category"],
  "type": "object"
}
```

2. **Version Control Your Schema Extensions**:
   - Document changes to the schema in a changelog within the schema file itself
   - Metadata object withing `$def` object can be updated to user's specifications
   - Use semantic versioning for your custom schema extensions (MAJOR.MINOR.PATCH)
   - Include a metadata section with versioning information:

```json
"metadata": {
  "schemaVersion": "1.0.0",
  "releaseDate": "2025-03-09",
  "changelog": [
    {
      "version": "1.0.0",
      "date": "2025-03-09",
      "description": "Initial release of Custom Organization Schema",
      "changes": [
        "Added Category object structure with standard properties",
        "Implemented support for Sponsors, Vendors, TherapeuticAreas arrays",
        "Added support for CompanyRuleLibrary boolean and OutputType enum",
        "Enabled extensibility with additionalProperties: true"
      ]
    }
  ],
  "maintainer": {
    "name": "Your Organization Name",
    "email": "standards@yourorganization.com"
  }
}
```

- Ensure backwards compatibility when possible

3. **Create Schema Documentation**:
   - Document your custom properties with descriptions and examples
   - Provide validation rules or constraints for each property
   - Share the documentation with all teams that will be using the rules

## Example Rule with Custom Attributes

Here's a complete example of a rule with custom attributes in the Category section:

```yaml
Authorities:
  - Organization: "MyCompany"
    Standards:
      - Name: "Internal Standard"
        References:
          - Criteria:
              Plain Language Expression: "Dataset must include a variable named STUDYID"
              Type: "Success"
            Origin: "Internal Conformance Rules"
            Rule Identifier:
              Id: "INT001"
              Version: "1"
            Version: "1.0"
        Version: "2.1"
    Category:
      CompanyRuleLibrary: true
      CustomAttribute: "This is a custom property"
      Keywords:
        - "word1"
        - "word2"
      OutputType: "Check"
      Purpose: "RAW data validation"
      Sponsors:
        - "Sponsor A"
        - "Sponsor B"
      TherapeuticAreas:
        - "Oncology"
        - "Immunology"
      Trials:
        - "ONC-2025-01"
        - "ONC-2025-02"
      Vendors:
        - "CRO A"
        - "Data Management B"
Check:
  all:
    - name: $records_in_dataset
      operator: equal_to
      value: 0
Core:
  Id: custom_rule001
  Status: Draft
  Version: "1"
Description: "Custom rule to verify STUDYID exists in datasets"
Executability: Fully Executable
Operations:
  - id: $records_in_dataset
    operator: record_count
Outcome:
  Message: "Your data is weak."
Rule Type: Record Data
Scope:
  Classes:
    Include:
      - "ALL"
  Domains:
    Include:
      - "ALL"
Sensitivity: Record
```

## Best Practices

When adding custom attributes to rules:

1. **Be consistent** - Use the same attribute names and values across rules
2. **Document conventions** - Create internal documentation for your custom attributes
3. **Use controlled vocabularies** - For fields like TherapeuticAreas, maintain a list of standard terms
4. **Consider hierarchies** - Categorize using hierarchical structures when appropriate
5. **Avoid redundancy** - Don't duplicate information already in core CDISC properties

## Validation

When using custom attributes, it's important to ensure your rules remain valid:

1. **Schema validation** - Test your rules against the Organization_Custom.json schema
2. **Category placement** - Ensure the Category object is correctly placed under the Authorities item, at the same level as Organization and Standards
3. **Required properties** - Make sure all required properties are still present in your rules
4. **Data types** - Use the correct data types for each property as defined in the schema

## FAQ

### Will custom attributes affect rule execution?

It depends on which attributes:

- **Organization, Standards, Custom Rule IDs, and Version**: These values may impact rule execution.
- **Category properties**: The properties within the Category object (Sponsors, Vendors, TherapeuticAreas, etc.) are purely for organizational and filtering purposes and do not affect rule execution or validation logic.

### How do I execute custom rules?

Currently, custom rules can be executed using the local rules option (`-lr`) when running the validation engine. However:

- The engine currently disregards the `-s` (standard) and `-v` (version) options when executing local rules
- All rules in the local folder will be executed regardless of standard/version
- Rules need a Core: Id property for execution.
- We are working on further support for custom rule properties and execution.
- This will allow for more targeted execution of custom rules based on specified criteria

### Can I use custom attributes with standard CDISC/FDA rules?

Yes, but only within your internal systems. You can add custom attributes to standard CDISC and FDA rules in your local rule repository for internal categorization and filtering. However:

- CDISC will maintain their official rules free of these custom attributes
- Custom attributes should not be included when contributing rules back to CDISC
- When importing updated rules from CDISC, you'll need to re-apply your custom attributes
- Your internal rule management system should handle the separation between official CDISC/FDA rule content and your organization's custom attributes

### Can I modify the core structure of a CDISC rule?

No, the core structure and required fields of CDISC rules must be preserved. You can only add new properties, not modify or remove existing ones.

### What if I need to track additional information not listed in the examples?

Add or remove any property you need or don't need directly to the Category object. The schema is designed to allow additional properties beyond the predefined ones.

### How do I add a new property to the schema itself?

Edit your Organization_Custom.json file to add the property definition under the Category object's properties section. Define the property type, description, and any constraints (like enum values). Keep the `additionalProperties: true` to maintain flexibility or don't.

### Do I need to update the schema every time I add a custom property to a rule?

No. The schema with `additionalProperties: true` allows any property to be added to rule YAML files without updating the schema. However, formally defining properties in the schema provides better documentation.

### Why must the Category be under Authorities and not at the root level?

The Category object needs to be placed within each Authorities item because:

1. **Schema validation** - The Organization_Custom.json schema expects to find Category at this level
2. **Organization association** - This structure associates the custom attributes directly with the specific organization
3. **Multiple authorities** - In cases where a rule might have multiple authorities, each can have its own Category object with organization-specific attributes
