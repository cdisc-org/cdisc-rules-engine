# Schema Markdown Merge Tool

This directory contains a script to merge JSON schema files with their corresponding markdown descriptions.

## Overview

The `merge_schema_markdown.py` script:
- Reads JSON schema files from `resources/schema/rule/`
- Finds matching markdown files (e.g., `Operator.json` → `Operator.md`)
- Parses markdown sections (headers starting with `##` or `###`)
- Adds `markdownDescription` properties to schema items where `const` values match section names
- Outputs merged schemas to `resources/schema/rule-merged/`

## Usage

### Manual Execution

Run the script locally:

```bash
python scripts/merge_schema_markdown.py
```

This will process all schema files and output merged versions to `resources/schema/rule-merged/`.

### Automatic Execution

The GitHub Action workflow (`.github/workflows/merge-schema-markdown.yml`) automatically runs when:
- Files in `resources/schema/rule/` are changed
- On pull requests or pushes to main/dev branches

The workflow will:
- Run the merge script
- Commit and push merged schemas back to the branch (for same-repo PRs and direct pushes)
- Add a comment on forked PRs instructing contributors to run the script locally

## How It Works

The script follows the logic from the [conformance-rules-editor](https://github.com/cdisc-org/conformance-rules-editor/blob/main/api/utils/addMarkdownDescriptions.ts):

1. **Parse Markdown**: Extract sections from `.md` files where section names are defined by `##` or `###` headings
2. **Traverse JSON**: Recursively search for `const` properties in schema files
3. **Add Descriptions**: When a `const` value matches a markdown section name, add a `markdownDescription` property to that object
4. **Preserve Structure**: Maintain all original schema properties and formatting

## Example

Given `Rule_Type.json`:
```json
{
  "anyOf": [
    {
      "const": "Record Data",
      "title": "Content data at record level"
    }
  ]
}
```

And `Rule_Type.md`:
```markdown
## Record Data

#### Columns
Columns are the columns within the original dataset
```

The output `rule-merged/Rule_Type.json` will be:
```json
{
  "anyOf": [
    {
      "const": "Record Data",
      "title": "Content data at record level",
      "markdownDescription": "\n#### Columns\nColumns are the columns within the original dataset\n"
    }
  ]
}
```

## Results

The script successfully processes:
- 12 JSON schema files
- 223 total markdown descriptions added
- Handles complex nested structures (e.g., `Operator.json`)
