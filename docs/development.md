# Development

This page covers integrating CORE as a library, building from source, running tests, creating executables, and packaging.

---

## PyPI Integration

CORE is available as a Python package for direct integration into data pipelines.

```bash
pip install cdisc-rules-engine
```

This allows you to:

- Import the rules engine library into your Python projects
- Validate data without requiring XPT format files
- Integrate rules validation into existing pipelines

For implementation details, see [PYPI.md](./PYPI.md).

---

## Environment Setup

**Python 3.12 is required.** Other versions are not supported and may produce unexpected errors or incorrect validation results.

```bash
# Check your Python version
python --version

# Clone the repository
git clone https://github.com/cdisc-org/cdisc-rules-engine.git
cd cdisc-rules-engine

# Create a virtual environment
python -m venv venv

# If you have multiple Python versions, be explicit:
# python3.12 -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
.\venv\Scripts\Activate

# Install dependencies
python -m pip install -r requirements-dev.txt
```

---

## Running Tests

From the project root, run both unit and regression tests:

```bash
python -m pytest tests
```

---

## Creating an Executable

Pre-built executables are available on the [Releases page](https://github.com/cdisc-org/cdisc-rules-engine/releases). If you need to build your own, see [README_Build_Executable.md](../README_Build_Executable.md) in the repository root.

For reference, the PyInstaller commands are:

**Linux:**

```bash
pyinstaller core.py \
  --add-data=venv/lib/python3.12/site-packages/xmlschema/schemas:xmlschema/schemas \
  --add-data=resources/cache:resources/cache \
  --add-data=resources/templates:resources/templates \
  --add-data=resources/jsonata:resources/jsonata
```

**Windows:**

```bash
pyinstaller core.py ^
  --add-data=".venv/Lib/site-packages/xmlschema/schemas;xmlschema/schemas" ^
  --add-data="resources/cache;resources/cache" ^
  --add-data="resources/templates;resources/templates" ^
  --add-data="resources/jsonata;resources/jsonata"
```

The executable is created in the `dist/` folder and does not require Python to be installed on the target machine.

---

## Creating a Python Package (.whl)

All non-Python files must be listed in `MANIFEST.in` to be included in the distribution.

**Unix / Mac:**

```bash
python3 -m pip install --upgrade build
python3 -m build
```

Install locally from the `dist/` folder:

```bash
pip3 install dist/cdisc_rules_engine-{version}-py3-none-any.whl
```

Upload to PyPI:

```bash
python3 -m pip install --upgrade twine
python3 -m twine upload --repository {repository_name} dist/*
```

**Windows:**

```bash
py -m pip install --upgrade build
py -m build
```

Install locally:

```bash
pip install dist\cdisc_rules_engine-{version}-py3-none-any.whl
```

Upload to PyPI:

```bash
py -m pip install --upgrade twine
py -m twine upload --repository {repository_name} dist/*
```

---

## Updating the USDM JSON Schema

CORE validates against USDM JSON Schema versions 3.0 and 4.0. Schema definitions are stored as `.pkl` files in `resources/cache/`:

- `resources/cache/usdm-3-0-schema.pkl`
- `resources/cache/usdm-4-0-schema.pkl`

These are derived from the OpenAPI specs in [`cdisc-org/DDF-RA`](https://github.com/cdisc-org/DDF-RA). To update or add a schema version:

1. Extract the OpenAPI spec for the target tag:

   ```bash
   git --no-pager --git-dir DDF-RA.git show --format=format:"%B" {tag}:Deliverables/API/USDM_API.json > USDM_API_{version}.json
   ```

   Example tag: `v3.0.0`

2. Convert the OpenAPI spec to JSON Schema:

   ```bash
   python scripts/openapi-to-json.py
   ```

3. Convert the JSON Schema to `.pkl`:

   ```bash
   python scripts/json_pkl_converter.py
   ```

4. Place the resulting `.pkl` file in `resources/cache/`.

---

## Dataset Format Reference (JSON)

When validating a single rule with `--local-rules`, JSON datasets must match the Dataset-JSON format used by the rule editor:

```json
{
  "datasets": [
    {
      "filename": "cm.xpt",
      "label": "Concomitant/Concurrent medications",
      "domain": "CM",
      "variables": [
        {
          "name": "STUDYID",
          "label": "Study Identifier",
          "type": "Char",
          "length": 10
        }
      ],
      "records": {
        "STUDYID": ["CDISC-TEST", "CDISC-TEST", "CDISC-TEST", "CDISC-TEST"]
      }
    }
  ]
}
```
