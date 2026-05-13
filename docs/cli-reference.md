# CLI Reference

> Throughout this reference, examples use `python core.py`. If you're using the pre-built executable, replace this with `.\core.exe` (Windows) or `./core` (Linux/Mac).

---

## `validate`

Run conformance validation against a CDISC standard.

```bash
python core.py validate --help
```

### Required Flags

| Flag                      | Description                                                                                   |
| ------------------------- | --------------------------------------------------------------------------------------------- |
| `-s, --standard TEXT`     | CDISC standard to validate against (e.g. `sdtmig`, `tig`). Also via `PRODUCT` env var.        |
| `-v, --version TEXT`      | Standard version (e.g. `3-4`). Also via `VERSION` env var.                                    |
| `-ss, --substandard TEXT` | **Required for TIG.** One of `SDTM`, `SEND`, `ADaM`, `CDASH`. Also via `SUBSTANDARD` env var. |
| `-uc, --use-case TEXT`    | Use Case for TIG Custom Domains. When performing a TIG validation with custom domain(s), this |
|                           | must be given to identify the custom domains' use case One of `INDH`, `PROD`, `NONCLIN`,      |
|                           | `ANALYSIS`. Also via `USE_CASE` env var.                                                      |

### Dataset Input

| Flag                           | Description                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `-d, --data TEXT`              | Path to directory containing dataset files. Only the last value is used if specified multiple times.                |
| `-dp, --dataset-path TEXT`     | Absolute path to a single dataset file. Can be specified multiple times.                                            |
| `-dxp, --define-xml-path TEXT` | Path to Define-XML. Also via `DEFINE_XML` env var.                                                                  |
| `-ft, --filetype TEXT`         | File extension filter applied to the `-d` directory (e.g. `xpt`). Has higher priority than --dataset-path parameter |
| `-e, --encoding TEXT`          | File encoding for reading datasets (default: `utf-8`). Common values: `cp1252`, `latin-1`, `utf-16`.                |
| `-vcp, --variables-csv-path`   | Path to `_variables.csv` when using multiple `-dp` paths across different folders.                                  |
| `-dcp, --datasets-csv-path`    | Path to `_datasets.csv`. Required when multiple `-dp` paths refer to different folders.                             |

### Rules Selection

| Flag                                    | Description                                                                 |
| --------------------------------------- | --------------------------------------------------------------------------- |
| `-r, --rules TEXT`                      | Validate only specific rule(s) by CORE ID (e.g. `CORE-000001`). Repeatable. |
| `-er, --exclude-rules TEXT`             | Exclude specific rule(s) by CORE ID. Repeatable.                            |
| `-lr, --local-rules TEXT`               | Path to directory or file containing local rule YAML/JSON files.            |
| `-cs, --custom-standard`                | Use a custom standard (uploaded to cache via `update-cache`).               |
| `-cse, --custom-standard-encoding TEXT` | Encoding for custom standard JSON files (auto-detected if omitted).         |

### Controlled Terminology

| Flag                                         | Description                                                                                                                                                               |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-ct, --controlled-terminology-package TEXT` | CT package(s) to validate against. Repeatable. If Define-XML 2.1 is provided, CT is taken from the define. Also via `CT` env var (`:` separated on Unix, `;` on Windows). |

### External Dictionaries

| Flag                    | Description                             |
| ----------------------- | --------------------------------------- |
| `--whodrug TEXT`        | Path to WHODrug dictionary files.       |
| `--meddra TEXT`         | Path to MedDRA dictionary files.        |
| `--loinc TEXT`          | Path to LOINC dictionary files.         |
| `--medrt TEXT`          | Path to MedRT dictionary files.         |
| `--unii TEXT`           | Path to UNII dictionary files.          |
| `--snomed-version TEXT` | SNOMED CT version (e.g. `2024-09-01`).  |
| `--snomed-url TEXT`     | SNOMED CT API base URL.                 |
| `--snomed-edition TEXT` | SNOMED CT edition (e.g. `SNOMEDCT-US`). |

### Output

| Flag                                         | Description                                                                                                           |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `-o, --output TEXT`                          | Output file path (without extension). Extension is added automatically based on format.                               |
| `-of, --output-format [JSON\|XLSX]`          | Output format.                                                                                                        |
| `-rr, --raw-report`                          | Raw output format (JSON only).                                                                                        |
| `-mr, --max-report-rows INTEGER`             | Max rows in the Issue Details tab of Excel output (default: 1000; 0 = unlimited). Also via `MAX_REPORT_ROWS` env var. |
| `-me, --max-errors-per-rule INTEGER BOOLEAN` | Limit errors per rule. Format: `-me <limit> <per_dataset_flag>`. See below.                                           |
| `-rt, --report-template TEXT`                | Path to a custom Excel report template.                                                                               |

#### `--max-errors-per-rule` Detail

```bash
-me 100 False   # Cumulative soft limit across all datasets (default per_dataset=False)
-me 100 True    # Hard limit per dataset per rule
```

- `False` (default): After each dataset, if cumulative errors for a rule meet the limit, that rule stops processing further datasets.
- `True`: Limits reported issues to `<limit>` per dataset per rule. The rule still executes on all datasets.
- Can also be set via `MAX_ERRORS_PER_RULE` env var. The larger of the env var and CLI values is used. If either sets `per_dataset_flag` to `True`, it will be `True`.

### Performance & Behavior

| Flag                               | Description                                                                       |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `-ca, --cache TEXT`                | Relative path to cache files.                                                     |
| `-ps, --pool-size INTEGER`         | Number of parallel processes.                                                     |
| `-dep, --dotenv-path`              | Path to `.env` file for environment variables.                                    |
| `-l, --log-level`                  | Log verbosity: `debug`, `info`, `warn`, `error`, `critical`, `disabled`.          |
| `-vx, --validate-xml`              | XML validation toggle (default: enabled). Pass a value other than `y` to disable. |
| `-p, --progress`                   | Progress display: `verbose_output`, `disabled`, `percents`, or `bar` (default).   |
| `-jcf, --jsonata-custom-functions` | Variable name + path to directory of custom JSONata functions. Repeatable.        |
| `--help`                           | Show the help message and exit.                                                   |

### Understanding Rule Run Statuses

The **Rules Report** tab in the output summarizes the outcome for each rule:

| Status            | Meaning                                                                            |
| ----------------- | ---------------------------------------------------------------------------------- |
| `SUCCESS`         | Rule ran; no issues found.                                                         |
| `SKIPPED`         | Rule could not run (column/domain not found, schema validation off, out of scope). |
| `ISSUE REPORTED`  | Rule ran; issues were found.                                                       |
| `EXECUTION ERROR` | Rule failed for an unexpected reason. Details in the Issue Details tab.            |

### Large Dataset Processing (Dask)

CORE uses Dask instead of pandas for datasets exceeding 1/4 of available RAM. To force Dask for all datasets:

**Linux/Mac:**

```bash
DATASET_SIZE_THRESHOLD=0 ./core validate -s sdtmig -v 3-4 -d /path/to/datasets
```

**Windows (PowerShell):**

```powershell
$env:DATASET_SIZE_THRESHOLD=0; .\core.exe validate -s sdtmig -v 3-4 -d C:\path\to\datasets
```

Or create a `.env` file in the root directory:

```
DATASET_SIZE_THRESHOLD=0
```

---

## `update-cache`

Download and refresh locally cached rules, controlled terminology, and metadata.

```bash
python core.py update-cache
```

An API key is required for metadata and CT. Rules are accessible without a key. Set your key via the `CDISC_LIBRARY_API_KEY` environment variable or in a `.env` file in the root directory (no quotes needed around the value).

To obtain an API key: [wiki.cdisc.org — Getting Started](https://wiki.cdisc.org/display/LIBSUPRT/Getting+Started%3A+Access+to+CDISC+Library+API+using+API+Key+Authentication)

> **Firewall note:** CORE connects to `api.library.cdisc.org` on port 443. If you see SSL certificate verification errors, contact your IT department to obtain the corporate CA bundle or request whitelisting for this hostname.

### Options

| Flag                                    | Description                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------------- |
| `-c, --cache-path TEXT`                 | Relative path to cache (only needed if cache has moved from its default location). |
| `--apikey TEXT`                         | CDISC Library API key. Also via `CDISC_LIBRARY_API_KEY` env var.                   |
| `-crd, --custom-rules-directory TEXT`   | Path to a directory of local rule YAML/JSON files to add to the cache.             |
| `-cr, --custom-rule TEXT`               | Path to a single local rule file. Repeatable.                                      |
| `-rcr, --remove-custom-rules TEXT`      | Remove rules by ID, comma-separated list, or `ALL`.                                |
| `-ucr, --update-custom-rule TEXT`       | Path to an updated rule file. Replaces the existing rule in cache.                 |
| `-cs, --custom-standard TEXT`           | Path to a JSON file defining a custom standard.                                    |
| `-cse, --custom-standard-encoding TEXT` | Encoding for the custom standard JSON file.                                        |
| `-rcs, --remove-custom-standard TEXT`   | Remove a custom standard by `standard/version`. Repeatable.                        |

### Custom Rules

Custom rules are stored in the cache indexed by their CORE ID (e.g. `COMPANY-000123`).

```bash
# Add a directory of rules
python core.py update-cache --custom-rules-directory path/to/rules/

# Add a single rule
python core.py update-cache --custom-rule path/to/rule.yaml

# Update an existing rule
python core.py update-cache --update-custom-rule path/to/updated_rule.yaml

# Remove rules
python core.py update-cache --remove-custom-rules RULE-000001
python core.py update-cache --remove-custom-rules RULE-000001,RULE-000002
python core.py update-cache --remove-custom-rules ALL
```

### Custom Standards

Custom standards map a standard identifier to a list of applicable rule IDs. Add rules to the cache first, then create a standard that references them.

**Standard JSON format:**

```json
{
  "cust_standard/1-0": ["CUSTOM-000123", "CUSTOM-000456"]
}
```

Custom standards can also reference CDISC standard names to inherit library metadata while using custom rules:

```json
{
  "sdtmig/3-4": ["CUSTOM-000123", "CUSTOM-000456"]
}
```

```bash
# Add or update a custom standard
python core.py update-cache --custom-standard path/to/standard.json

# Remove a custom standard
python core.py update-cache --remove-custom-standard mycustom/1-0
```

---

## `list-rules`

List conformance rules available in the cache.

```bash
# All published rules
python core.py list-rules

# Rules for a specific standard and version
python core.py list-rules -s sdtmig -v 3-4

# Rules for a TIG substandard
python core.py list-rules -s tig -v 1-0 -ss SDTM

# Specific rules by ID
python core.py list-rules -r CORE-000351 -r CORE-000591

# All custom rules
python core.py list-rules --custom-rules

# Custom rules for a specific standard
python core.py list-rules --custom-rules -s custom_standard -v 1-0
```

### Options

| Flag                      | Description                                                           |
| ------------------------- | --------------------------------------------------------------------- |
| `-s, --standard TEXT`     | Filter by standard (e.g. `sdtmig`, `tig`).                            |
| `-v, --version TEXT`      | Filter by standard version (e.g. `3-4`).                              |
| `-ss, --substandard TEXT` | Filter by substandard for integrated standards (e.g. `SDTM`, `ADaM`). |
| `-r, --rule-id TEXT`      | List specific rule(s) by CORE ID. Repeatable.                         |
| `--custom-rules`          | List custom rules instead of published CDISC rules.                   |
| `-c, --cache-path TEXT`   | Relative path to cache.                                               |
| `--help`                  | Show the help message and exit.                                       |

---

## `list-rule-sets`

List all standards and versions for which rules are available in the cache.

```bash
# CDISC standards
python core.py list-rule-sets

# Custom standards only
python core.py list-rule-sets --custom
```

**Options:**

| Flag                    | Description                                                    |
| ----------------------- | -------------------------------------------------------------- |
| `-c, --cache-path TEXT` | Relative path to cache.                                        |
| `-o, --custom`          | List custom standards and versions instead of CDISC standards. |

---

## `list-ct`

List controlled terminology packages available in the cache.

```bash
python core.py list-ct

# Filter by subset type
python core.py list-ct -s sdtmct
```

**Options:**

| Flag                    | Description                                                     |
| ----------------------- | --------------------------------------------------------------- |
| `-c, --cache-path TEXT` | Relative path to cache.                                         |
| `-s, --subsets TEXT`    | CT subset type filter (e.g. `sdtmct`). Multiple values allowed. |

---

## Environment Variables

Key environment variables that can substitute for or supplement CLI flags:

| Variable                 | Equivalent Flag                               |
| ------------------------ | --------------------------------------------- |
| `CDISC_LIBRARY_API_KEY`  | `--apikey`                                    |
| `PRODUCT`                | `-s` / `--standard`                           |
| `VERSION`                | `-v` / `--version`                            |
| `SUBSTANDARD`            | `-ss` / `--substandard`                       |
| `USE_CASE`               | `-uc` / `--use-case`                          |
| `DEFINE_XML`             | `-dxp` / `--define-xml-path`                  |
| `CT`                     | `-ct` (`:` separated on Unix, `;` on Windows) |
| `MAX_REPORT_ROWS`        | `-mr` / `--max-report-rows`                   |
| `MAX_ERRORS_PER_RULE`    | `-me` / `--max-errors-per-rule`               |
| `DATASET_SIZE_THRESHOLD` | Dask threshold (set to `0` to force Dask)     |

These can be set in a `.env` file in the root directory. See `env.example`.
