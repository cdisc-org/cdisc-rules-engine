### Supported python versions

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120)

### Windows Command Compatibility

Note: The Windows commands provided in this README are written for PowerShell. While most commands are compatible with both PowerShell and Command Prompt, some adjustments may be necessary when using Command Prompt. If you encounter any issues running these commands in Command Prompt, try using PowerShell or consult the Command Prompt documentation for equivalent commands.

# cdisc-rules-engine

Open source offering of the CDISC Rules Engine, a tool designed for validating clinical trial data against data standards.
To learn more, visit our official CDISC website or for other implementation options, see our DockerHub repository:  
<br>  
[CDISC Website](https://www.cdisc.org/)  
<br>  
[CDISC Rules Engine on DockerHub](https://hub.docker.com/repository/docker/cdiscdocker/cdisc-rules-engine/general)

### **Quick start**

To quickly get up and running with CORE, users can download the latest executable version of the engine for their operating system from here: <https://github.com/cdisc-org/cdisc-rules-engine/releases>

Once downloaded, simply unzip the file and run the following command based on your Operating System:

Windows:

```
.\core.exe validate -s <standard> -v <standard_version> -d path/to/datasets

# ex: .\core.exe validate -s sdtmig -v 3-4 -d .\xpt\
```

Linux/Mac:

```
./core validate -s <standard> -v <standard_version> -d path/to/datasets

# ex: ./core validate -s sdtmig -v 3-4 -d .\xpt\
```

> **_NOTE:_** For Linux users, you will need to run this command from the executable root directory:
>
> ```bash
> chmod +x ./core
> ```
>
> For Mac users, you will need to remove the Apple signature quarantine in addition to making the app executable.
>
> ```bash
> xattr -rd com.apple.quarantine /path/to/core/root/dir
> chmod +x ./core
> ```

### **Command-line Interface**

### **Getting Started**

In the terminal, navigate to the directory you intend to install CORE rules engine in

1. Clone the repository:

   ```
   git clone https://github.com/cdisc-org/cdisc-rules-engine
   ```

2. Ensure you have Python 3.12 installed:
   You can check your Python version with:
   ```
   python --version
   ```
   If you don't have Python 3.12, please download and install it from [python.org](https://www.python.org/downloads/) or using your system's package manager.

### **Code formatter**

This project uses the `black` code formatter, `flake8` linter for python and `prettier` for JSON, YAML and MD.
It also uses `pre-commit` to run `black`, `flake8` and `prettier` when you commit.
Both dependencies are added to _requirements-dev.txt_.

**Required**

Setting up `pre-commit` requires one extra step. After installing it you have to run

`pre-commit install`

This installs `pre-commit` in your `.git/hooks` directory.

### **Installing dependencies**

These steps should be run before running any tests or core commands using the non compiled version.

- Create a virtual environment:

  `python -m venv <virtual_environment_name>`

NOTE: if you have multiple versions of python on your machine, you can call python 3.12 for the virtual environment's creation instead of the above command:
`python3.12 -m venv <virtual_environment_name>`

- Activate the virtual environment:

`./<virtual_environment_name>/bin/activate` -- on linux/mac </br>
`.\<virtual_environment_name>\Scripts\Activate` -- on windows

- Install the requirements.

`python -m pip install -r requirements-dev.txt` # From the root directory

### **Running The Tests**

From the root of the project run the following command (this will run both the unit and regression tests):

`python -m pytest tests`

### **Running a validation**

#### From the command line

Clone the repository and run `python core.py --help` to see the full list of commands.

Run `python core.py validate --help` to see the list of validation options.

```
  -ca, --cache TEXT               Relative path to cache files containing pre
                                  loaded metadata and rules
  -ps, --pool-size INTEGER         Number of parallel processes for validation
  -d, --data TEXT                 Path to directory containing data files
  -dp, --dataset-path TEXT        Absolute path to dataset file. Can be specified multiple times.
  -dxp, --define-xml-path TEXT    Path to Define-XML
  -l, --log-level [info|debug|error|critical|disabled|warn]
                                  Sets log level for engine logs, logs are
                                  disabled by default
  -rt, --report-template TEXT     File path of report template to use for
                                  excel output
  -s, --standard TEXT             CDISC standard to validate against
                                  [required]
  -v, --version TEXT              Standard version to validate against
                                  [required]
  -ss, --substandard TEXT         Substandard to validate against
                                  [required for TIG]
  -ct, --controlled-terminology-package TEXT
                                  Controlled terminology package to validate
                                  against, can provide more than one
                                  NOTE: if a defineXML is provided, if it is version 2.1
                                  engine will use the CT laid out in the define.  If it is
                                  version 2.0, -ct is expected to specify the CT package
  -o, --output TEXT               Report output file destination and name. Path will be
                                  relative to the validation execution directory
                                  and should end in the desired output filename
                                  without file extension
                                  '/user/reports/result' will be 'user/report' directory
                                  with the filename as 'result'
  -of, --output-format [JSON|XLSX]
                                  Output file format
  -rr, --raw-report               Report in a raw format as it is generated by
                                  the engine. This flag must be used only with
                                  --output-format JSON.
  -mr, --max-report-rows INTEGER  Maximum rows for 'Issue Details' per Excel report. When exceeded,
                                  issues beyond the limit will not be reported.
                                  Defaults to 1,000 'Issue Details' rows
                                  Can be set via MAX_REPORT_ROWS env variable;
                                  if both .env and -mr are specified, the larger value will be used.
                                  If set to 0, no maximum will be enforced.
                                  Excel row limit is 1,048,576 rows
  -me, --max-errors-per-rule INTEGER BOOLEAN
                                  Imposes a maximum number of errors per rule to enforce.
                                  Usage: -me <limit> <per_dataset_flag>
                                  Example: -me 100 true

                                  <limit>: Maximum number of errors (integer)

                                  <per_dataset_flag>:
                                    - false (default): Cumulative soft limit across all datasets.
                                      After each dataset is validated for a single rule,
                                      the limit is checked and if met or exceeded,
                                      validation for that rule will cease for remaining datasets.
                                    - true: Non-cumulative per-dataset limit.
                                      Limits reported issues to <limit> per dataset per rule.
                                      The rule continues to execute on all datasets, but only
                                      the first <limit> issues per dataset are included in the report.

                                  Can be set via MAX_ERRORS_PER_RULE env variable;
                                  if both .env and -me <limit> are specified, the larger value will be used.
                                  If limit is set to 0, no maximum will be enforced.
                                  No maximum is the default behavior.
  -dv, --define-version TEXT      Define-XML version used for validation
  -dxp, --define-xml-path         Path to define-xml file.
  -vx, --validate-xml             Enable XML validation (default 'y' to enable, otherwise disable).
  --whodrug TEXT                  Path to directory with WHODrug dictionary
                                  files
  --meddra TEXT                   Path to directory with MedDRA dictionary
                                  files
  --loinc TEXT                  Path to directory with LOINC dictionary
                                  files
  --medrt TEXT                  Path to directory with MEDRT dictionary
                                  files
  --unii TEXT                  Path to directory with UNII dictionary
                                  files
  --snomed-version TEXT        Version of snomed to use. (ex. 2024-09-01)
  --snomed-url TEXT            Base url of snomed api to use. (ex. https://snowstorm.snomedtools.org/snowstorm/snomed-ct)
  --snomed-edition TEXT        Edition of snomed to use. (ex. SNOMEDCT-US)
  -r, --rules TEXT                Specify rule core ID ex. CORE-000001. Can be specified multiple times.
  -er, --exclude-rules TEXT       Specify rule core ID to exclude, ex. CORE-000001. Can be specified multiple times.
  -lr, --local-rules TEXT         Specify relative path to directory or file containing
                                  local rule yml and/or json rule files.
  -cs, --custom-standard       Adding this flag tells engine to use a custom standard specified with -s and -v
                                  that has been uploaded to the cache using update-cache
  -vo, --verbose-output           Specify this option to print rules as they
                                  are completed
  -p, --progress [verbose_output|disabled|percents|bar]
                                  Defines how to display the validation
                                  progress. By default a progress bar like
                                  "[████████████████████████████--------]
                                  78%"is printed.
  -jcf, --jsonata-custom-functions Pair containing a variable name and a Path to directory containing a set of custom JSONata functions. Can be specified multiple times
  --help                          Show this message and exit.
```

##### Available log levels

- `debug` - Display all logs
- `info` - Display info, warnings, and error logs
- `warn` - Display warnings and errors
- `error` - Display only error logs
- `critical` - Display critical logs

##### **Validate folder**

To validate a folder using rules for SDTM-IG version 3.4 use the following command:

`python core.py validate -s sdtmig -v 3-4 -d path/to/datasets`

**_NOTE:_** Before running a validation in the CLI, you must first populate the cache with rules to validate against. See the update-cache command below.

##### **Validate single rule**

`python core.py validate -s sdtmig -v 3-4 -dp <path to dataset json file> -lr <path to rule json file> --meddra ./meddra/ --whodrug ./whodrug/`
Note: JSON dataset should match the format provided by the rule editor:

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

##### **Understanding the Rules Report**

The rules report tab displays the run status of each rule selected for validation

The possible rule run statuses are:

- `SUCCESS` - The rule ran and data was validated against the rule. May or may not produce results
- `SKIPPED` - The rule was unable to be run. Usually due to missing required data, but could also be cause by rule execution errors.

# Additional Core Commands

**- update-cache** - update locally stored cache data (Requires an environment variable - `CDISC_LIBRARY_API_KEY`) This is stored in the .env folder in the root directory, the API key does not need quotations around it.

```bash
  python core.py update-cache
```

**NOTE:** When running a validation, CORE uses rules in the cache unless -lr is specified. Running the above command populates the cache with controlled terminology, rules, metadata, etc.

To obtain an api key, please follow the instructions found here: <https://wiki.cdisc.org/display/LIBSUPRT/Getting+Started%3A+Access+to+CDISC+Library+API+using+API+Key+Authentication>. Please note it can take up to an hour after sign up to have an api key issued

# Custom Standards and Rules

## Custom Rules Management

- **Custom rules** are stored in a flat file in the cache, indexed by their core ID (e.g., 'COMPANY-000123' or 'CUSTOM-000123').
- Each rule is stored independently in this file, allowing for efficient lookup and management.

## Custom Standards Management

- **Custom standards** act as a lookup mechanism that maps a standard identifier to a list of applicable rule IDs.
- When adding a custom standard, you need to provide a JSON file with the following structure:

  ```json
  {
    "standard_id/version": ["RULE_ID1", "RULE_ID2", "RULE_ID3", ...]
  }
  ```

  For example:

  ```json
  {
    "cust_standard/1-0": [
      "CUSTOM-000123",
      "CUSTOM-000456",
      "CUSTOM-001",
      "CUSTOM-002"
    ]
  }
  ```

- To add or update a custom standard, use:

  ```bash
  python core.py update-cache --custom-standard 'path/to/standard.json'
  ```

- To remove custom standards, use the `--remove-custom-standard` or `-rcs` flag:

  ```bash
  python core.py update-cache --remove-custom-standard 'mycustom/1-0'
  ```

- When executing validation against a custom standard, the system will use the standard as a lookup to determine which rules to apply from the rule cache. Custom standards which match CDISC standard names and versions can be used to get library metadata for the standard while still utilizing custom rules. If a custom name does not match a CDISC standard, library metadata will not be populated.

  ```json
  {
    "sdtmig/3-4": ["CUSTOM-000123", "CUSTOM-000456", "CUSTOM-001", "CUSTOM-002"]
  }
  ```

  This rule will get metadata from SDTMIG version 3.4 but utilize the custom rules listed in the custom standard that need this library metadata.

## Relationship Between Custom Rules and Standards

- You should first add your custom rules to the cache, then create a custom standard that references those rules.
- Custom standards can reference both core CDISC rules and your own custom rules in the same standard definition.
- This two-level architecture allows for flexible rule reuse across multiple standards.

## Custom Rules Management

- **Add custom rules**: Use the `--custom-rules-directory` or `-crd` flag to specify a directory containing local rules, or `--custom-rule` or `-cr` flag to specify a single rule file:
  ```bash
  python core.py update-cache --custom-rules-directory 'path/to/directory'
  python core.py update-cache --custom-rule 'path/to/rule.json' --custom-rule 'path/to/rule.yaml'
  ```
- **Update a custom rule**: Use the `--update-custom-rule` or `-ucr` flag to update an existing rule in the cache:

  ```bash
  python core.py update-cache --update-custom-rule 'path/to/updated_rule.yaml'
  ```

- **Remove custom rules**: Use the `--remove-custom-rules` or `-rcr` flag to remove rules from the cache. Can be a single rule ID, a comma-separated list of IDs, or ALL to remove all custom rules:
  ```bash
  python core.py update-cache --remove-custom-rules 'RULE_ID'
  python core.py update-cache --remove-custom-rules 'RULE_ID1,RULE_ID2,RULE_ID3'
  python core.py update-cache --remove-custom-rules ALL
  ```

## List Rules

**- list-rules** - list published rules available in the cache

- list all published rules:

      `python core.py list-rules`

- list rules for standard:

      `python core.py list-rules -s sdtmig -v 3-4`

- list rules for integrated standard (substandard: "SDTM", "SEND", "ADaM", "CDASH"):

      `python core.py list-rules -s tig -v 1-0 -ss SDTM`

- list rules by ID:

      `python core.py list-rules -r CORE-000351 -r CORE-000591`

- List all custom rules:

  ```bash
  python core.py list-rules --custom-rules
  ```

- List custom rules with a specific ID:
  ```bash
  python core.py list-rules --custom-rules -s custom_standard -v 1-0
  ```

**- list-rule-sets** - lists all standards and versions for which rules are available:

```bash
python core.py list-rule-sets
```

To list custom standards and versions instead:

```bash
python core.py list-rule-sets --custom
# or using the short form:
python core.py list-rule-sets -o
```

**Options:**

- `-c, --cache-path` - Relative path to cache files containing pre-loaded metadata and rules
- `-o, --custom` - Flag to list all custom standards and versions in the cache instead of CDISC standards & rules

**- list-ct** - list ct packages available in the cache

```
Usage: python core.py list-ct [OPTIONS]

  Command to list the ct packages available in the cache.

Options:
  -c, --cache-path TEXT  Relative path to cache files containing pre loaded
                         metadata and rules
  -s, --subsets TEXT     CT package subset type. Ex: sdtmct. Multiple values
                         allowed
  --help                 Show this message and exit.
```

## PyPI Integration

The CDISC Rules Engine is available as a Python package through PyPI. This allows you to:

- Import the rules engine library directly into your Python projects
- Validate data without requiring .xpt format files
- Integrate rules validation into your existing data pipelines

```python
pip install cdisc-rules-engine
```

For implementation instructions, see [PYPI.md](PYPI.md).

### **Creating an executable version**

**Note:** Further directions to create your own executable are contained in [README_Build_Executable.md](README_Build_Executable.md) if you wish to build an unofficial release executable for your own use.

**Linux**

`pyinstaller core.py --add-data=venv/lib/python3.12/site-packages/xmlschema/schemas:xmlschema/schemas --add-data=resources/cache:resources/cache --add-data=resources/templates:resources/templates`

**Windows**

`pyinstaller core.py --add-data=".venv/Lib/site-packages/xmlschema/schemas;xmlschema/schemas" --add-data="resources/cache;resources/cache" --add-data="resources/templates;resources/templates"`

_Note .venv should be replaced with path to python installation or virtual environment_

This will create an executable version in the `dist` folder. The version does not require having Python installed and
can be launched by running `core` script with all necessary CLI arguments.

### **Creating .whl file**

All non-python files should be listed in `MANIFEST.in` to be included in the distribution.
Files must be in python package.

**Unix/MacOS**

`python3 -m pip install --upgrade build`
`python3 -m build`

To install from dist folder
`pip3 install {path_to_file}/cdisc_rules_engine-{version}-py3-none-any.whl`

To upload built distributive to pypi

`python3 -m pip install --upgrade twine`
`python3 -m twine upload --repository {repository_name} dist/*`

**Windows(Untested)**

`py -m pip install --upgrade build`
`py -m build`

To install from dist folder
`pip install {path_to_file}/cdisc_rules_engine-{version}-py3-none-any.whl`

To upload built distributive to pypi

`py -m pip install --upgrade twine`
`py -m twine upload --repository {repository_name} dist/*`

## Submit an Issue

If you encounter any bugs, have feature requests, or need assistance, please submit an issue on our GitHub repository:

[https://github.com/cdisc-org/cdisc-rules-engine/issues](https://github.com/cdisc-org/cdisc-rules-engine/issues)

When submitting an issue, please include:

- A clear description of the problem or request
- Steps to reproduce the issue (for bugs)
- Your operating system and environment details
- Any relevant logs or error messages

# Setting DATASET_SIZE_THRESHOLD for Large Datasets

The CDISC Rules Engine respects the `DATASET_SIZE_THRESHOLD` environment variable to determine when to use Dask for large dataset processing. Setting this to 0 coerces Dask usage over Pandas. A .env in the root directory with this variable set will cause this implementation coercion for the CLI. This can also be done with the executable releases via multiple methods:

## Quick Commands

### Windows (Command Prompt)

```cmd
set DATASET_SIZE_THRESHOLD=0 && core.exe validate -rest -of -config -commands
```

### Windows (PowerShell)

```powershell
$env:DATASET_SIZE_THRESHOLD=0; core.exe validate -rest -of -config -commands
```

### Linux/Mac (Bash)

```bash
DATASET_SIZE_THRESHOLD=0 ./core -rest -of -config -commands
```

## .env File (Alternative)

Create a `.env` file in the root directory of the release containing:

```
DATASET_SIZE_THRESHOLD=0
```

Then run normally: `core.exe validate -rest -of -config -commands

---

**Note:** Setting `DATASET_SIZE_THRESHOLD=0` tells the engine to use Dask processing for all datasets regardless of size, size threshold defaults to 1/4 of available RAM so datasets larger than this will use Dask. See env.example to see what the CLI .env file should look like
