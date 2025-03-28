### Supported python versions

[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100)

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

2. Ensure you have Python 3.10 installed:
   You can check your Python version with:
   ```
   python --version
   ```
   If you don't have Python 3.10, please download and install it from [python.org](https://www.python.org/downloads/) or using your system's package manager.

### **Code formatter**

This project uses the `black` code formatter, `flake8` linter for python and `prettier` for JSON, YAML and MD.
It also uses `pre-commit` to run `black`, `flake8` and `prettier` when you commit.
Both dependencies are added to _requirements.txt_.

**Required**

Setting up `pre-commit` requires one extra step. After installing it you have to run

`pre-commit install`

This installs `pre-commit` in your `.git/hooks` directory.

### **Installing dependencies**

These steps should be run before running any tests or core commands using the non compiled version.

- Create a virtual environment:

  `python -m venv <virtual_environment_name>`

NOTE: if you have multiple versions of python on your machine, you can call python 3.10 for the virtual environment's creation instead of the above command:
`python3.10 -m venv <virtual_environment_name>`

- Activate the virtual environment:

`./<virtual_environment_name>/bin/activate` -- on linux/mac </br>
`.\<virtual_environment_name>\Scripts\Activate` -- on windows

- Install the requirements.

`python -m pip install -r requirements.txt` # From the root directory

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
  -dxp, --define_xml_path TEXT    Path to Define-XML
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
  -dv, --define-version TEXT      Define-XML version used for validation
  -dxp, --define-xml-path         Path to define-xml file.
  -vx, --validate-xml             Enable XML validation (default 'y' to enable, otherwise disable)
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
  -lr, --local_rules TEXT         Specify relative path to directory or file containing
                                  local rule yml and/or json rule files.
  -lrc, --local_rules_cache       Adding this flag tells engine to use local rules
                                  uploaded to the cache instead of published rules
                                  in the cache for the validation run.
  -lri, --local_rule_id TEXT      Specify ID for custom, local rules in the cache
                                  you wish to run a validation with.
  -vo, --verbose-output           Specify this option to print rules as they
                                  are completed
  -p, --progress [verbose_output|disabled|percents|bar]
                                  Defines how to display the validation
                                  progress. By default a progress bar like
                                  "[████████████████████████████--------]
                                  78%"is printed.
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

##### Additional Core Commands

**- update-cache** - update locally stored cache data (Requires an environment variable - `CDISC_LIBRARY_API_KEY`) This is stored in the .env folder in the root directory, the API key does not need quotations around it.

    `python core.py update-cache`

To obtain an api key, please follow the instructions found here: <https://wiki.cdisc.org/display/LIBSUPRT/Getting+Started%3A+Access+to+CDISC+Library+API+using+API+Key+Authentication>. Please note it can take up to an hour after sign up to have an api key issued

- an additional local rule `-lr` flag can be added to the update-cache command that points to a directory of local rules. This adds the rules contained in the directory to the cache. It will not update the cache from library when `-lr` is specified. A `-lri` local rules ID must be given when -lr is used to ID your rules in the cache.
  **NOTE:** local rules must contain a 'custom_id' key to be added to the cache. This should replace the Core ID field in the rule.

            `python core.py update-cache -lr 'path/to/directory' -lri 'CUSTOM123'`

- to remove local rules from to the cache, remove rules `-rlr` is added to update-cache to remove local rules from the cache. A previously used local_rules_id can be specified to remove all local rules with that ID from the cache or the keyword 'ALL' is reserved to remove all local rules from the cache.

          `python core.py update-cache -rlr 'CUSTOM123'`

**- list-rules** - list published rules available in the cache

- list all published rules:

      `python core.py list-rules`

- list rules for standard:

      `python core.py list-rules -s sdtmig -v 3-4`

-list all local rules:

      `python core.py list-rules -lr`

-list local rules with a specific local rules id:

      `python core.py list-rules -lr -lri 'CUSTOM1'`

**- list-rule-sets** - lists all standards and versions for which rules are available:
`python core.py list-rule-sets`

**- list-ct** - list ct packages available in the cache

```
Usage: python core.py list-ct [OPTIONS]

  Command to list the ct packages available in the cache.

Options:
  -c, --cache_path TEXT  Relative path to cache files containing pre loaded
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

For implementation instructions, see [PyPI.md](PyPI.md).

### **Creating an executable version**

**Linux**

`pyinstaller core.py --add-data=venv/lib/python3.10/site-packages/xmlschema/schemas:xmlschema/schemas --add-data=resources/cache:resources/cache --add-data=resources/templates:resources/templates`

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
