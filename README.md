# cdisc-rules-engine
Open source offering of the cdisc rules engine

### Code formatter
This project uses the `black` code formatter for python.
It also uses `pre-commit` to run `black` when you commit.
Both dependencies are added to *requirements.txt*.

**Required**

Setting up `pre-commit` requires one extra step. After installing it you have to run

`pre-commit install`

This installs `pre-commit` in your `.git/hooks` directory.

### Running The Tests
From the root of the project run the following command:

`python -m pytest tests/unit/`
### Running a validation

Clone the repository and run `core.py --help` to see the full list of commands.

Run `core.py validate --help` to see the list of validation options.

```
* -ca, --cache TEXT                          Relative path to cache files containing pre loaded metadata and rules
* -p, --pool_size INTEGER                    Number of parallel processes for validation
* -d, --data TEXT                            Relative path to directory containing data files
* -l, --log_level TEXT                       Sets log level for engine logs, logs are disabled by default
* -rt, --report_template TEXT                File path of report template to use for excel output
* -s, --standard TEXT                        CDISC standard to validate against
* -v, --version TEXT                         Standard version to validate against
* -ct, --controlled_terminology_package TEXT Controlled terminology package to validate against, can provide more than one
* -o, --output TEXT                          Report output file destination
* -dv, --define_version TEXT                 Define-XML version used for validation
* --whodrug TEXT                             Path to directory with WHODrug dictionary files
* --meddra TEXT                              Path to directory with MedDRA dictionary files
```

#### Validate folder
To validate a folder using rules for SDTM-IG version 3.4 use the following command:

`python core.py -s SDTM -v 3.4 -d path/to/datasets`

### Creating an executable version

**Linux**

`pyinstaller core.py --add-data=venv/lib/python3.9/site-packages/xmlschema/schemas:xmlschema/schemas --add-data=cdisc_rules_engine/resources/cache:cdisc_rules_engine/resources/cache --add-data=cdisc_rules_engine/resources/templates:cdisc_rules_engine/resources/templates`

**Windows**

`pyinstaller core.py --add-data=".venv/Lib/site-packages/xmlschema/schemas;xmlschema/schemas" --add-data="cdisc_rules_engine/resources/cache;cdisc_rules_engine/resources/cache" --add-data="cdisc_rules_engine/resources/templates;cdisc_rules_engine/resources/templates"`

_Note .venv should be replaced with path to python installation or virtual environment_

This will create an executable version in the `dist` folder. The version does not require having Python installed and
can be launched by running `core` script with all necessary CLI arguments.

### Creating .whl file

All non-python files should be listed in `MANIFEST.in` to be included in the distribution.
Files must be in python package.

**Unix/MacOS**

`python3 -m pip install --upgrade build`
`python3 -m build`

To install from dist folder
`pip3 install {path_to_file}/cdisc_rules_engine-0.1.0-py3-none-any.whl`

To upload built distributive to pypi

`python3 -m pip install --upgrade twine`
`python3 -m twine upload --repository {repository_name} dist/*`

**Windows(Untested)**

`py -m pip install --upgrade build`
`py -m build`

To install from dist folder
`pip install {path_to_file}/cdisc_rules_engine-0.1.0-py3-none-any.whl`

To upload built distributive to pypi

`py -m pip install --upgrade twine`
`py -m twine upload --repository {repository_name} dist/*`
