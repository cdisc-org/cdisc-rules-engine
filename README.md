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

Validation can be run by cloning the repo and running the `run_validation.py` script

Command line arguments:

```
* -ca, --cache: Relative path to cache files containing pre loaded metadata and rules
* -d, --data: Relative path to directory containing data files
* -l, --log_level: Sets log level for engine logs, logs are disabled by default
* -s, --standard: Standard to validate against
* -v, --version: Standard version to validate against
* -ct, --controlled_terminology_package: CT package(s) to validate against, can supply more than one
* -dv, --define_version: Define XML version to use for validation.
* -rt, --report_template: Report template to use for excel output
* -o, --output: Output file destination
* -dp, --dictionaries_path: Path to directory with dictionaries files
* -dt, --dictionary_type: Dictionary type (MedDra, WhoDrug). Required if dictionaries_path is provided.
```

### Creating an executable version
Run the following command:

`pyinstaller run_validation.py --add-data=venv/lib/python3.9/site-packages/xmlschema/schemas:xmlschema/schemas`

This will create an executable version in the `dist` folder. The version does not require having Python installed and
can be launched by running `./run_validation` with all necessary CLI arguments.
