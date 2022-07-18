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
```