# cdisc-rules-engine
Open source offering of the cdisc rules engine

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