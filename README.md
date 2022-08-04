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

#### Validate folder
To validate a folder using rules for SDTM-IG version 3.4 use the following command:

`python core.py -s SDTM -v 3.4 -d path/to/datasets`

### Creating an executable version

**Linux**

`pyinstaller core.py --add-data=venv/lib/python3.9/site-packages/xmlschema/schemas:xmlschema/schemas`

**Windows**

`pyinstaller core.py --add-data=".venv/Lib/site-packages/xmlschema/schemas;xmlschema/schemas"`

_Note .venv should be replaced with path to python installation or virtual environment_

This will create an executable version in the `dist` folder. The version does not require having Python installed and
can be launched by running `core` script with all necessary CLI arguments.
