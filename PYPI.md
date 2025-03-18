### Supported python versions

[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100)

#### **PyPI Quickstart: Validate data within python**

An alternative to running the validation from the command line is to instead import the rules engine library in python and run rules against data directly (without needing your data to be in `.xpt` format).

##### Step 0: Install the library

```
pip install cdisc-rules-engine
```

In addition to installing the library, you'll also want to download the rules cache (found in the `resources/cache` folder of this repository) and store them somewhere in your project.
Notably, when pip install is run, it will install the USDM and dataset-JSON dataset schemas should you decide to implement the dataset reader classes in `cdisc_rules_engine/services/data_readers` or the metadata_readers in `cdisc_rules_engine/services`

##### Step 1: Load the Rules

The rules can be loaded into an in-memory cache by doing the following:

```python
import os
import pathlib

from multiprocessing.managers import SyncManager
from cdisc_rules_engine.services.cache import InMemoryCacheService

class CacheManager(SyncManager):
    pass

# If you're working from a terminal you may need to
# use SyncManager directly rather than define CacheManager
CacheManager.register("InMemoryCacheService", InMemoryCacheService)


def load_rules_cache(path_to_rules_cache):
  cache_path = pathlib.Path(path_to_rules_cache)
  manager = CacheManager()
  manager.start()
  cache = manager.InMemoryCacheService()

  files = next(os.walk(cache_path), (None, None, []))[2]

  for fname in files:
      with open(cache_path / fname, "rb") as f:
          cache.add_all(pickle.load(f))

  return cache
```

Rules in this cache can be accessed by standard and version using the `get_rules_cache_key` function.

```python
from cdisc_rules_engine.utilities.utils import get_rules_cache_key

cache = load_rules_cache()
# Note that the standard version is separated by a dash, not a period
cache_key_prefix = get_rules_cache_key("sdtmig", "3-4")
rules = cache.get_all_by_prefix(cache_key_prefix)
```

`rules` will now be a list of dictionaries the following keys

- `core_id`
  - e.g. "CORE-000252"
- `domains`
  - e.g. `{'Include': ['DM'], 'Exclude': []}` or `{'Include': ['ALL']}`
- `author`
- `reference`
- `sensitivity`
- `executability`
- `description`
- `authorities`
- `standards`
- `classes`
- `rule_type`
- `conditions`
- `actions`
- `datasets`
- `output_variables`

##### Step 2: Prepare your data

In order to pass your data through the rules engine, it must be a pandas dataframe of an SDTM dataset. For example:

```
>>> data
STUDYID DOMAIN USUBJID  AESEQ AESER    AETERM    ... AESDTH AESLIFE AESHOSP
0          AE      001     0     Y     Headache  ...     N       N       N

[1 rows x 19 columns]
```

Before passing this into the rules engine, we need to wrap it in a DatasetVariable.

```python
from cdisc_rules_engine.models.dataset_variable import DatasetVariable

dataset = DatasetVariable(data)
```

##### Step 3: Run the (relevant) rules

Next, we need to actually run the rules. We can select which rules we want to run based on the domain of the data we're checking and the `"Include"` and `"Exclude"` domains of the rule.

```python
# Get the rules for the domain AE
# (Note: we're ignoring ALL domain rules here)
ae_rules = [
  rule for rule in rules
  if "AE" in rule["domains"].get("Include", [])
]
```

There's one last thing we need before we can actually run the rule, and that's a `COREActions` object. This object will handle generating error messages should the rule fail.

To instantiate a `COREActions` object, we need to pass in the following:

- `results`: An array to which errors will be appended
- `variable`: Our DatasetVariable
- `domain`: e.g. "AE"
- `rule`: Our rule

```python
from cdisc_rules_engine.models.actions import COREActions

rule = ae_rules[0]
results = []
core_actions = COREActions(
  results,
  variable=dataset,
  domain="AE",
  rule=rule
)
```

All that's left is to run the rule!

```python
from business_rules.engine import run

was_triggered = run(
  rule=rule,
  defined_variables=dataset_variable,
  defined_actions=core_actions,
)
```

##### Step 5: Interpret the results

The return value of run will tell us if the rule was triggered.

- A `False` value means that there were no errors
- A `True` value means that there were errors

If there were errors, they will have been appended to the results array passed into your `COREActions` instance. Here's an example error:

```python
{
  'executionStatus': 'success',
  'domain': 'AE',
  'variables': ['AESLIFE'],
  'message': 'AESLIFE is completed, but not equal to "N" or "Y"',
  'errors': [
    {'value': {'AESLIFE': 'Maybe'}, 'row': 1}
  ]
}
```
