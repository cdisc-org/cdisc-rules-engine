# CDISC Rules Engine Guide

## Step 0: Install the Library

```
pip install cdisc-rules-engine
```

In addition to installing the library, you'll also want to download the rules cache (found in the resources/cache folder of this repository) and store them somewhere in your project. Notably, when pip install is run, it will install the USDM and dataset-JSON dataset schemas should you decide to implement the dataset reader classes in cdisc_rules_engine/services/data_readers or the metadata_readers in cdisc_rules_engine/services

## Step 1: Load the Rules

The rules can be loaded into an in-memory cache by doing the following:

```python
import os
import pathlib
import pickle

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

Rules in this cache can be accessed by standard and version using the get_rules_cache_key function.

```python
from cdisc_rules_engine.utilities.utils import get_rules_cache_key

cache = load_rules_cache("path/to/rules/cache")
# Note that the standard version is separated by a dash, not a period
cache_key_prefix = get_rules_cache_key("sdtmig", "3-4")
rules = cache.get_all_by_prefix(cache_key_prefix)
```

`rules` will now be a list of dictionaries with the following keys:

- core_id (e.g. "CORE-000252")
- domains (e.g. {'Include': ['DM'], 'Exclude': []} or {'Include': ['ALL']})
- author
- reference
- sensitivity
- executability
- description
- authorities
- standards
- classes
- rule_type
- conditions
- actions
- datasets
- output_variables

## Step 2: Prepare Your Data

In order to pass your data through the rules engine, it must be a pandas dataframe of an SDTM dataset. For example:

```python
>>> data
STUDYID DOMAIN USUBJID  AESEQ AESER    AETERM    ... AESDTH AESLIFE AESHOSP
0          AE      001     0     Y     Headache  ...     N       N       N

[1 rows x 19 columns]
```

Before passing this into the rules engine, we need to wrap it in a DatasetVariable, which requires first wrapping it in a PandasDataset:

```python
import pandas as pd
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

# First, create a PandasDataset from your DataFrame
pandas_dataset = PandasDataset(data=data)

# Create dataset metadata (needed for column_prefix_map)
dataset_metadata = SDTMDatasetMetadata(
    name="AE",
    label="Adverse Events",
    first_record=data.iloc[0].to_dict() if not data.empty else None
)

# Then create the DatasetVariable
dataset_variable = DatasetVariable(
    pandas_dataset,
    column_prefix_map={"--": dataset_metadata.domain},
)
```

NOTE: DatasetVariable has several arguments that can be instantiated but dataset and column prefix are the most vital for rule execution.

```
dataset_variable = DatasetVariable(
    dataset,
    column_prefix_map={"--": dataset_metadata.domain},
    relationship_data=relationship_data,
    value_level_metadata=value_level_metadata,
    column_codelist_map=variable_codelist_map,
    codelist_term_maps=codelist_term_maps,
)
```

## Step 3: Run the (Relevant) Rules

Next, we need to actually run the rules. We can select which rules we want to run based on the domain of the data we're checking and the "Include" and "Exclude" domains of the rule.

```python
# Get the rules for the domain AE
# (Note: we're ignoring ALL domain rules here)
ae_rules = [
    rule for rule in rules
    if "AE" in rule["domains"].get("Include", [])
]
```

There's one last thing we need before we can actually run the rule, and that's a COREActions object. This object will handle generating error messages should the rule fail.

To instantiate a COREActions object, we need to pass in the following:

- output_container: A list to which errors will be appended
- variable: Our DatasetVariable
- dataset_metadata: Our SDTMDatasetMetadata instance
- rule: Our rule
- value_level_metadata (optional): A list of value level metadata

```python
from cdisc_rules_engine.models.actions import COREActions

rule = ae_rules[0]
results = []
core_actions = COREActions(
    output_container=results,
    variable=dataset_variable,
    dataset_metadata=dataset_metadata,
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

## Step 4: Interpret the Results

The return value of run will tell us if the rule was triggered.

- A False value means that there were no errors
- A True value means that there were errors

If there were errors, they will have been appended to the results array passed into your COREActions instance. Here's an example error:

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

## Understanding Dataset Abstraction

The CDISC Rules Engine uses an abstraction layer for datasets, which allows for flexibility but requires properly initializing your data before validation. Here's how to work with the PandasDataset class:

### Using PandasDataset

To use your pandas DataFrame with the CDISC Rules Engine:

```python
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pandas as pd

# Create or load your DataFrame
my_dataframe = pd.DataFrame({
    'STUDYID': ['STUDY1', 'STUDY1'],
    'USUBJID': ['001', '002'],
    'DOMAIN': ['DM', 'DM'],
    # Add other columns as needed
})

# Create a PandasDataset instance
dataset = PandasDataset(data=my_dataframe)

# Now 'dataset' can be used with DatasetVariable:
dataset_variable = DatasetVariable(
    dataset,
    column_prefix_map={"--": dataset_metadata.domain},
    relationship_data=relationship_data,
    value_level_metadata=value_level_metadata,
    column_codelist_map=variable_codelist_map,
    codelist_term_maps=codelist_term_maps,
)
```

### Understanding DatasetMetadata and column_prefix_map

The column_prefix_map is often dynamically set using the domain information from SDTMDatasetMetadata. Here's how these components work together:

#### SDTMDatasetMetadata

The SDTMDatasetMetadata class provides essential information about SDTM datasets:

```python
from dataclasses import dataclass
from typing import Union
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS

@dataclass
class SDTMDatasetMetadata(DatasetMetadata):
    """
    This class is a container for SDTM dataset metadata
    """
    @property
    def domain(self) -> Union[str, None]:
        return (self.first_record or {}).get("DOMAIN", None)

    @property
    def rdomain(self) -> Union[str, None]:
        return (self.first_record or {}).get("RDOMAIN", None) if self.is_supp else None

    @property
    def is_supp(self) -> bool:
        """
        Returns true if name starts with SUPP or SQ
        """
        return self.name.startswith(SUPPLEMENTARY_DOMAINS)

    @property
    def unsplit_name(self) -> str:
        if self.domain:
            return self.domain
        if self.name.startswith("SUPP"):
            return f"SUPP{self.rdomain}"
        if self.name.startswith("SQ"):
            return f"SQ{self.rdomain}"
        return self.name

    @property
    def is_split(self) -> bool:
        return self.name != self.unsplit_name
```

### How column_prefix_map Uses domain Information

In the rule execution flow, the column_prefix_map is typically set using the domain from the dataset metadata:

```python
dataset_variable = DatasetVariable(
    dataset,
    column_prefix_map={"--": dataset_metadata.domain},
    relationship_data=relationship_data,
    value_level_metadata=value_level_metadata,
    column_codelist_map=variable_codelist_map,
    codelist_term_maps=codelist_term_maps,
)
```

This dynamic mapping allows the engine to correctly interpret variable names based on their domain context.

## Complete End-to-End Example

Here's a comprehensive example showing how to set up your environment with dataset metadata and execute a rule:

```python
import pandas as pd
import os
import pickle
from copy import deepcopy
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.constants.rule_constants import RuleConstants
from cdisc_rules_engine.services.dataset_preprocessor import DatasetPreprocessor
from business_rules import run
from cdisc_rules_engine.models.actions import COREActions

# 1. Create your DataFrame
ae_data = pd.DataFrame({
    'STUDYID': ['STUDY1', 'STUDY1', 'STUDY1'],
    'USUBJID': ['SUBJ-001', 'SUBJ-002', 'SUBJ-003'],
    'DOMAIN': ['AE', 'AE', 'AE'],
    'AETERM': ['Headache', 'Nausea', 'Fever'],
    'AESEV': ['MILD', 'MODERATE', 'SEVERE'],
    # ... other columns
})

# 2. Create PandasDataset
dataset = PandasDataset(data=ae_data)

# 3. Create SDTMDatasetMetadata
# Note how first_record is set with the first row of data, which includes DOMAIN='AE'
dataset_metadata = SDTMDatasetMetadata(
    name="AE",
    label="Adverse Events",
    filename="ae.xpt",
    record_count=len(ae_data),
    first_record=ae_data.iloc[0].to_dict() if not ae_data.empty else None
)

# 4. Prepare other parameters
relationship_data = {}
value_level_metadata = []
variable_codelist_map = {
    'name': 'sdtmig-3-4-codelists',
    'AESEV': [
        {'coded_value': 'MILD', 'decode': 'Mild'},
        {'coded_value': 'MODERATE', 'decode': 'Moderate'},
        {'coded_value': 'SEVERE', 'decode': 'Severe'}
    ],
}
codelist_term_maps = []

# or create script to load CT files and parse variable_codelist_map pickle files
def load_cached_data(cache_file_path):
    """Load data from a pickle cache file."""
    if os.path.exists(cache_file_path):
        with open(cache_file_path, 'rb') as f:
            return pickle.load(f)
    return None
# using sdtmct-2021-12-17.pkl as desired CT package
variable_codelist_map_cache = os.path.join(current_dir, "cache", "variable_codelist_maps.pkl")
codelist_term_maps_cache = os.path.join(current_dir, "cache", "sdtmct-2021-12-17.pkl")
codelist_term_maps = load_cached_data(codelist_term_maps_cache)
if variable_codelist_map and "sdtmig-3-4-codelists" in variable_codelist_map:
    variable_codelist_map = variable_codelist_map["sdtmig-3-4-codelists"]
else:
    variable_codelist_map = None
    print("SDTMIG 3.4 codelists not found in cache")

# 5. Create dataset variable
# Note how column_prefix_map uses dataset_metadata.domain which will resolve to "AE"
dataset_variable = DatasetVariable(
    dataset,
    column_prefix_map={"--": dataset_metadata.domain},  # This becomes {"--": "AE"}
    relationship_data=relationship_data,
    value_level_metadata=value_level_metadata,
    column_codelist_map=variable_codelist_map,
    codelist_term_maps=codelist_term_maps,
)

# 6. Create a rule
rule = Rule(
    rule_id="CG0001",
    rule_category="SDTM",
    standards=["SDTMIG 3.4"],
    severity=RuleConstants.SEVERITY_ERROR,
    domain="AE",
    datasets_targeted=["AE"],
    expression="not is_missing(AETERM)",
    human_description="AETERM cannot be missing",
    machine_description="AETERM variable cannot be null or empty"
)

# 7. Process the rule
rule_processor = RuleProcessor()
results = rule_processor.process_rule(rule, dataset_variable)

# 8. Display results
for result in results:
    print(f"Rule {result.rule_id} - Violation: {result.violation}")
    if result.violations_data:
        for violation in result.violations_data:
            print(f"  Row: {violation.get('row_number')}, {violation}")
```

## Troubleshooting

If you're seeing errors related to the dataset integration, check that:

- Your DataFrame contains all the required columns for the validation rules
- The column_prefix_map correctly maps variable prefixes to domains (e.g., {"--": "AE"} for Adverse Events)
- Your column_codelist_map includes all the necessary codelists for variables that have controlled terminology
- Any metadata passed to the DatasetVariable constructor is correctly formatted
- The dataset object is an instance of PandasDataset, not a raw pandas DataFrame
- All required parameters are provided to the DatasetVariable constructor

## Working with XPT files

If you need to read XPT files (SAS transport format), you can use the pyreadstat package:

```python
import pyreadstat
import pandas as pd
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

# Read the XPT file
ae_data, meta = pyreadstat.read_xport("path/to/ae.xpt")

# Create a PandasDataset
pandas_dataset = PandasDataset(data=ae_data)

# Create dataset metadata
dataset_metadata = SDTMDatasetMetadata(
    name="AE",
    label=meta.file_label if hasattr(meta, 'file_label') else "Adverse Events",
    first_record=ae_data.iloc[0].to_dict() if not ae_data.empty else None
)

# Create DatasetVariable
dataset_variable = DatasetVariable(
    pandas_dataset,
    column_prefix_map={"--": dataset_metadata.domain},
)
```
