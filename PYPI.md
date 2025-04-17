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

Rules in this cache can also be accessed by standard and version using the get_rules_cache_key function.

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

A rule using JSON/dict containing the rule keys above.

```python
from cdisc_rules_engine.models.rule import Rule
rule_metadata = {
    "Authorities": [
    ],
    "Check": {
    },
    "Core": {
        "Id": "CORE-000659",
        "Status": "Published",
        "Version": "1"
    },
    # ... rest of your rule metadata
}

# Convert the CDISC metadata format to the internal format
rule_dict = Rule.from_cdisc_metadata(rule_metadata)

# Create the Rule object
rule_obj = Rule(rule_dict)
```

# Implementation Options

You can run rules using the business rules logic or using CDISC RulesEngine() class. The RulesEngine class provides a higher-level interface with additional features like dataset preprocessing, rule operations, and more comprehensive error handling but requires more setup and physical data files. Using the business rules directly uses the business_rules package to run each rule individually against your dataset.

# Option A: Direct Use of Business Rules Engine

## Step 2: Prepare Your Data

In order to pass your data through the business rules, it must be a pandas dataframe of an SDTM dataset. For example:

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

## Step 3: Run Rules

```python
from business_rules.engine import run
from cdisc_rules_engine.models.actions import COREActions

# Get the rules for the domain AE
ae_rules = [
    rule for rule in rules
    if "AE" in rule.get("domains", {}).get("Include", []) or
       "ALL" in rule.get("domains", {}).get("Include", [])
]

print(f"Found {len(ae_rules)} rules applicable to AE domain")

all_results = []
for idx, rule in enumerate(ae_rules):
    print(f"\nProcessing rule {idx+1}/{len(ae_rules)}: {rule.get('core_id')}")

    results = []
    core_actions = COREActions(
        output_container=results,
        variable=dataset_variable,
        dataset_metadata=dataset_metadata,
        rule=rule,
        value_level_metadata=None  # This is optional
    )

    try:
        was_triggered = run(
            rule=rule,
            defined_variables=dataset_variable,
            defined_actions=core_actions,
        )

        if was_triggered and results:
            print(f"  Rule {rule.get('core_id')} was triggered - issues found!")
            all_results.extend(results)
        else:
            print(f"  Rule {rule.get('core_id')} passed - no issues found")
    except Exception as e:
        print(f"  Error processing rule {rule.get('core_id')}: {str(e)}")
```

all_results now contains your validation result. You can print it to a text file using

```python
import os
OUTPUT_DIR = os.getcwd()
with open(os.path.join(OUTPUT_DIR, 'results.txt'), 'w') as f:
    for result in all_results:
        f.write(str(result) + '\n')
```

# Option B: Using the RulesEngine Class

## Step 2: Prepare Your Data

We will use an XPT files (SAS transport format) for this example. Other dataset readers classes to model your implementation can be found in cdisc_rules_engine/services/data_readers

```python
import pandas as pd
import pyreadstat
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

def create_dataset_metadata(file_path):
    """Create dataset metadata from an XPT file"""
    try:
        # Read the XPT file
        data, meta = pyreadstat.read_xport(file_path)

        # Extract domain from first record if available
        first_record = data.iloc[0].to_dict() if not data.empty else None

        # Create metadata object
        return SDTMDatasetMetadata(
            name=os.path.basename(file_path).split('.')[0].upper(),
            label=meta.file_label if hasattr(meta, 'file_label') else "",
            filename=os.path.basename(file_path),
            full_path=file_path,
            file_size=os.path.getsize(file_path),
            record_count=len(data),
            first_record=first_record
        )
    except Exception as e:
        print(f"Error creating metadata for {file_path}: {e}")
        return None

# Create dataset metadata for all XPT files in a directory
def get_datasets_metadata(directory):
    datasets = []
    for file in os.listdir(directory):
        if file.lower().endswith('.xpt'):
            file_path = os.path.join(directory, file)
            metadata = create_dataset_metadata(file_path)
            if metadata:
                datasets.append(metadata)
    return datasets

# Get metadata for all datasets
datasets = get_datasets_metadata("path/to/dataset/directory")
```

For the RulesEngine approach, you DON'T need to manually create PandasDataset or DatasetVariable objects - the engine handles this
Multiple SDTMDatasetMetadata datasets can be made and loaded into the `datasets = []` list which will be fed into engine

### Step 3: Initialize Library Metadata

Library metadata provides essential information about standards, models, variables, and controlled terminology:

```python
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.utilities.utils import (
    get_library_variables_metadata_cache_key,
    get_model_details_cache_key_from_ig,
    get_standard_details_cache_key,
    get_variable_codelist_map_cache_key,
)

standard = "sdtmig"
standard_version = "3-4"
standard_substandard = None

# Get cache keys for metadata
standard_details_cache_key = get_standard_details_cache_key(
    standard, standard_version, standard_substandard
)
variable_details_cache_key = get_library_variables_metadata_cache_key(
    standard, standard_version, standard_substandard
)

# Get standard metadata from cache
standard_metadata = cache.get(standard_details_cache_key)

# Get model metadata based on standard metadata
model_metadata = {}
if standard_metadata:
    model_cache_key = get_model_details_cache_key_from_ig(standard_metadata)
    model_metadata = cache.get(model_cache_key)

# Get variable-codelist mapping
variable_codelist_cache_key = get_variable_codelist_map_cache_key(
    standard, standard_version, standard_substandard
)

# Load controlled terminology packages
ct_packages = ["sdtmct-2021-12-17"]  # Replace with your CT package versions
ct_package_metadata = {}
for codelist in ct_packages:
    ct_package_metadata[codelist] = cache.get(codelist)

# Create the library metadata container
library_metadata = LibraryMetadataContainer(
    standard_metadata=standard_metadata,
    model_metadata=model_metadata,
    variables_metadata=cache.get(variable_details_cache_key),
    variable_codelist_map=cache.get(variable_codelist_cache_key),
    ct_package_metadata=ct_package_metadata,
)
```

### Step 4: Initialize Data Service

The data service handles reading and processing dataset files:

```python
from cdisc_rules_engine.config import config as default_config
from cdisc_rules_engine.services.data_services import DataServiceFactory

max_dataset_size = max(datasets, key=lambda x: x.file_size).file_size # set to 0 for Dask implementationt

# Create data service factory
data_service_factory = DataServiceFactory(
    config=default_config,
    cache_service=cache,
    standard=standard,
    standard_version=standard_version,
    standard_substandard=standard_substandard,
    library_metadata=library_metadata,
    max_dataset_size=max_dataset_size,
)

# Set dataset implementation (imported from cdisc_rules_engine.models.dataset)
dataset_implementation = PandasDataset or DaskDataset

# Get data service
data_service = data_service_factory.get_data_service(dataset_paths)
```

### Step 5: Initialize Rules Engine

Now you can create the rules engine with all required components:

```python
from cdisc_rules_engine.rules_engine import RulesEngine

# Initialize the rules engine
rules_engine = RulesEngine(
    cache=cache,
    data_service=data_service,
    config_obj=default_config,
    external_dictionaries=None, # see bottom for implementation tips
    standard=standard,
    standard_version=standard_version,
    standard_substandard=None, # substandard if indicated
    library_metadata=library_metadata,
    max_dataset_size=max_dataset_size,
    dataset_paths=dataset_paths,
    ct_packages=ct_packages,
    define_xml_path="path/to/define.xml",  # Optional
    validate_xml=False,  # Whether to validate XML against schema
)
```

### Step 6: Run Validation

Now you can iterate through the rules and validate them against your datasets:

```python
import time
import itertools
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult

# Process each rule
start_time = time.time()
validation_results = []

for rule in rules:
    try:
        print(f"Validating rule {rule['core_id']}...")
        if isinstance(rule["conditions"], dict):
            rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
        results = rules_engine.validate_single_rule(rule, datasets)
        flattened_results = []
        for domain_results in results.values():
            flattened_results.extend(domain_results)
        validation_results.append(RuleValidationResult(rule, flattened_results))
    except Exception as e:
        print(f"Error validating rule {rule.get('core_id')}: {str(e)}")
end_time = time.time()
elapsed_time = end_time - start_time
```

### Step 7: Generate Report

The simplest way to output your validation results is to write them to a text file:

```python
import os
import json

output_dir = os.getcwd()
output_file = os.path.join(output_dir, "validation_results.txt")
with open(output_file, "w") as f:
    for result in validation_results:
        rule_id = result.rule.get("core_id", "Unknown")
        f.write(f"Rule: {rule_id}\n")
        if hasattr(result, 'violations') and result.violations:
            f.write(f"Found {len(result.violations)} violations\n")
            for violation in result.violations:
                f.write(f"  - {json.dumps(violation, default=str)}\n")
        else:
            f.write("  No violations found\n")
        f.write("\n")
print(f"Results written to {output_file}")
```

For more advanced reporting, you can implement custom reports using the ReportFactory class

```python
reporting_factory = ReportFactory(
     datasets=datasets,
     validation_results=validation_results,
     elapsed_time=elapsed_time,
     args=args,  # Provide validation args
     data_service=data_service
 )
reporting_services = reporting_factory.get_report_services()
```

### Other Information

## Interpret the Results

The return value of a run will tell us if the rule was triggered.

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

There is also a DaskDataset which can be utilized.

## Understanding DatasetMetadata and column_prefix_map

The column_prefix_map is often dynamically set using the domain information from SDTMDatasetMetadata. Here's how these components work together:

### SDTMDatasetMetadata

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

### How column_prefix_map uses domain Information

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

Here's a comprehensive example showing how to set up your environment with dataset metadata and execute a rule using the business rule implementation:

```python
import os
import pathlib
import pickle
import pandas as pd
import pyreadstat
from multiprocessing import freeze_support
from multiprocessing.managers import SyncManager
from cdisc_rules_engine.services.cache import InMemoryCacheService
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.utilities.utils import get_rules_cache_key
from cdisc_rules_engine.models.actions import COREActions
from business_rules.engine import run

# Step 1: Load the Rules Cache
class CacheManager(SyncManager):
    pass

CacheManager.register("InMemoryCacheService", InMemoryCacheService)

def load_rules_cache(path_to_rules_cache):
    cache_path = pathlib.Path(path_to_rules_cache)
    manager = CacheManager()
    manager.start()
    cache = manager.InMemoryCacheService()

    # Get all files in the cache directory
    files = next(os.walk(cache_path), (None, None, []))[2]

    for fname in files:
        with open(cache_path / fname, "rb") as f:
            cache.add_all(pickle.load(f))

    return cache

def main():
    # Define file paths
    current_dir = os.getcwd()
    cache_path = os.path.join(current_dir, "cache")
    ae_file_path = os.path.join(current_dir, "ae.xpt")

    print(f"Looking for rules cache in: {cache_path}")
    print(f"Using AE dataset from: {ae_file_path}")

    try:
        # Step 1: Load the rules cache
        cache = load_rules_cache(cache_path)
        print("Successfully loaded rules cache")

        # Get SDTMIG 3.4 rules
        cache_key_prefix = get_rules_cache_key("sdtmig", "3-4")
        rules = cache.get_all_by_prefix(cache_key_prefix)
        print(f"Found {len(rules)} rules for SDTMIG 3.4")

        # Step 2: Load and prepare the AE dataset
        # Read the XPT file
        ae_data, meta = pyreadstat.read_xport(ae_file_path)
        print(f"Successfully loaded AE dataset with {len(ae_data)} records")
        print(f"Columns: {', '.join(ae_data.columns)}")

        # Convert to PandasDataset
        pandas_dataset = PandasDataset(data=ae_data)

        # Create dataset metadata
        dataset_metadata = SDTMDatasetMetadata(
            name="AE",
            label=meta.file_label if hasattr(meta, 'file_label') else "Adverse Events",
            first_record=ae_data.iloc[0].to_dict() if not ae_data.empty else None
        )

        # Create the DatasetVariable
        dataset_variable = DatasetVariable(
            pandas_dataset,
            column_prefix_map={"--": dataset_metadata.domain},
        )

        # Step 3: Run the AE Domain Rules
        ae_rules = [
            rule for rule in rules
            if "AE" in rule.get("domains", {}).get("Include", []) or
               "ALL" in rule.get("domains", {}).get("Include", [])
        ]

        print(f"Found {len(ae_rules)} rules applicable to AE domain")

        # Process the rules
        all_results = []

        for idx, rule in enumerate(ae_rules):
            print(f"\nProcessing rule {idx+1}/{len(ae_rules)}: {rule.get('core_id')} - {rule.get('description')[:80]}...")

            results = []
            # Updated initialization of COREActions with correct parameters
            core_actions = COREActions(
                output_container=results,
                variable=dataset_variable,
                dataset_metadata=dataset_metadata,
                rule=rule,
                value_level_metadata=None  # This is optional
            )

            try:
                was_triggered = run(
                    rule=rule,
                    defined_variables=dataset_variable,
                    defined_actions=core_actions,
                )

                if was_triggered and results:
                    print(f"  ❌ Rule {rule.get('core_id')} was triggered - issues found!")
                    for result in results:
                        if isinstance(result, dict) and 'errors' in result and result['errors']:
                            error_count = len(result.get('errors', []))
                            print(f"  Message: {result.get('message')} ({error_count} errors)")
                            # Only show up to 3 errors for brevity
                            for error in result.get('errors', [])[:3]:
                                print(f"    Row: {error.get('row')}, Value: {error.get('value')}")
                            if error_count > 3:
                                print(f"    ... and {error_count - 3} more errors")
                        else:
                            print(f"  Message: {result}")
                    all_results.extend(results)
                else:
                    print(f"  ✓ Rule {rule.get('core_id')} passed - no issues found")
            except Exception as e:
                print(f"  ⚠️ Error processing rule {rule.get('core_id')}: {str(e)}")

        # Summary of results
        print("\n===== VALIDATION SUMMARY =====")
        print(f"Total rules checked: {len(ae_rules)}")
        print(f"Rules with issues: {len(all_results)}")

        # Count total issues, handling different result formats
        issue_count = 0
        for result in all_results:
            if isinstance(result, dict) and 'errors' in result:
                issue_count += len(result['errors'])
            else:
                issue_count += 1

        print(f"Total issues found: {issue_count}")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    freeze_support()
    main()
```

# Troubleshooting

If you're seeing errors related to the dataset integration, check that:

- Your DataFrame contains all the required columns for the validation rules
- The column_prefix_map correctly maps variable prefixes to domains (e.g., {"--": "AE"} for Adverse Events)
- Your column_codelist_map includes all the necessary codelists for variables that have controlled terminology
- Any metadata passed to the DatasetVariable constructor is correctly formatted
- The dataset object is an instance of PandasDataset, not a raw pandas DataFrame
- All required parameters are provided to the DatasetVariable constructor
- Ensure dataset metadata has the correct domain property (usually taken from the first record's DOMAIN column)
- Check that full_path is provided in dataset_metadata when using RulesEngine approach
- Verify the rule's domain inclusion criteria matches your dataset domain (in the rule's domains.Include array)
- Make sure your cache contains the appropriate controlled terminology if validating against standard terminologies
- Confirm the standard_version format is consistent
- If using external dictionaries, verify all file paths are correct and accessible
- When working with define.xml, ensure the define_xml_path is valid and accessible and the file is named `define.xml`

## For Windows compatibility

you will need freeze_support() for multiprocessing compatibility

```python
from multiprocessing import freeze_support

if __name__ == "__main__":
    freeze_support()
    main()
```

## External Dictionaries

To feed external dictionaries into engine, you will need to instantiate the container.

```python
from cdisc_rules_engine.models.external_dictionaries_container import ExternalDictionariesContainer
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes

# Create a dictionary path mapping with the dictionary you are providing
external_dictionaries = ExternalDictionariesContainer(
    {
        DictionaryTypes.UNII.value: unii_path,
        DictionaryTypes.MEDRT.value: medrt_path,
        DictionaryTypes.MEDDRA.value: meddra_path,
        DictionaryTypes.WHODRUG.value: whodrug_path,
        DictionaryTypes.LOINC.value: loinc_path,
        DictionaryTypes.SNOMED.value: {
            "edition": snomed_edition,
            "version": snomed_version,
            "base_url": snomed_url,
        },
    }
)

# Instantiate the container with the paths
external_dictionaries = ExternalDictionariesContainer(dictionary_path_mapping=dictionary_paths)

# Then pass it to RulesEngine
rules_engine = RulesEngine(
    cache=cache,
    dataset_paths=[os.path.dirname(ae_file_path)],
    standard="sdtmig",
    standard_version="3.4",
    external_dictionaries=external_dictionaries
)
```

see [External Dictionary Reference](https://cdisc-org.github.io/conformance-rules-editor/#/exdictionary) for more information
