# PyPI Integration

CORE is available as a Python package for direct integration into your own pipelines and tooling.

```bash
pip install cdisc-rules-engine
```

This installs the engine underlying the CLI and executable, but **does not include `core.py`** or the CLI entrypoints. If you need the full CLI, use the [executable or source code](quick-start.md) instead.

---

## What You'll Need

Installing the package alone is not enough to run validations. You also need:

1. **The rules cache** — download the contents of `resources/cache/` from the [repository](https://github.com/cdisc-org/cdisc-rules-engine) and store them somewhere in your project. Keep this in sync with the package version you're using.
2. **A CDISC Library API key** — required for controlled terminology and library metadata. See [update-cache](cli-reference.md#updating-the-cache-update-cache) for how to obtain one.

The package also includes the USDM and Dataset-JSON schemas, available if you use the dataset reader classes in `cdisc_rules_engine/services/data_readers` or the metadata readers in `cdisc_rules_engine/services`.

---

## Choosing an Approach

|                | Option A: Business Rules Engine | Option B: RulesEngine Class            |
| -------------- | ------------------------------- | -------------------------------------- |
| **Interface**  | Low-level, rule-by-rule         | High-level, dataset-oriented           |
| **Data input** | pandas DataFrame                | XPT or other file-based datasets       |
| **Setup**      | Minimal                         | More configuration required            |
| **Best for**   | Simple in-memory validation     | Full multi-domain validation pipelines |

---

## Loading the Rules Cache

Both options start by loading the cache:

```python
import os
import pathlib
import pickle
from multiprocessing.managers import SyncManager
from cdisc_rules_engine.services.cache import InMemoryCacheService

class CacheManager(SyncManager):
    pass

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

Retrieve rules for a standard and version:

```python
from cdisc_rules_engine.utilities.utils import get_rules_cache_key

cache = load_rules_cache("path/to/rules/cache")
# Note: version uses dashes, not dots
rules = cache.get_all_by_prefix(get_rules_cache_key("sdtmig", "3-4"))
```

Each rule is a dict with keys: `core_id`, `domains`, `author`, `reference`, `sensitivity`, `executability`, `description`, `authorities`, `standards`, `classes`, `rule_type`, `conditions`, `actions`, `datasets`, `output_variables`.

If you have rules in raw CDISC metadata format, convert them first:

```python
from cdisc_rules_engine.models.rule import Rule

rule_dict = Rule.from_cdisc_metadata(rule_metadata)
rule_obj = Rule(rule_dict)
```

---

## Option A: Business Rules Engine

Minimal setup — good for validating a single domain against an in-memory DataFrame.

### Prepare Your Data

```python
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

pandas_dataset = PandasDataset(data=df)
dataset_metadata = SDTMDatasetMetadata(
    name="AE",
    label="Adverse Events",
    first_record=df.iloc[0].to_dict() if not df.empty else None
)
dataset_variable = DatasetVariable(
    pandas_dataset,
    column_prefix_map={"--": dataset_metadata.domain},
)
```

`DatasetVariable` accepts additional optional arguments for richer validation:

```python
dataset_variable = DatasetVariable(
    pandas_dataset,
    column_prefix_map={"--": dataset_metadata.domain},
    value_level_metadata=value_level_metadata,
    column_codelist_map=variable_codelist_map,
    codelist_term_maps=codelist_term_maps,
)
```

### Run Rules

```python
from business_rules.engine import run
from cdisc_rules_engine.models.actions import COREActions

ae_rules = [
    r for r in rules
    if "AE" in r.get("domains", {}).get("Include", [])
    or "ALL" in r.get("domains", {}).get("Include", [])
]

all_results = []
for rule in ae_rules:
    results = []
    core_actions = COREActions(
        output_container=results,
        variable=dataset_variable,
        dataset_metadata=dataset_metadata,
        rule=rule,
        value_level_metadata=None,
    )
    try:
        was_triggered = run(rule=rule, defined_variables=dataset_variable, defined_actions=core_actions)
        if was_triggered:
            all_results.extend(results)
    except Exception as e:
        print(f"Error in {rule.get('core_id')}: {e}")
```

`was_triggered` is `True` if issues were found. Each result in `all_results` looks like:

```python
{
    'executionStatus': 'success',
    'domain': 'AE',
    'variables': ['AESLIFE'],
    'message': 'AESLIFE is completed, but not equal to "N" or "Y"',
    'errors': [{'value': {'AESLIFE': 'Maybe'}, 'row': 1}]
}
```

---

## Option B: RulesEngine Class

More setup, but handles dataset reading, preprocessing, and multi-domain validation. The source code in `cdisc_rules_engine/rules_engine.py` and the existing CLI implementation in `core.py` are the best reference for wiring this together — the initializer arguments map closely to the CLI flags documented in the [CLI Reference](cli-reference.md).

### Step 1: Prepare Dataset Metadata

```python
import os
import pyreadstat
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

def create_dataset_metadata(file_path):
    data, meta = pyreadstat.read_xport(file_path)
    first_record = data.iloc[0].to_dict() if not data.empty else None
    return SDTMDatasetMetadata(
        name=os.path.basename(file_path).split('.')[0].upper(),
        label=meta.file_label if hasattr(meta, 'file_label') else "",
        filename=os.path.basename(file_path),
        full_path=file_path,
        file_size=os.path.getsize(file_path),
        record_count=len(data),
        first_record=first_record,
    )

datasets = [
    create_dataset_metadata(os.path.join(directory, f))
    for f in os.listdir(directory)
    if f.lower().endswith('.xpt')
]
```

You don't need to manually create `PandasDataset` or `DatasetVariable` objects for Option B — the engine handles this internally.

### Step 2: Initialize Library Metadata

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

standard_metadata = cache.get(get_standard_details_cache_key(standard, standard_version, standard_substandard))
model_metadata = cache.get(get_model_details_cache_key_from_ig(standard_metadata)) if standard_metadata else {}

ct_packages = ["sdtmct-2021-12-17"]  # replace with your CT package versions
ct_package_metadata = {pkg: cache.get(pkg) for pkg in ct_packages}

library_metadata = LibraryMetadataContainer(
    standard_metadata=standard_metadata,
    model_metadata=model_metadata,
    variables_metadata=cache.get(get_library_variables_metadata_cache_key(standard, standard_version, standard_substandard)),
    variable_codelist_map=cache.get(get_variable_codelist_map_cache_key(standard, standard_version, standard_substandard)),
    ct_package_metadata=ct_package_metadata,
)
```

### Step 3: Initialize Data Service

```python
from cdisc_rules_engine.config import config as default_config
from cdisc_rules_engine.services.data_services import DataServiceFactory

max_dataset_size = max(datasets, key=lambda x: x.file_size).file_size
# Set max_dataset_size=0 to force Dask processing for all datasets

data_service_factory = DataServiceFactory(
    config=default_config,
    cache_service=cache,
    standard=standard,
    standard_version=standard_version,
    standard_substandard=standard_substandard,
    library_metadata=library_metadata,
    max_dataset_size=max_dataset_size,
)

data_service = data_service_factory.get_data_service(dataset_paths)
```

### Step 4: Initialize Rules Engine

```python
from cdisc_rules_engine.rules_engine import RulesEngine

rules_engine = RulesEngine(
    cache=cache,
    data_service=data_service,
    config_obj=default_config,
    external_dictionaries=None,
    standard=standard,
    standard_version=standard_version,
    standard_substandard=None,
    library_metadata=library_metadata,
    max_dataset_size=max_dataset_size,
    dataset_paths=dataset_paths,
    ct_packages=ct_packages,
    define_xml_path="path/to/define.xml",  # optional
    validate_xml=False,
)
```

### Step 5: Run Validation

Note the `ConditionCompositeFactory` conversion step — this is required before passing rules to `validate_single_rule`:

```python
import time
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult

start_time = time.time()
validation_results = []

for rule in rules:
    try:
        if isinstance(rule["conditions"], dict):
            rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
        results = rules_engine.validate_single_rule(rule, datasets)
        flattened = [r for domain_results in results.values() for r in domain_results]
        validation_results.append(RuleValidationResult(rule, flattened))
    except Exception as e:
        print(f"Error validating rule {rule.get('core_id')}: {e}")

elapsed_time = time.time() - start_time
```

### Step 6: Generate Report

Simple text output:

```python
import json

with open("validation_results.txt", "w") as f:
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
```

For structured output, use `ReportFactory`:

```python
reporting_factory = ReportFactory(
    datasets=datasets,
    validation_results=validation_results,
    elapsed_time=elapsed_time,
    args=args,
    data_service=data_service,
)
reporting_services = reporting_factory.get_report_services()
```

---

## Notes

**Cache key format** — always use dashes in version strings (`3-4`, not `3.4`).

**`column_prefix_map`** — maps the `--` variable prefix to the dataset domain (e.g. `{"--": "AE"}`), resolving placeholders like `--SEQ` → `AESEQ`.

**External dictionaries** — pass an `ExternalDictionariesContainer` to `RulesEngine` if validating rules that require MedDRA, WHODrug, LOINC, UNII, MedRT, or SNOMED. See the [External Dictionary Reference](https://cdisc-org.github.io/cdisc-open-rules/#/exdictionary).

**Dask** — set `max_dataset_size=0` when initializing `DataServiceFactory` to force Dask processing for all datasets.

**Windows compatibility** — add `freeze_support()` for multiprocessing:

```python
from multiprocessing import freeze_support

if __name__ == "__main__":
    freeze_support()
    main()
```

---

## Troubleshooting

- Ensure the DataFrame contains all required columns for the rules being run
- `column_prefix_map` must correctly map `"--"` to the domain (e.g. `{"--": "AE"}`)
- The dataset object must be a `PandasDataset` instance, not a raw pandas DataFrame
- `full_path` must be set in `SDTMDatasetMetadata` when using the `RulesEngine` approach
- The rule's `domains.Include` must match your dataset's domain
- `standard_version` format must be consistent throughout (`3-4`, not `3.4`)
- CT package metadata must be present in the cache if validating against controlled terminology
- When using `define.xml`, the file must be named `define.xml` and the path must be valid
- If using external dictionaries, verify all file paths are correct and accessible
- Don't forget the `ConditionCompositeFactory` conversion before calling `validate_single_rule` (Option B)
