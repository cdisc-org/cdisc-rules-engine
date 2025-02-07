# External Dictionaries

External dictionaries provide standardized terminology and coding systems for medical terms, drugs, and laboratory tests. This guide covers how to integrate and validate external dictionaries in both the Command Line Interface (CLI) and Rule Editor.

## Supported Dictionaries

- **MedDRA**: Medical terminology for regulatory activities
- **WHODrug**: Global drug reference dictionary
- **LOINC**: Laboratory test terminology
- **MEDRT**: Medication Reference Terminology
- **UNII**: Unique Ingredient Identifier
- **SNOMED CT**: Clinical healthcare terminology

## CLI Usage

> **IMPORTANT**: All dictionary paths must point to the _directory containing_ the dictionary files, not to specific files.

### Medical Terminology Dictionaries

#### MedDRA

```
--meddra TEXT      Path to directory containing MedDRA dictionary files
                   Required files in directory:
                    Term files (*.asc format):
                        - pt.asc (Preferred Terms)
                        - llt.asc (Lowest Level Terms)
                        - hlt.asc (High Level Terms)
                        - hlgt.asc (High Level Group Terms)
                        - soc.asc (System Organ Classes)
                    Relationship files:
                        - soc_hlgt.asc
                        - hlgt_hlt.asc
                        - hlt_pt.asc
                    Version file:
                        - version.txt containing version number followed by $ delimiter
                            Example: 24.1$
```

#### SNOMED CT

```
--snomed-version TEXT    Version of SNOMED to use (e.g., 2024-09-01)
--snomed-url TEXT        Base URL of SNOMED API (e.g., https://snowstorm.snomedtools.org/snowstorm/snomed-ct)
--snomed-edition TEXT    Edition of SNOMED to use (e.g., SNOMEDCT-US)
```

For SNOMED CT:

- API access requires hosting your own instance using [Snowstorm](https://github.com/IHTSDO/snowstorm) or an alternative implementation
  as https://snowstorm.snomedtools.org/snowstorm/snomed-ct is not for commercial use
- Browse terms at [SNOMED Browser](https://browser.ihtsdotools.org)

### Drug and Substance Dictionaries

#### WHODrug

```
--whodrug TEXT     Path to directory containing WHODrug dictionary files
                    Required files:
                      - DD.txt (Drug Dictionary)
                      - DDA.txt (ATC Classification)
                      - INA.txt (ATC Text)
                      - version.txt (Contains Vault Safety label format, e.g., "GLOBALC3Mar24")

                    Supported formats:
                      - B3: Substance Name field is 45 characters
                      - C3: Substance Name field is 110-250 characters (expanded after March 2022)
```

#### Other Drug Dictionaries

```
--medrt TEXT       Path to directory containing MEDRT dictionary files
                    Dictionary file must be named `Core_MEDRT_*_DTS.xml`

--unii TEXT        Path to directory containing UNII dictionary files
                    Required files:
                        - UNII_Records_*.* (tab-delimited file containing UNII codes and terms)
                            Format: Tab-delimited file with following columns:
                                1. UNII code
                                2. UNII term
                            Note: Version is extracted from filename (e.g., "UNII_Records_2024.txt" → version "2024")
```

### Laboratory Dictionaries

```
--loinc TEXT       Path to directory containing LOINC dictionary files
                    Directory must contain the `Loinc.csv` with capital 'L'
```

## Operations & Rule Editor

### Dictionary Version Validation

#### valid_define_external_dictionary_version

Validates dictionary versions against define.xml specifications.

```yaml
- operation: valid_define_external_dictionary_version
  id: $is_valid_loinc_version
  external_dictionary_type: loinc
```

### Value and Code Validation

#### valid_external_dictionary_value

Validates dictionary values with optional case sensitivity.

```yaml
- operation: valid_external_dictionary_value
  name: --DECOD
  id: $is_valid_decod_value
  external_dictionary_type: meddra
  dictionary_term_type: PT
  case_sensitive: false
```

#### valid_external_dictionary_code

Validates dictionary codes.

```yaml
- operation: valid_external_dictionary_code
  name: --COD
  id: $is_valid_cod_code
  external_dictionary_type: meddra
  dictionary_term_type: PT
```

#### valid_external_dictionary_code_term_pair

Validates matching of code-term pairs.

```yaml
- operation: valid_external_dictionary_code_term_pair
  name: --COD
  id: $is_valid_loinc_code_term_pair
  external_dictionary_type: loinc
  external_dictionary_term_variable: --DECOD
```

### MedDRA-Specific Operations

#### valid_meddra_code_references

Validates MedDRA codes across all levels:

- `--SOCCD` (System Organ Class Code)
- `--HLGTCD` (High Level Group Term Code)
- `--HLTCD` (High Level Term Code)
- `--PTCD` (Preferred Term Code)
- `--LLTCD` (Lowest Level Term Code)

Example:

```yaml
- operation: valid_meddra_code_references
  id: $meddra_codes_valid
```

#### valid_meddra_code_term_pairs

Validates corresponding code-term pairs:

- `--SOCCD` with `--SOC`
- `--HLGTCD` with `--HLGT`
- `--HLTCD` with `--HLT`
- `--PTCD` with `--DECOD`
- `--LLTCD` with `--LLT`

Example:

```yaml
- operation: valid_meddra_code_term_pairs
  id: $meddra_pairs_valid
```

#### valid_meddra_term_references

Validates terms at each MedDRA level:

- `--SOC` (System Organ Class)
- `--HLGT` (High Level Group Term)
- `--HLT` (High Level Term)
- `--DECOD` (Decoded Term)
- `--LLT` (Lowest Level Term)

Example:

```yaml
- operation: valid_meddra_term_references
  id: $meddra_terms_valid
```

### WHODrug-Specific Operations

#### valid_whodrug_references

Validates WHODrug terms against the ATC Text (INA) file.

Example:

```yaml
- operation: valid_whodrug_references
  id: $whodrug_refs_valid
```

#### valid_whodrug_code_hierarchy

Validates hierarchical relationships between:

- `--DECOD`
- `--CLAS`
- `--CLASCD`

Example:

```yaml
- operation: valid_whodrug_code_hierarchy
  id: $whodrug_hierarchy_valid
```
