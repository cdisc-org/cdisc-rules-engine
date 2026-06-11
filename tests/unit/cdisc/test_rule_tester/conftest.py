import pytest


@pytest.fixture
def input_yml():
    return """Core:
  Id: CORE-CG0092
Version: "1"
Authority:
  Organization: CDISC
Reference:
  Origin: SDTM Conformance Rules
  Version: "2.0"
  Id: CG0092
Description: Raise an error when --TPTREF is not present in dataset.
Sensitivity: Dataset
Severity: Error
Scopes:
  Standards:
    - Name: SDTMIG
      Version: "3.4"
  Classes:
    Include:
      - All
  Domains:
    Include:
      - All
Rule Type:
  Variable Presence:
    Check:
      all:
        - name: "--ELTM"
          operator: "non_empty"
        - name: "-TPTREF"
          operator: "empty"
Outcome:
  Message: --TPTREF is not present when --ELTM present in a dataset
Citations:
  - Document: SDTM Model v2.0
    Section: Timing
    Cited Guidance: >-
      "The description of a time point that acts as a fixed reference for a
      series of planned
      time points, used for study data tabulation. Description of the fixed
      reference point referred to by --ELTM, --TPTNUM, --TPT, --STINT, and
      --ENINT."""
