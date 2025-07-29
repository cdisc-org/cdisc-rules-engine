import pytest
import pickle


@pytest.fixture
def get_core_rule(pytestconfig) -> dict:
    def _call_core_rule(rule: str) -> dict:
        with open(str(pytestconfig.rootpath) + "/resources/cache/rules.pkl", "rb") as f:
            rules = pickle.load(f)
        return rules.get(rule)

    return _call_core_rule


@pytest.fixture
def get_sample_lb_rule() -> dict:
    return {
        "core_id": "QC.CDISC.SDTMIG.CG0032",
        "classes": {"Include": ["ALL"]},
        "domains": {"Include": ["ALL"]},
        "rule_type": "Range & Limit",
        "sensitivity": "Record",
        "severity": "error",
        "Authorities": [{"Standards": [{"Name": "SDTMIG", "Version": "3.4"}]}],
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "greater_than",
                    "value": {"target": "LBSEQ", "comparator": 0},
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "LBSEQ greater than 0"},
            }
        ],
    }
