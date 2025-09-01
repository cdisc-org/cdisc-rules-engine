import pickle
from ast import literal_eval

import pandas as pd
import pytest


@pytest.fixture
def get_core_rule(pytestconfig) -> dict:
    def _call_core_rule(rule: str) -> dict:
        with open(str(pytestconfig.rootpath) + "/resources/cache/rules.pkl", "rb") as f:
            rules = pickle.load(f)
        return rules.get(rule)

    return _call_core_rule


@pytest.fixture
def get_core_rules_df(pytestconfig) -> pd.DataFrame:
    def _call_core_rules_df() -> dict:
        sdtm = pd.read_csv(
            str(pytestconfig.rootpath) + "/tests/resources/rules/sdtm_rules.csv",
            converters={
                "std": lambda s: [] if pd.isna(s) or s == "" else literal_eval(s),
                "rids": lambda s: [] if pd.isna(s) or s == "" else literal_eval(s),
            },
        )
        sdtm["standard_source"] = "SDTMIG"
        adam = pd.read_csv(
            str(pytestconfig.rootpath) + "/tests/resources/rules/adam_rules.csv",
            converters={
                "std": lambda s: [] if pd.isna(s) or s == "" else literal_eval(s),
                "rids": lambda s: [] if pd.isna(s) or s == "" else literal_eval(s),
            },
        )
        adam["standard_source"] = "ADAMIG"
        define = pd.read_csv(
            str(pytestconfig.rootpath) + "/tests/resources/rules/define_rules.csv",
            converters={
                "std": lambda s: [] if pd.isna(s) or s == "" else literal_eval(s),
                "rids": lambda s: [] if pd.isna(s) or s == "" else literal_eval(s),
            },
        )
        define["standard_source"] = "define"
        return_df = pd.concat([sdtm, adam, define]).copy()
        return_df_sorted = return_df.sort_values(by="Core-ID", ascending=True, na_position="last")
        return return_df_sorted

    return _call_core_rules_df


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
                    "operator": "less_than",
                    "value": {"target": "LBSEQ", "comparator": "$lbseqmax"},
                }
            ]
        },
        "operations": [
            {
                "id": "$lbseqmax",
                "name": "LBSEQ",
                "operator": "max",
            }
        ],
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "LBSEQ less than maximum value"},
            }
        ],
    }
