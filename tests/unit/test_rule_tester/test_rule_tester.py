from os import path
from unittest.mock import patch

from scripts.run_validation import run_single_rule_validation

test_define_file_path: str = (
    f"{path.dirname(__file__)}/../../resources/test_defineV22-SDTM.xml"
)


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_with_errors(mock_get_dataset_class):
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "variables": [
                {
                    "name": "DOMAIN",
                    "label": "Domain Abbreviation",
                    "type": "Char",
                    "length": 4,
                },
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
            ],
            "records": {
                "DOMAIN": ["LB", "LB"],
                "LBSEQ": [1, 2],
            },
        }
    ]
    rule = {
        "core_id": "QC.CDISC.SDTMIG.CG0032",
        "classes": {"Include": ["ALL"]},
        "domains": {"Include": ["ALL"]},
        "rule_type": "Range & Limit",
        "sensitivity": "Value",
        "severity": "error",
        "Authorities": [{"Standards": [{"Name": "SDTMIG", "Version": "3.4"}]}],
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "less_than",
                    "value": {"target": "LBSEQ", "comparator": 2},
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "LBSEQ less than 2"},
            }
        ],
    }
    mock_get_dataset_class.return_value = None
    data = run_single_rule_validation(datasets, rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert len(data["LB"][0]["errors"]) == 1
    error = data["LB"][0]["errors"][0]["value"]
    assert error["row"] == 0
    assert error["SEQ"] == 0
    assert error["uSubjId"] == "N/A"
    assert error["value"] == {"ERROR": "Invalid or undefined sensitivity in the rule"}


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_without_errors(mock_get_dataset_class):
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "variables": [
                {
                    "name": "DOMAIN",
                    "label": "Domain Abbreviation",
                    "type": "Char",
                    "length": 4,
                },
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
            ],
            "records": {
                "DOMAIN": ["LB", "LB"],
                "LBSEQ": [1, 2],
            },
        }
    ]
    rule = {
        "core_id": "QC.CDISC.SDTMIG.CG0032",
        "classes": {"Include": ["ALL"]},
        "domains": {"Include": ["ALL"]},
        "rule_type": "Range & Limit",
        "sensitivity": "Value",
        "severity": "error",
        "Authorities": [{"Standards": [{"Name": "SDTMIG", "Version": "3.4"}]}],
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "greater_than",
                    "value": {"target": "LBSEQ", "comparator": 2},
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "LBSEQ greater than 2"},
            }
        ],
    }
    mock_get_dataset_class.return_value = None
    data = run_single_rule_validation(datasets, rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert len(data["LB"][0]["errors"]) == 0


def test_rule_skipped():
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "variables": [
                {
                    "name": "DOMAIN",
                    "label": "Domain Abbreviation",
                    "type": "Char",
                    "length": 4,
                },
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
            ],
            "records": {
                "DOMAIN": ["LB", "LB"],
                "LBSEQ": [1, 2],
            },
        }
    ]
    rule = {
        "core_id": "QC.CDISC.SDTMIG.CG0032",
        "classes": {"Include": ["ALL"]},
        "domains": {"Exclude": ["LB"]},
        "rule_type": "Range & Limit",
        "sensitivity": "Value",
        "severity": "error",
        "Authorities": [{"Standards": [{"Name": "SDTMIG", "Version": "3.4"}]}],
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
        "conditions": {
            "all": [
                {
                    "name": "get_dataset",
                    "operator": "greater_than",
                    "value": {"target": "AESEQ", "comparator": 2},
                }
            ]
        },
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {"message": "AEWSEQ greater than 2"},
            }
        ],
    }
    data = run_single_rule_validation(datasets, rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert len(data["LB"][0]["errors"]) == 0
    assert data["LB"][0]["executionStatus"] == "skipped"


def test_rule_with_define_xml(define_xml_variable_validation_rule: dict):
    datasets = [
        {
            "filename": "ae.xpt",
            "variables": [
                {
                    "name": "DOMAIN",
                    "label": "Domain Abbreviation",
                    "type": "Char",
                    "length": 4,
                },
                {
                    "name": "USUBJID",
                    "label": "Unique Subject Id",
                    "type": "Num",
                    "length": 8,
                },
                {
                    "name": "AESEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                },
                {
                    "name": "AESTDY",
                    "label": "Study Day",
                    "type": "Char",
                    "length": 200,
                },
            ],
            "records": {
                "DOMAIN": ["AE", "AE", "AE", "AE", "AE"],
                "USUBJID": [
                    1,
                    2,
                    2,
                    1,
                    3,
                ],
                "AESEQ": [
                    1,
                    2,
                    3,
                    4,
                    5,
                ],
                "AESTDY": ["test", "alex", "alex", "test", "test"],
            },
        }
    ]

    with open(test_define_file_path, "r") as file:
        contents: str = file.read()
        data = run_single_rule_validation(
            datasets, rule=define_xml_variable_validation_rule, define_xml=contents
        )
        assert "AE" in data
        assert len(data["AE"]) == 1
        assert len(data["AE"][0]["errors"]) > 0
        error = data["AE"][0]["errors"][1]
        assert error["row"] == 3
        assert error["value"] == {"variable_size": 8}
