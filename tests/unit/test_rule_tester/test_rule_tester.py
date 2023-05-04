from cdisc_rule_tester.models.rule_tester import RuleTester
from os import path

test_define_file_path: str = (
    f"{path.dirname(__file__)}/../../resources/test_defineV22-SDTM.xml"
)


def test_rule_with_errors():
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "domain": "LB",
            "variables": [
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                }
            ],
            "records": {
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
    tester = RuleTester(datasets)
    data = tester.validate(rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert len(data["LB"][0]["errors"]) == 1
    error = data["LB"][0]["errors"][0]
    assert error["row"] == 1
    assert error["SEQ"] == 1
    assert error["value"] == {"LBSEQ": 1}


def test_rule_without_errors():
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "domain": "LB",
            "variables": [
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                }
            ],
            "records": {
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
    tester = RuleTester(datasets)
    data = tester.validate(rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert len(data["LB"][0]["errors"]) == 0


def test_rule_skipped():
    datasets = [
        {
            "filename": "lb.xpt",
            "label": "Laboratory Test Results",
            "domain": "LB",
            "variables": [
                {
                    "name": "LBSEQ",
                    "label": "Sequence Number",
                    "type": "Num",
                    "length": 8,
                }
            ],
            "records": {
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
    tester = RuleTester(datasets)
    data = tester.validate(rule)
    assert "LB" in data
    assert len(data["LB"]) == 1
    assert len(data["LB"][0]["errors"]) == 0
    assert data["LB"][0]["executionStatus"] == "skipped"


def test_rule_with_define_xml(define_xml_variable_validation_rule: dict):
    datasets = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
            "variables": [
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
        contents: bytes = file.read()
        tester = RuleTester(datasets, contents)
        data = tester.validate(define_xml_variable_validation_rule)
        assert "AE" in data
        assert len(data["AE"]) == 1
        assert len(data["AE"][0]["errors"]) > 0
        error = data["AE"][0]["errors"][0]
        assert error["row"] == 2
        assert error["value"] == {"variable_size": 8}
