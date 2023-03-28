from cdisc_rule_tester.models.rule_tester import RuleTester


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
