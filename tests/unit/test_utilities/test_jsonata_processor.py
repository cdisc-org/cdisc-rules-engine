from unittest.mock import MagicMock, patch
from yaml import safe_load
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.utilities.jsonata_processor import JSONataProcessor


@patch(
    "cdisc_rules_engine.utilities.jsonata_processor.JSONataProcessor.get_custom_functions"
)
def test_jsonata_processor(mock_get_custom_functions: MagicMock):
    rule = """
        Check: |
            **.$filter($, $utils.equals).{"path":path, "A":A, "B":B}
        Core:
            Id: JSONATA Test
        Status: Draft
        Outcome:
            Message: "A equals B"
            Output Variables:
                - id
                - name
                - path
                - A
                - B
        Rule Type: JSONata
        Scope:
            Entities:
                Include:
                - ALL
        Sensitivity: Record
    """
    mock_get_custom_functions.return_value = """
        $utils:={
            "equals": function($v){ $v.A=$v.B }
        };
    """
    dataset = {
        "path": "",
        "A": "same value 1",
        "B": "same value 1",
        "C": {
            "path": "C",
            "A": "different value 1",
            "B": "different value 2",
            "C": {"path": "C.C", "A": "same value 2", "B": "same value 2"},
        },
    }
    expected = [
        {
            "executionStatus": "success",
            "dataset": "",
            "domain": None,
            "variables": ["A", "B", "id", "name", "path"],
            "message": None,
            "errors": [
                {
                    "value": {"path": "", "A": "same value 1", "B": "same value 1"},
                    "dataset": "",
                    "row": "",
                },
                {
                    "value": {"path": "C.C", "A": "same value 2", "B": "same value 2"},
                    "dataset": "",
                    "row": "C.C",
                },
            ],
        }
    ]
    rule = Rule.from_cdisc_metadata(safe_load(rule))
    result = JSONataProcessor.execute_jsonata_rule(
        rule=rule,
        dataset=dataset,
        dataset_metadata=SDTMDatasetMetadata(),
        jsonata_functions_path="",
    )

    assert result == expected
