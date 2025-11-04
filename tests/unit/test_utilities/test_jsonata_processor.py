from unittest import TestCase
from unittest.mock import MagicMock, patch
from yaml import safe_load
from cdisc_rules_engine.dataset_builders.jsonata_dataset_builder import (
    add_json_pointer_paths,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    MissingDataError,
    RuleExecutionError,
    RuleFormatError,
)
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.utilities.jsonata_processor import JSONataProcessor


class TestJSONataProcessor(TestCase):

    rule = """
        Check: |
            **.$filter($, $utils.equals).{"row":_path, "A":A, "B":B}
        Core:
            Id: JSONATA Test
        Status: Draft
        Outcome:
            Message: "A equals B"
            Output Variables:
                - row
                - A
                - B
        Rule Type: JSONata
        Scope:
            Entities:
                Include:
                - ALL
        Sensitivity: Record
    """
    get_custom_functions = """
        $utils:={
            "equals": function($v){ $v.A=$v.B }
        };
    """
    dataset = {
        "A": "same value 1",
        "B": "same value 1",
        "C": {
            "A": "different value 1",
            "B": "different value 2",
            "C": {"A": "same value 2", "B": "same value 2"},
        },
    }
    expected = [
        {
            "executionStatus": "success",
            "dataset": None,
            "domain": None,
            "variables": ["A", "B", "row"],
            "message": "A equals B",
            "errors": [
                {
                    "value": {"row": "", "A": "same value 1", "B": "same value 1"},
                    "row": "",
                },
                {
                    "value": {
                        "row": "/C/C",
                        "A": "same value 2",
                        "B": "same value 2",
                    },
                    "row": "/C/C",
                },
            ],
        }
    ]

    @patch(
        "cdisc_rules_engine.utilities.jsonata_processor.JSONataProcessor.get_custom_functions"
    )
    def test_jsonata_processor(self, mock_get_custom_functions: MagicMock):
        mock_get_custom_functions.return_value = self.get_custom_functions
        rule = Rule.from_cdisc_metadata(safe_load(self.rule))
        add_json_pointer_paths(self.dataset)
        result = JSONataProcessor.execute_jsonata_rule(
            rule=rule,
            dataset=self.dataset,
            jsonata_custom_functions=(),
        )
        assert result == self.expected

    @patch(
        "cdisc_rules_engine.utilities.jsonata_processor.JSONataProcessor.get_custom_functions"
    )
    def test_jsonata_rule_parsing_error(self, mock_get_custom_functions: MagicMock):
        rule = """
            Check: |
                Bad jsonata rule
            Core:
                Id: JSONATA Test
            Status: Draft
            Outcome:
                Message: "A equals B"
                Output Variables:
                    - row
                    - A
                    - B
            Rule Type: JSONata
            Scope:
                Entities:
                    Include:
                    - ALL
            Sensitivity: Record
        """
        mock_get_custom_functions.return_value = self.get_custom_functions
        rule = Rule.from_cdisc_metadata(safe_load(rule))
        with self.assertRaises(RuleFormatError):
            JSONataProcessor.execute_jsonata_rule(
                rule=rule,
                dataset=self.dataset,
                jsonata_custom_functions=(),
            )

    @patch(
        "cdisc_rules_engine.utilities.jsonata_processor.JSONataProcessor.get_custom_functions"
    )
    def test_jsonata_rule_execution_error(self, mock_get_custom_functions: MagicMock):
        rule = """
            Check: |
                **.$filter($, $missing_utils.equals).{"row":path, "A":A, "B":B}
            Core:
                Id: JSONATA Test
            Status: Draft
            Outcome:
                Message: "A equals B"
                Output Variables:
                    - row
                    - A
                    - B
            Rule Type: JSONata
            Scope:
                Entities:
                    Include:
                    - ALL
            Sensitivity: Record
        """
        mock_get_custom_functions.return_value = self.get_custom_functions
        rule = Rule.from_cdisc_metadata(safe_load(rule))
        with self.assertRaises(RuleExecutionError):
            JSONataProcessor.execute_jsonata_rule(
                rule=rule,
                dataset=self.dataset,
                jsonata_custom_functions=(),
            )

    def test_jsonata_rule_custom_load_error(self):
        rule = Rule.from_cdisc_metadata(safe_load(self.rule))
        with self.assertRaises(MissingDataError):
            JSONataProcessor.execute_jsonata_rule(
                rule=rule,
                dataset=self.dataset,
                jsonata_custom_functions=(("utils_name", "bad_path"),),
            )
