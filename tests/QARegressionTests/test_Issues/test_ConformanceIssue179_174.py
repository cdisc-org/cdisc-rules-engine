import json
import os
import unittest
import pytest


@pytest.mark.skip
class TestConformanceIssue179_174(unittest.TestCase):
    def test_rule_id_in_citations(self):
        # Load the JSON file
        with open(
            os.path.join("tests", "resources", "ConformanceIssue179_174", "Rule.json"),
            "r",
        ) as file:
            data = json.load(file)
            cited_guidance = data["Authorities"][0]["Standards"][0]["References"][0][
                "Citations"
            ][0]["Cited_Guidance"]

        # Assert checking if the citations.citedguidance have Rule Id
        self.assertIn("Id", cited_guidance)

    def test_rule_id_regex_match(self):
        # Load the JSON file
        with open(
            os.path.join("tests", "resources", "ConformanceIssue179_174", "Rule.json"),
            "r",
        ) as file:
            data = json.load(file)
            cited_guidance = data["Authorities"][0]["Standards"][0]["References"][0][
                "Citations"
            ][0]["Cited_Guidance"]

        # Check if Id in Rule Identifier matches the regex pattern
        rule_id_regex = r"(CT|SD|SE)\d{4}[A-Z]?"
        self.assertRegex(cited_guidance, rule_id_regex)

    def test_fda_references_version(self):
        # Load the JSON file
        with open(
            os.path.join("tests", "resources", "ConformanceIssue179_174", "Rule.json"),
            "r",
        ) as file:
            data = json.load(file)

        # Check FDA Standards.References.Version
        self.assertEqual(
            data["Authorities"][0]["Standards"][0]["References"][0]["Version"], "1.6"
        )

    def test_citations_document(self):
        # Load the JSON file
        with open(
            os.path.join("tests", "resources", "ConformanceIssue179_174", "Rule.json"),
            "r",
        ) as file:
            data = json.load(file)

        # Check Citations.Document
        self.assertIn(
            data["Authorities"][0]["Standards"][0]["References"][0]["Citations"][0][
                "Document"
            ],
            ["FDA", "CDISC"],
        )


if __name__ == "__main__":
    unittest.main()
