import json
import os
import subprocess
import unittest
import pytest

"""This regression test is for automating the validation of acceptancce criteria
of CORE issue 208. For Other checks please run the related unit tests"""


def find_value(json_data, key):
    results = []

    if isinstance(json_data, dict):
        for k, v in json_data.items():
            if k == key:
                results.append(v)
            elif isinstance(v, (dict, list)):
                results.extend(find_value(v, key))
    elif isinstance(json_data, list):
        for item in json_data:
            if isinstance(item, (dict, list)):
                results.extend(find_value(item, key))

    return results


@pytest.mark.regression
class JSONSearchTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Run the command to generate the JSON file
        command = [
            "python",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            "tests/resources/datasets/ae.xpt",
            "-of",
            "JSON",
        ]
        subprocess.run(command, check=True)

        # Get the latest created JSON file
        files = os.listdir()
        json_files = [
            file
            for file in files
            if file.startswith("CORE-Report-") and file.endswith(".json")
        ]
        cls.json_file_path = sorted(json_files)[-1]

    def test_searched_values_are_capital(self):
        # Read the generated JSON file
        with open(self.json_file_path, "r") as file:
            json_data = json.load(file)

        # Key to search for
        search_key = "your_key"

        # Find the values for the searched key
        values = find_value(json_data, search_key)

        # Check if all values are capitalized
        for value in values:
            self.assertEqual(
                value, value.upper(), f"Value '{value}' is not capitalized."
            )

    @classmethod
    def tearDownClass(cls):
        # Delete the generated JSON file
        if os.path.exists(cls.json_file_path):
            os.remove(cls.json_file_path)


if __name__ == "__main__":
    unittest.main()
