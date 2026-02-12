import json
import os
import subprocess
import unittest
import pytest
from conftest import get_python_executable


"""This regression test is for automating the validation of acceptancce criteria
which is "For any variables that come from datasets and appear in the results,
the variables should have the same case as the variable names in the dataset".
For Other checks please run the related unit tests"""


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
            f"{get_python_executable()}",
            "-m",
            "core",
            "validate",
            "-s",
            "sdtmig",
            "-v",
            "3.4",
            "-dp",
            os.path.join("resources", "datasets", "ae.xpt"),
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
        search_keys = ["usubjid", "seq"]

        # Find the values for the searched key
        result = []
        for key in search_keys:
            values = find_value(json_data, key)
            for val in values:
                result.append(val)

        # Check if all values are capitalized
        for value in result:
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
