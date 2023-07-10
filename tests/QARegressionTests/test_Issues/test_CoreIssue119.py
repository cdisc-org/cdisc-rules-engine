from business_rules.operators import DataframeType
import pandas as pd
import unittest
import pytest

"""This regression test is for accpetance criteria of Core Issue 119.
These tests only checks if the is_unique_set and is_not_unique_set functions now
handle the missing columns in the value list properly by ignoring them or not.
For other tests please efer to the unit tests"""


@pytest.mark.skip
class TestCoreIssue119(unittest.TestCase):
    def setUp(self):
        data = {
            "Name": ["John", "Jane", "John", "Jane"],
            "Age": [25, 30, 25, 30],
            "City": ["New York", "London", "Paris", "London"],
        }
        self.df = pd.DataFrame(data)
        self.df_type = DataframeType({"value": self.df})

    def test_is_unique_set_with_missing_columns(self):
        # Columns missing in the dataset
        result = self.df_type.is_unique_set(
            {"target": "Name", "comparator": ["Age", "Country"]}
        )
        expected = pd.Series([False, False, False, False])
        self.assertTrue(result.equals(expected))

    def test_is_not_unique_set_with_missing_columns(self):
        # Columns missing in the dataset
        result = self.df_type.is_not_unique_set(
            {"target": "Name", "comparator": ["Age", "Country"]}
        )
        expected = pd.Series([True, True, True, True])
        self.assertTrue(result.equals(expected))


if __name__ == "__main__":
    unittest.main()
