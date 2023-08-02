import unittest
import pandas as pd
from business_rules.utils import flatten_list
import pytest


@pytest.mark.regression
class TestFlattenList(unittest.TestCase):
    def test_flatten_list_with_array_column(self):
        data = pd.DataFrame({"ARRAY_COLUMN": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]})
        result = list(flatten_list(data, ["ARRAY_COLUMN"]))
        expected = [1, 2, 3]
        self.assertEqual(result, expected)

    def test_flatten_list_with_nonexistent_column(self):
        data = pd.DataFrame({"COLUMN_A": [1, 2, 3], "COLUMN_B": ["A", "B", "C"]})
        result = list(flatten_list(data, ["COLUMN_C"]))
        expected = ["COLUMN_C"]
        self.assertEqual(result, expected)

    def test_flatten_list_with_mixed_columns(self):
        data = pd.DataFrame(
            {
                "ARRAY_COLUMN": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                "NON_ARRAY_COLUMN": ["A", "B", "C"],
            }
        )
        result = list(flatten_list(data, ["ARRAY_COLUMN", "NON_ARRAY_COLUMN"]))
        expected = [1, 2, 3, "NON_ARRAY_COLUMN"]
        self.assertEqual(result, expected)

    def test_flatten_list_with_empty_dataframe(self):
        empty_data = pd.DataFrame()
        result = list(flatten_list(empty_data, ["COLUMN"]))
        expected = ["COLUMN"]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
