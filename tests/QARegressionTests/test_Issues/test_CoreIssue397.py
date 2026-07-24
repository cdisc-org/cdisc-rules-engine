import unittest
import pandas as pd
from cdisc_rules_engine.check_operators.helpers import flatten_list


class TestFlattenList(unittest.TestCase):
    # NOTE: These are plain unit tests of the flatten_list() helper, not CLI
    # regression tests, even though they live in this folder.
    def test_flatten_list_with_array_column(self):
        """Verify flatten_list() flattens a DataFrame column of list values
        into a single flat list of scalars."""
        data = pd.DataFrame({"ARRAY_COLUMN": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]})
        result = list(flatten_list(data, ["ARRAY_COLUMN"]))
        expected = [1, 2, 3]
        self.assertEqual(result, expected)

    def test_flatten_list_with_nonexistent_column(self):
        """Verify flatten_list() falls back to yielding the column name
        itself when the requested column is not present in the DataFrame."""
        data = pd.DataFrame({"COLUMN_A": [1, 2, 3], "COLUMN_B": ["A", "B", "C"]})
        result = list(flatten_list(data, ["COLUMN_C"]))
        expected = ["COLUMN_C"]
        self.assertEqual(result, expected)

    def test_flatten_list_with_mixed_columns(self):
        """Verify flatten_list() handles a mix of array-valued and scalar
        columns, flattening the array column while passing through the
        scalar column's name unchanged."""
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
        """Verify flatten_list() degrades gracefully on an empty DataFrame,
        yielding the requested column name instead of raising an error."""
        empty_data = pd.DataFrame()
        result = list(flatten_list(empty_data, ["COLUMN"]))
        expected = ["COLUMN"]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
