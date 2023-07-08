import pandas
import unittest

from business_rules.operators import DataframeType


class NumericComparisonTests(unittest.TestCase):
    def setUp(self):
        self.df = pandas.DataFrame.from_dict(
            {
                "var1": [1, 2, 4],
                "var2": [3, 5, 6],
                "var3": [1, 3, 8],
                "var4": [1, 2, 4],
                "var5": ["1", "3", "5"],
                "var6": ["ad", "ab", "al"],
            }
        )
        self.dataframe_type = DataframeType({"value": self.df})

    def test_less_than_numeric_to_char(self):
        self.assertTrue(
            self.dataframe_type.less_than(
                {"target": "var1", "comparator": "var6"}
            ).equals(pandas.Series([False, False, False]))
        )

    def test_less_than_char_to_numeric(self):
        self.assertTrue(
            self.dataframe_type.less_than(
                {"target": "var6", "comparator": "var1"}
            ).equals(pandas.Series([False, False, False]))
        )

    def test_greater_than_numeric_to_char(self):
        self.assertTrue(
            self.dataframe_type.greater_than(
                {"target": "var1", "comparator": "var6"}
            ).equals(pandas.Series([False, False, False]))
        )

    def test_greater_than_char_to_numeric(self):
        self.assertTrue(
            self.dataframe_type.greater_than(
                {"target": "var6", "comparator": "var1"}
            ).equals(pandas.Series([False, False, False]))
        )

    def test_less_than_or_equal_to_numeric_to_char(self):
        self.assertTrue(
            self.dataframe_type.less_than_or_equal_to(
                {"target": "var1", "comparator": "var6"}
            ).equals(pandas.Series([False, False, False]))
        )

    def test_greater_than_or_equal_to_numeric_to_char(self):
        self.assertTrue(
            self.dataframe_type.greater_than_or_equal_to(
                {"target": "var1", "comparator": "var6"}
            ).equals(pandas.Series([False, False, False]))
        )

    def test_greater_than_or_equal_to_char_to_numeric(self):
        self.assertTrue(
            self.dataframe_type.greater_than_or_equal_to(
                {"target": "var6", "comparator": "var1"}
            ).equals(pandas.Series([False, False, False]))
        )


if __name__ == "__main__":
    unittest.main()
