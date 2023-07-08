import unittest
import pandas as pd
from business_rules.operators import DataframeType

"""This automated test only validates the acceptance criteria for core issue 165
for complete test cases coverage, refer to unit tests"""


class TestCoreIssue165(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame.from_dict(
            {
                "times_without_colons": [
                    "2022-03-11T09-20-30",
                    "2022-03-11T092030",
                    "2022-03-11T09,20,30",
                    "2022-03-11T09@20@30",
                    "2022-03-11T09!20:30",
                    "2022-03-11T09:20:30",
                    "2022-03-11T09:20:30",
                ],
                "partial_dates": ["2022", "2022-03", "2022-03-11T09", "", "", "", ""],
                "partial_datetimes": [
                    "2022-03-11T09",
                    "2022-03-11T09:20",
                    "2022:03:11T09-20",
                    "2022-03:11T09:20",
                    "",
                    "",
                    "",
                ],
                "invalid_date": [
                    "20220311",
                    "2022-03-11T",
                    "2022-03-11T9:20:30",
                    "2022-03-11T09:20:30Z",
                    "",
                    "",
                    "",
                ],
            }
        )

    def test_invalid_date_times_without_colons(self):
        expected_result = pd.Series([True, True, True, True, True, False, False])
        result = DataframeType({"value": self.df}).invalid_date(
            {"target": "times_without_colons"}
        )
        self.assertTrue(result.equals(expected_result))

    def test_valid_partial_dates(self):
        expected_result = pd.Series([False, False, False, True, True, True, True])
        result = DataframeType({"value": self.df}).invalid_date(
            {"target": "partial_dates"}
        )
        self.assertTrue(result.equals(expected_result))

    def test_valid_partial_datetimes(self):
        expected_result = pd.Series([False, False, True, True, True, True, True])
        result = DataframeType({"value": self.df}).invalid_date(
            {"target": "partial_datetimes"}
        )
        self.assertTrue(result.equals(expected_result))

    def test_invalid_iso8601_date(self):
        df = self.df
        result = DataframeType({"value": df}).invalid_date({"target": "invalid_date"})
        expected_result = pd.Series([True, True, True, False, True, True, True])
        self.assertTrue(result.equals(expected_result))


if __name__ == "__main__":
    unittest.main()
