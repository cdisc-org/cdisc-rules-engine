import unittest
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)
from unittest.mock import MagicMock


class TestConcatSplitDatasets(unittest.TestCase):
    def setUp(self):

        self.cache_service = None
        self.reader_factory = None
        self.config = None
        self.data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())

    def test_concat_split_datasets(self):
        full_dataset = PandasDataset.from_dict(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "DOMAIN": ["A", "B", "C"],
            }
        )

        # Sample supplementary dataset
        supp_dataset = PandasDataset.from_dict(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "IDVAR": [1, 2, 3],
                "IDVARVAL": [1, 2, 3],
                "RDOMAIN": ["A", "B", "C"],
            }
        )

        expected_merged_df = PandasDataset.from_dict(
            {
                "STUDYID": [1, 2, 3],
                "USUBJID": [101, 102, 103],
                "APID": [201, 202, 203],
                "POOLID": [301, 302, 303],
                "SPDEVID": [401, 402, 403],
                "IDVAR": [1, 2, 3],
                "RDOMAIN": ["A", "B", "C"],
                "DOMAIN": ["A", "B", "C"],
                "IDVARVAL": [1, 2, 3],
            }
        )

        # Mock async_get_datasets function
        def mock_async_get_datasets(func_to_call, dataset_names, **kwargs):
            return [full_dataset, supp_dataset]

        # Set up mock
        self.data_service._async_get_datasets = MagicMock(
            side_effect=mock_async_get_datasets
        )

        # Call the function to test
        result = self.data_service.concat_split_datasets(
            MagicMock(), ["parent_dataset", "supp_dataset"]
        )

        result.equals(expected_merged_df)


if __name__ == "__main__":
    unittest.main()
