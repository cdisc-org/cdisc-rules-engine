from typing import List

import pandas as pd

from cdisc_rules_engine.services import logger
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.utilities.utils import (
    replace_pattern_in_list_of_strings,
    search_in_list_of_dicts,
)


class DatasetPreprocessor:
    """
    The class is responsible for preprocessing the dataset
    before starting the validation.

    Preprocessing means merging the dataset with other datasets
    provided in the rule. Depending on the dataset type the merging
    process is different:
        Usual SDTM datasets are merged on match keys provided in the rule.
        Relationship datasets are merged on the columns that describe their relation.

    preprocess method is an entrypoint for the class clients.
    """

    def __init__(
        self,
        dataset: pd.DataFrame,
        dataset_domain: str,
        dataset_path: str,
        data_service: DataServiceInterface,
        cache_service: CacheServiceInterface,
    ):
        self._dataset: pd.DataFrame = dataset
        self._dataset_domain: str = dataset_domain
        self._dataset_path: str = dataset_path
        self._data_service = data_service
        self._rule_processor = RuleProcessor(self._data_service, cache_service)

    def preprocess(self, rule: dict, datasets: List[dict]) -> pd.DataFrame:
        """
        Preprocesses the dataset by merging it with the
        datasets from the provided rule.
        """
        rule_datasets: List[dict] = rule.get("datasets")
        if not rule_datasets:
            return self._dataset  # nothing to preprocess

        rule_targets = self._rule_processor.extract_referenced_variables_from_rule(rule)
        # Get all targets that reference the merge domain.
        result: pd.DataFrame = self._dataset.copy()
        for domain_details in rule_datasets:
            domain_name: str = domain_details.get("domain_name")
            if self._is_split_domain(domain_name):
                continue

            # download other dataset from blob storage and merge
            file_info: dict = search_in_list_of_dicts(
                datasets, lambda item: item.get("domain") == domain_name
            )
            if not file_info:
                continue
            other_dataset: pd.DataFrame = self._download_dataset(file_info["filename"])
            referenced_targets = set(
                [
                    target.replace(f"{domain_name}.", "")
                    for target in rule_targets
                    if target.startswith(f"{domain_name}.")
                ]
            )
            other_dataset.columns = [
                f"{domain_name}.{column}" if column in referenced_targets else column
                for column in other_dataset.columns
            ]
            result = self._merge_datasets(
                left_dataset=result,
                left_dataset_domain_name=self._dataset_domain,
                right_dataset=other_dataset,
                right_dataset_domain_details=domain_details,
            )

        logger.info(f"Dataset after preprocessing = {result}")
        return result

    def _is_split_domain(self, domain: str) -> bool:
        return domain == self._dataset_domain

    def _download_dataset(self, filename: str) -> pd.DataFrame:
        return self._data_service.get_dataset(
            dataset_name=f'{self._dataset_path.rsplit("/", 1)[0]}/{filename}'
        )

    def _merge_datasets(
        self,
        left_dataset: pd.DataFrame,
        left_dataset_domain_name: str,
        right_dataset: pd.DataFrame,
        right_dataset_domain_details: dict,
    ) -> pd.DataFrame:
        """
        Merges datasets on their match keys.
        Identifies dataset type and merges based on it.
        """
        # replace -- pattern in match keys for each domain
        match_keys: List[str] = right_dataset_domain_details.get("match_key")
        left_dataset_match_keys = replace_pattern_in_list_of_strings(
            match_keys, "--", left_dataset_domain_name
        )
        right_dataset_domain_name: str = right_dataset_domain_details.get("domain_name")
        right_dataset_match_keys = replace_pattern_in_list_of_strings(
            match_keys, "--", right_dataset_domain_name
        )

        # merge datasets based on their type
        if self._rule_processor.is_relationship_dataset(right_dataset_domain_name):
            result: pd.DataFrame = DataProcessor.merge_relationship_datasets(
                left_dataset=left_dataset,
                left_dataset_match_keys=left_dataset_match_keys,
                right_dataset=right_dataset,
                right_dataset_match_keys=right_dataset_match_keys,
                right_dataset_domain=right_dataset_domain_details,
            )
        else:
            result: pd.DataFrame = DataProcessor.merge_sdtm_datasets(
                left_dataset=left_dataset,
                right_dataset=right_dataset,
                left_dataset_match_keys=left_dataset_match_keys,
                right_dataset_match_keys=right_dataset_match_keys,
                right_dataset_domain_name=right_dataset_domain_name,
            )
        return result
