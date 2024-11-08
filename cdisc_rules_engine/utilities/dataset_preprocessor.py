from typing import List, Union

from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface

from cdisc_rules_engine.enums.join_types import JoinTypes
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
    get_sided_match_keys,
    get_dataset_name_from_details,
)
import os


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
        dataset: DatasetInterface,
        dataset_domain: str,
        dataset_path: str,
        data_service: DataServiceInterface,
        cache_service: CacheServiceInterface,
    ):
        self._dataset: DatasetInterface = dataset
        self._dataset_domain: str = dataset_domain
        self._dataset_path: str = dataset_path
        self._data_service = data_service
        self._rule_processor = RuleProcessor(self._data_service, cache_service)

    def preprocess(self, rule: dict, datasets: List[dict]) -> DatasetInterface:
        """
        Preprocesses the dataset by merging it with the
        datasets from the provided rule.
        """
        rule_datasets: List[dict] = rule.get("datasets")
        if not rule_datasets:
            return self._dataset  # nothing to preprocess

        rule_targets = self._rule_processor.extract_referenced_variables_from_rule(rule)
        # Get all targets that reference the merge domain.
        result: DatasetInterface = self._dataset.copy()
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
            filename = get_dataset_name_from_details(file_info)
            other_dataset: DatasetInterface = self._download_dataset(filename)
            referenced_targets = set(
                [
                    target.replace(f"{domain_name}.", "")
                    for target in rule_targets
                    if isinstance(target, str) and target.startswith(f"{domain_name}.")
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
                datasets=datasets,
            )
        logger.info(f"Dataset after preprocessing = {result}")
        return result

    def _is_split_domain(self, domain: str) -> bool:
        return domain == self._dataset_domain

    def _download_dataset(self, filename: str) -> DatasetInterface:
        return self._data_service.get_dataset(
            dataset_name=os.path.join(os.path.dirname(self._dataset_path), filename)
        )

    def _merge_datasets(
        self,
        left_dataset: DatasetInterface,
        left_dataset_domain_name: str,
        right_dataset: DatasetInterface,
        right_dataset_domain_details: dict,
        datasets: List[dict],
    ) -> DatasetInterface:
        """
        Merges datasets on their match keys.
        Identifies dataset type and merges based on it.
        """
        # replace -- pattern in match keys for each domain
        match_keys: List[Union[str, dict]] = right_dataset_domain_details.get(
            "match_key"
        )
        left_dataset_match_keys = replace_pattern_in_list_of_strings(
            get_sided_match_keys(match_keys=match_keys, side="left"),
            "--",
            left_dataset_domain_name,
        )
        right_dataset_domain_name: str = right_dataset_domain_details.get("domain_name")
        right_dataset_match_keys = replace_pattern_in_list_of_strings(
            get_sided_match_keys(match_keys=match_keys, side="right"),
            "--",
            right_dataset_domain_name,
        )

        # merge datasets based on their type
        if right_dataset_domain_name == "RELREC":
            result: DatasetInterface = DataProcessor.merge_relrec_datasets(
                left_dataset=left_dataset,
                left_dataset_domain_name=left_dataset_domain_name,
                relrec_dataset=right_dataset,
                datasets=datasets,
                dataset_preprocessor=self,
                wildcard=right_dataset_domain_details.get("wildcard"),
            )
        elif self._rule_processor.is_relationship_dataset(right_dataset_domain_name):
            result: DatasetInterface = DataProcessor.merge_relationship_datasets(
                left_dataset=left_dataset,
                left_dataset_match_keys=left_dataset_match_keys,
                right_dataset=right_dataset,
                right_dataset_match_keys=right_dataset_match_keys,
                right_dataset_domain=right_dataset_domain_details,
            )
        else:
            result: DatasetInterface = DataProcessor.merge_sdtm_datasets(
                left_dataset=left_dataset,
                right_dataset=right_dataset,
                left_dataset_match_keys=left_dataset_match_keys,
                right_dataset_match_keys=right_dataset_match_keys,
                right_dataset_domain_name=right_dataset_domain_name,
                join_type=JoinTypes(
                    right_dataset_domain_details.get("join_type", "inner")
                ),
            )
        return result
