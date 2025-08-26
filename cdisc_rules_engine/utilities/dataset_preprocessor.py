from typing import Iterable, List, Union

from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

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
    get_sided_match_keys,
    get_dataset_name_from_details,
)
import os
import pandas as pd


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
        dataset_metadata: SDTMDatasetMetadata,
        data_service: DataServiceInterface,
        cache_service: CacheServiceInterface,
    ):
        self._dataset: DatasetInterface = dataset
        self._dataset_metadata: SDTMDatasetMetadata = dataset_metadata
        self._data_service = data_service
        self._rule_processor = RuleProcessor(self._data_service, cache_service)

    def preprocess(  # noqa
        self, rule: dict, datasets: Iterable[SDTMDatasetMetadata]
    ) -> DatasetInterface:
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
        merged_domains = set()
        for domain_details in rule_datasets:
            domain_name: str = domain_details.get("domain_name")
            is_child = bool(domain_details.get("child"))
            # download other datasets from blob storage and merge
            if is_child:
                file_infos = []
                # find parent of SUPP or SQAP dataset
                if (
                    (domain_name[:4] == "SUPP" or domain_name[:4] == "SQAP")
                    and self._dataset_metadata.is_supp
                    and self._dataset_metadata.rdomain
                ):
                    if (
                        domain_name == "SUPP--"
                        or domain_name == self._dataset_metadata.name
                    ):
                        file_infos: list[SDTMDatasetMetadata] = [
                            item
                            for item in datasets
                            if (item.domain == self._dataset_metadata.rdomain)
                        ]
                # find parent of other datasets
                elif (
                    domain_name == self._dataset_metadata.domain
                    or domain_name == self._dataset_metadata.name
                ):
                    file_infos: list[SDTMDatasetMetadata] = self._find_parent_dataset(
                        datasets, domain_details
                    )
            else:
                if self._is_split_domain(domain_name):
                    continue
                file_infos: list[SDTMDatasetMetadata] = [
                    item
                    for item in datasets
                    if (
                        item.domain == domain_name
                        or item.name == domain_name
                        or item.unsplit_name == domain_name
                        or (
                            domain_name == "SUPP--"
                            and (not self._dataset_metadata.is_supp)
                            and item.rdomain == self._dataset_metadata.domain
                        )
                    )
                ]
            for file_info in file_infos:
                if file_info.domain in merged_domains:
                    continue
                filename = get_dataset_name_from_details(file_info)
                other_dataset: DatasetInterface = self._download_dataset(filename)
                referenced_targets = set(
                    [
                        target.replace(f"{domain_name}.", "")
                        for target in rule_targets
                        if isinstance(target, str)
                        and target.startswith(f"{domain_name}.")
                    ]
                )
                other_dataset.columns = [
                    (
                        f"{domain_name}.{column}"
                        if column in referenced_targets
                        else column
                    )
                    for column in other_dataset.columns
                ]
                if is_child:
                    result = self._child_merge_datasets(
                        left_dataset=result,
                        left_dataset_domain_name=self._dataset_metadata.domain,
                        right_dataset=other_dataset,
                        right_dataset_domain_name=file_info.domain,
                        right_dataset_metadata=file_info,
                        match_keys=domain_details.get("match_key"),
                        datasets=datasets,
                    )
                    merged_domains.add(file_info.domain)
                else:
                    result = self._merge_datasets(
                        left_dataset=result,
                        left_dataset_domain_name=self._dataset_metadata.domain,
                        right_dataset=other_dataset,
                        right_dataset_domain_details=domain_details,
                        datasets=datasets,
                    )
        logger.info(f"Dataset after preprocessing = {result}")
        return result

    def _find_parent_dataset(
        self, datasets: Iterable[SDTMDatasetMetadata], domain_details: dict
    ) -> SDTMDatasetMetadata:
        matching_datasets = []
        if "RDOMAIN" in self._dataset.columns:
            rdomain_column = self._dataset.data["RDOMAIN"]
            unique_domains = set(rdomain_column.unique())
            for dataset in datasets:
                if dataset.domain in unique_domains:
                    matching_datasets.append(dataset)
        else:
            match_keys = domain_details.get("match_key")
            for dataset in datasets:
                has_all_match_keys = all(
                    match_key in dataset.first_record for match_key in match_keys
                )
                if has_all_match_keys:
                    matching_datasets.append(dataset)
        if not matching_datasets:
            logger.warning(
                f"Child specified in match but no parent datasets found for: {domain_details}"
            )
        return matching_datasets

    def _is_split_domain(self, domain: str) -> bool:
        return domain == self._dataset_metadata.unsplit_name

    def _download_dataset(self, filename: str) -> DatasetInterface:
        return self._data_service.get_dataset(
            dataset_name=os.path.join(
                os.path.dirname(self._dataset_metadata.full_path), filename
            )
        )

    def _child_merge_datasets(
        self,
        left_dataset: DatasetInterface,
        left_dataset_domain_name: str,
        right_dataset: DatasetInterface,
        right_dataset_domain_name: str,
        right_dataset_metadata: SDTMDatasetMetadata,
        match_keys: List[str],
        datasets: Iterable[SDTMDatasetMetadata] = None,
    ) -> DatasetInterface:
        is_supplemental, rdomain_dataset = self._classify_dataset(
            left_dataset, self._dataset_metadata
        )
        has_idvar_keys = "IDVAR" in match_keys or "IDVARVAL" in match_keys
        if is_supplemental or rdomain_dataset and has_idvar_keys:
            return self._merge_rdomain_dataset(
                left_dataset,
                left_dataset_domain_name,
                right_dataset,
                right_dataset_domain_name,
                self._dataset_metadata,
                match_keys,
                datasets,
            )
        else:
            left_dataset_match_keys = replace_pattern_in_list_of_strings(
                get_sided_match_keys(match_keys=match_keys, side="left"),
                "--",
                left_dataset_domain_name,
            )
            right_dataset_match_keys = replace_pattern_in_list_of_strings(
                get_sided_match_keys(match_keys=match_keys, side="right"),
                "--",
                right_dataset_domain_name,
            )
            result = left_dataset.merge(
                right_dataset.data,
                how="left",
                left_on=left_dataset_match_keys,
                right_on=right_dataset_match_keys,
                suffixes=("", f".{right_dataset_domain_name}"),
            )
            return result

    def _classify_dataset(
        self, dataset: DatasetInterface, metadata: SDTMDatasetMetadata
    ) -> tuple[bool, bool]:
        is_supplemental = metadata and metadata.is_supp
        has_rdomain_not_supp = "RDOMAIN" in dataset.columns and not is_supplemental

        return is_supplemental, has_rdomain_not_supp

    def _merge_rdomain_dataset(
        self,
        left_dataset: DatasetInterface,
        left_dataset_domain_name: str,
        right_dataset: DatasetInterface,
        right_dataset_domain_name: str,
        right_dataset_metadata: SDTMDatasetMetadata,
        match_keys: List[str],
        datasets: Iterable[SDTMDatasetMetadata],
    ) -> DatasetInterface:
        """Simplified IDVAR/IDVARVAL merge with RDOMAIN filtering."""
        logger.info(
            f"Starting RDOMAIN merge: child={left_dataset_domain_name}, parent={right_dataset_domain_name}"
        )

        # Filter child records for this specific parent
        relevant_child_records = self._get_relevant_child_records(
            left_dataset, right_dataset_domain_name
        )
        merged_records = self._merge_with_idvar(relevant_child_records, right_dataset)
        logger.info(f"After IDVAR merge: {len(merged_records)} merged records")
        return self._update_dataset(
            left_dataset, relevant_child_records, merged_records
        )

    def _get_relevant_child_records(
        self, left_dataset: DatasetInterface, parent_domain: str
    ) -> DatasetInterface:
        """Get child records that should merge with this specific parent."""

        # Supplemental datasets: use metadata
        if self._dataset_metadata.is_supp and self._dataset_metadata.rdomain:
            if self._dataset_metadata.rdomain == parent_domain:
                return left_dataset  # All records should merge with this parent
            else:
                return left_dataset.iloc[
                    0:0
                ]  # Empty dataset - no records for this parent

        # Multi-domain datasets: use RDOMAIN column
        if "RDOMAIN" in left_dataset.columns:
            filtered = left_dataset[left_dataset["RDOMAIN"] == parent_domain]
            logger.info(
                f"Filtering {len(left_dataset)} records where RDOMAIN='{parent_domain}', found {len(filtered)} matches"
            )
            return filtered

        # Fallback: all records
        logger.warning(
            f"No RDOMAIN filtering available for parent '{parent_domain}', using all records"
        )
        return left_dataset

    def _merge_with_idvar(
        self, child_records: DatasetInterface, parent_dataset: DatasetInterface
    ) -> pd.DataFrame:
        """Merge using IDVAR/IDVARVAL columns."""
        results = []

        for _, child_row in child_records.iterrows():
            idvar = child_row.get("IDVAR")
            idvarval = child_row.get("IDVARVAL")

            # Find matching parent record
            if (
                pd.notna(idvar)
                and pd.notna(idvarval)
                and idvar in parent_dataset.columns
            ):
                # Try to match with type conversion
                try:
                    parent_col = parent_dataset[idvar]
                    if pd.api.types.is_numeric_dtype(parent_col.dtype):
                        idvarval = (
                            float(idvarval)
                            if "." in str(idvarval)
                            else int(float(str(idvarval)))
                        )

                    parent_match = parent_dataset[parent_col == idvarval]
                    if not parent_match.empty:
                        # Merge child + parent
                        merged = {
                            **child_row.to_dict(),
                            **parent_match.iloc[0].to_dict(),
                        }
                        results.append(merged)
                        continue
                except (ValueError, TypeError):
                    pass

            # No match found - child + None for parent columns
            merged = child_row.to_dict()
            for col in parent_dataset.columns:
                if col not in merged:
                    merged[col] = None
            results.append(merged)

        return pd.DataFrame(results)

    def _update_dataset(
        self,
        original: DatasetInterface,
        old_records: DatasetInterface,
        new_records: pd.DataFrame,
    ) -> DatasetInterface:
        """Replace old records with new merged records."""

        if old_records.empty:
            return original

        # For supplemental: replace entire dataset
        if self._dataset_metadata.is_supp:
            return self._dataset.__class__(data=new_records)

        # For multi-domain: replace specific records
        result = original.data.drop(old_records.index)
        result = pd.concat([result, new_records], ignore_index=True)
        return self._dataset.__class__(data=result)

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
        elif right_dataset_domain_name == "SUPP--":
            result: DatasetInterface = DataProcessor.merge_pivot_supp_dataset(
                dataset_implementation=self._data_service.dataset_implementation,
                left_dataset=left_dataset,
                right_dataset=right_dataset,
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
