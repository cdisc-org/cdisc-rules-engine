from abc import abstractmethod
from functools import wraps
from typing import Callable, List, TextIO

import numpy as np
import pandas as pd

from cdisc_rules_engine.constants.domains import AP_DOMAIN_LENGTH
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.utils import (
    get_dataset_cache_key_from_path,
    get_directory_path,
    search_in_list_of_dicts,
)


def cached_dataset(dataset_type: str):
    """
    Decorator that can be applied to get_dataset_... functions
    to support caching service.
    Before calling the wrapped function it checks
    if needed dataset exists in cache.
    Bear in mind that wrapped functions have to be
    called with kwargs in order to support cache key template.
    """
    if not DatasetTypes.contains(dataset_type):
        raise ValueError(f"Invalid dataset type: {dataset_type}")

    def decorator(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            instance: BaseDataService = args[0]
            dataset_name: str = kwargs["dataset_name"]
            logger.info(
                f"Downloading dataset from storage. dataset_name={dataset_name}, wrapped function={func.__name__}"
            )
            cache_key: str = get_dataset_cache_key_from_path(dataset_name, dataset_type)
            cache_data = instance.cache_service.get(cache_key)
            if cache_data is not None:
                logger.info(
                    f'Dataset "{dataset_name}" was found in cache. cache_key={cache_key}'
                )
                dataset = cache_data
            else:
                dataset = func(*args, **kwargs)
                instance.cache_service.add(cache_key, dataset)
            logger.info(f"Downloaded dataset. dataset={dataset}")
            return dataset

        return inner

    return decorator


class BaseDataService:
    def __init__(self, **params):
        self.blob_service = None
        self.cache_service = None
        self.reader_factory = None

    @abstractmethod
    def get_dataset(self, dataset_name: str, **params) -> pd.DataFrame:
        """
        Gets dataset from blob storage.
        """

    @abstractmethod
    def get_dataset_metadata(self, dataset_name: str, **kwargs):
        """
        Gets dataset metadata from blob storage.
        """

    @abstractmethod
    def join_split_datasets(self, func_to_call: Callable, dataset_names, **kwargs):
        """
        Accepts a list of split dataset filenames,
        downloads all of them and merges into a single DataFrame.
        """

    @abstractmethod
    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> pd.DataFrame:
        """
        Generic function to return dataset based on the type.
        dataset_type param can be: contents, metadata, variables_metadata.
        """

    def get_dataset_class(self, dataset, file_path, datasets):
        if self._contains_topic_variable(dataset, "TERM"):
            return "Events"
        elif self._contains_topic_variable(dataset, "TRT"):
            return "Interventions"
        elif self._contains_topic_variable(dataset, "TESTCD"):
            return "Findings"
        elif self._is_associated_persons(dataset):
            return self._get_associated_persons_inherit_class(
                dataset, file_path, datasets
            )
        else:
            # Default case, unknown class
            return None

    def _is_associated_persons(self, dataset) -> bool:
        """
        Check if AP-- domain.
        """
        return (
            "DOMAIN" in dataset
            and self._domain_starts_with(dataset["DOMAIN"].values[0], "AP")
            and len(dataset["DOMAIN"].values[0]) == AP_DOMAIN_LENGTH
        )

    def _get_associated_persons_inherit_class(
        self, dataset, file_path, datasets: List[dict]
    ):
        """
        Check with inherit class AP-- belongs to.
        """
        domain = dataset["DOMAIN"].values[0]
        ap_suffix = domain[2:]
        directory_path = get_directory_path(file_path)
        if len(datasets) > 1:
            domain_details: dict = search_in_list_of_dicts(
                datasets, lambda item: item.get("domain") == ap_suffix
            )
            if domain_details:
                file_name = domain_details["filename"]
                new_file_path = f"{directory_path}/{file_name}"
                new_domain_dataset = self.get_dataset(dataset_name=new_file_path)
            else:
                raise ValueError(f"Filename for domain doesn't exist")
            if self._is_associated_persons(new_domain_dataset):
                raise ValueError(f"Nested Associated Persons domain reference")
            return self.get_dataset_class(new_domain_dataset, new_file_path, datasets)
        else:
            return None

    def _contains_topic_variable(self, dataset, variable):
        """
        Checks if the given dataset-class string ends with a particular variable string.
        Returns True/False
        """
        return dataset.columns.str.endswith(variable).any()

    def _domain_starts_with(self, domain, variable):
        """
        Checks if the given dataset-class string starts with a particular variable string.
        Returns True/False
        """
        return domain.startswith(variable)

    @staticmethod
    def _replace_nans_in_numeric_cols_with_none(dataset: pd.DataFrame):
        """
        Replaces NaN in numeric columns with None.
        """
        numeric_columns = dataset.select_dtypes(include=np.number).columns
        dataset[numeric_columns] = dataset[numeric_columns].apply(
            lambda x: x.replace({np.nan: None})
        )

    @abstractmethod
    def read_data(self, file_path: str, read_mode: str = "r") -> TextIO:
        """
        Reads data from the given path and returns TextIO instance.
        """
