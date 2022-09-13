import asyncio
from abc import ABC
from functools import wraps, partial
from typing import Callable, List, Optional, Iterable

import numpy as np
import pandas as pd

from cdisc_rules_engine.constants.domains import AP_DOMAIN_LENGTH
from cdisc_rules_engine.interfaces import DataServiceInterface, CacheServiceInterface
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.data_readers import DataReaderFactory
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
                f"Downloading dataset from storage. dataset_name={dataset_name},"
                f" wrapped function={func.__name__}"
            )
            cache_key: str = get_dataset_cache_key_from_path(dataset_name, dataset_type)
            cache_data = instance.cache_service.get(cache_key)
            if cache_data is not None:
                logger.info(
                    f'Dataset "{dataset_name}" was found in cache.'
                    f" cache_key={cache_key}"
                )
                dataset = cache_data
            else:
                dataset = func(*args, **kwargs)
                instance.cache_service.add(cache_key, dataset)
            logger.info(f"Downloaded dataset. dataset={dataset}")
            return dataset

        return inner

    return decorator


class BaseDataService(DataServiceInterface, ABC):
    """
    An abstract base class for all data services that implement
    data service interface. The class implements some methods
    whose implementation stays the same regardless of the service.
    It is not necessary to inherit from this class.
    """

    def __init__(
        self, cache_service: CacheServiceInterface, reader_factory: DataReaderFactory
    ):
        self.cache_service = cache_service
        self.reader_factory = reader_factory

    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> pd.DataFrame:
        """
        Generic function to return dataset based on the type.
        dataset_type param can be: contents, metadata, variables_metadata.
        """
        dataset_type_to_function_map: dict = {
            DatasetTypes.CONTENTS.value: self.get_dataset,
            DatasetTypes.METADATA.value: self.get_dataset_metadata,
            DatasetTypes.VARIABLES_METADATA.value: self.get_variables_metadata,
        }
        return dataset_type_to_function_map[dataset_type](
            dataset_name=dataset_name, **params
        )

    def join_split_datasets(
        self, func_to_call: Callable, dataset_names: List[str], **kwargs
    ) -> pd.DataFrame:
        """
        Accepts a list of split dataset filenames, asynchronously downloads
        all of them and merges into a single DataFrame.

        function_to_call must accept dataset_name and kwargs
        as input parameters and return pandas DataFrame.
        """
        # pop drop_duplicates param at the beginning to avoid passing it to func_to_call
        drop_duplicates: bool = kwargs.pop("drop_duplicates", False)

        # download datasets asynchronously
        coroutines = [
            self._async_get_dataset(func_to_call, name, **kwargs)
            for name in dataset_names
        ]
        loop = asyncio.get_event_loop()
        datasets: Iterable[pd.DataFrame] = loop.run_until_complete(
            asyncio.gather(*coroutines)
        )

        # join datasets
        joined_dataset: pd.DataFrame = pd.concat(
            [dataset for dataset in datasets],
            ignore_index=True,
        )
        if drop_duplicates:
            joined_dataset.drop_duplicates()
        return joined_dataset

    def get_dataset_class(
        self, dataset: pd.DataFrame, file_path: str, datasets: List[dict]
    ) -> Optional[str]:
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
                raise ValueError("Filename for domain doesn't exist")
            if self._is_associated_persons(new_domain_dataset):
                raise ValueError("Nested Associated Persons domain reference")
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
        Checks if the given dataset-class string starts with
         a particular variable string.
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

    def _async_get_dataset(
        self, function_to_call: Callable, dataset_name: str, **kwargs
    ) -> pd.DataFrame:
        """
        Asynchronously executes passed function_to_call.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, partial(function_to_call, dataset_name=dataset_name, **kwargs)
        )
