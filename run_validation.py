import logging
from multiprocessing.managers import SyncManager

from engine.models.dictionaries import DictionaryTypes, TermsFactoryInterface
from engine.models.dictionaries.meddra import MedDRATermsFactory
from engine.models.dictionaries.whodrug import WhoDrugTermsFactory
from engine.services.cache.cache_service_interface import CacheServiceInterface
from engine.services.cache.in_memory_cache_service import InMemoryCacheService

from engine.services.cache.redis_cache_service import RedisCacheService

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True
import itertools
from multiprocessing import Pool
from functools import partial
import os
import time
import argparse
import pickle
from engine.rules_engine import RulesEngine
from engine.services import logger as engine_logger
from engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from engine.services.data_services import DataServiceFactory
from engine.services.data_services import BaseDataService
from engine.config import config
from engine.models.rule_conditions import ConditionCompositeFactory


"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def validate_single_rule(
    cache, path, datasets, rule, log_level="disabled", dictionaries_path: str = None
):
    # call rule engine
    set_log_level(log_level)
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
    engine = RulesEngine(cache=cache, dictionaries_path=dictionaries_path)
    results = [
        engine.validate_single_rule(
            rule, f"{path}/{dataset['filename']}", datasets, dataset["domain"]
        )
        for dataset in datasets
    ]
    # show_elapsed_time(rule, end-start)
    results = list(itertools.chain(*results))
    return results


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-ca", "--cache", help="Cache location", default="resources/cache"
    )
    parser.add_argument(
        "-p",
        "--pool_size",
        help="Number of parallel processes for validation",
        type=int,
        default=10,
    )
    parser.add_argument("-d", "--data", help="Data location", required=True)
    parser.add_argument("-l", "--log_level", help="Log level", default="disabled")
    parser.add_argument(
        "-dp", "--dictionaries_path", help="Path to directory with dictionaries files"
    )
    parser.add_argument(
        "-dt", "--dictionary_type", help="Dictionary type (MedDra, WhoDrug)"
    )
    args = parser.parse_args()
    return args


def fill_cache_with_provided_data(cache, cache_path: str):
    cache_files = next(os.walk(cache_path), (None, None, []))[2]
    for file_name in cache_files:
        with open(f"{cache_path}/{file_name}", "rb") as f:
            data = pickle.load(f)
            cache.add_all(data)
    return cache


def fill_cache_with_dictionaries(
    cache: CacheServiceInterface, dictionaries_path: str, dictionary_type: str
):
    """
    Extracts file contents from provided dictionaries files
    and saves to cache (inmemory or redis).
    """
    data_service = DataServiceFactory(config, cache).get_data_service()
    dictionary_type_to_factory_map: dict = {
        DictionaryTypes.MEDDRA.value: MedDRATermsFactory,
        DictionaryTypes.WHODRUG.value: WhoDrugTermsFactory,
    }
    factory: TermsFactoryInterface = dictionary_type_to_factory_map[dictionary_type](
        data_service
    )
    terms: dict = factory.install_terms(dictionaries_path)
    cache.add(dictionaries_path, terms)


def get_datasets(data_service: BaseDataService, data_path: str):
    data_files = [
        f
        for f in next(os.walk(data_path), (None, None, []))[2]
        if f != DEFINE_XML_FILE_NAME
    ]
    datasets = []
    for data_file in data_files:
        dataset_name = f"{data_path}/{data_file}"
        metadata = data_service.get_dataset_metadata(dataset_name=dataset_name)
        datasets.append(
            {
                "domain": metadata["dataset_name"].iloc[0],
                "filename": metadata["dataset_location"].iloc[0],
            }
        )

    return datasets


def set_log_level(level):
    levels = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    if level == "disabled":
        engine_logger.disabled = True
    else:
        engine_logger.setLevel(levels.get(level, logging.ERROR))


def get_cache_service(manager):
    cache_service_type = config.getValue("CACHE_TYPE")
    if cache_service_type == "redis":
        return manager.RedisCacheService(
            config.getValue("REDIS_HOST_NAME"), config.getValue("REDIS_ACCESS_KEY")
        )
    else:
        return manager.InMemoryCacheService()


def main():
    args = parse_arguments()
    set_log_level(args.log_level.lower())
    cache_path: str = f"{os.path.dirname(__file__)}/{args.cache}"
    data_path: str = f"{os.path.dirname(__file__)}/{args.data}"
    dictionaries_path: str = args.dictionaries_path
    dictionary_type: str = args.dictionary_type

    # fill cache
    CacheManager.register("RedisCacheService", RedisCacheService)
    CacheManager.register("InMemoryCacheService", InMemoryCacheService)
    manager = CacheManager()
    manager.start()
    shared_cache = get_cache_service(manager)
    shared_cache = fill_cache_with_provided_data(shared_cache, cache_path)

    # install dictionaries if needed
    if dictionaries_path:
        assert dictionary_type, "Obligatory parameter dictionary_type is not provided"
        fill_cache_with_dictionaries(shared_cache, dictionaries_path, dictionary_type)

    rules = shared_cache.get_all_by_prefix("rules/")
    data_service = DataServiceFactory(config, shared_cache).get_data_service()
    datasets = get_datasets(data_service, data_path)

    # TODO: Convert results to excel output
    start = time.time()
    pool = Pool(args.pool_size)
    results = pool.map(
        partial(
            validate_single_rule,
            shared_cache,
            data_path,
            datasets,
            log_level=args.log_level,
            dictionaries_path=dictionaries_path,
        ),
        rules,
    )
    pool.close()
    pool.join()
    end = time.time()
    print(f"Total time = {end-start} seconds")


if __name__ == "__main__":
    main()
