import logging
from multiprocessing.managers import SyncManager
from engine.services.cache.in_memory_cache_service import InMemoryCacheService

from engine.services.cache.redis_cache_service import RedisCacheService
logging.getLogger("asyncio").disabled = True
logging.getLogger('xmlschema').disabled = True
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
from engine.services.cache.cache_service_factory import CacheServiceFactory
from engine.services.data_service_factory import DataServiceFactory
from engine.services.base_data_service import BaseDataService
from engine.config import config
from engine.models.rule_conditions import ConditionCompositeFactory


class CacheManager(SyncManager): pass

def validate_single_rule(cache, path, datasets, rule, log_level="disabled"):
    # call rule engine
    set_log_level(log_level)
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
    engine = RulesEngine(cache=cache)
    results = [
        engine.validate_single_rule(rule, f"{path}/{dataset['filename']}", datasets, dataset["domain"]) for dataset in datasets
    ]
    # show_elapsed_time(rule, end-start)
    results = list(itertools.chain(*results))
    return results

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ca", "--cache", help="Cache location", default="resources/cache")
    parser.add_argument("-p", "--pool_size", help="Number of parallel processes for validation", type=int, default=10)
    parser.add_argument("-d", "--data", help="Data location", required=True)
    parser.add_argument("-l", "--log_level", help="Log level", default="disabled")
    args = parser.parse_args()
    return args

def fill_cache(cache, cache_path: str):
    cache_files = next(os.walk(cache_path), (None, None, []))[2]
    for file_name in cache_files:
        with open(f"{cache_path}/{file_name}", "rb") as f:
            data = pickle.load(f)
            cache.add_all(data)
    return cache

def get_datasets(data_service: BaseDataService, data_path: str):
    data_files = [f for f in next(os.walk(data_path), (None, None, []))[2] if f != DEFINE_XML_FILE_NAME]
    datasets = []
    for data_file in data_files:
        dataset_name = f"{data_path}/{data_file}"
        metadata = data_service.get_dataset_metadata(dataset_name=dataset_name)
        datasets.append({
            "domain": metadata["dataset_name"].iloc[0],
            "filename": metadata["dataset_location"].iloc[0]
        })

    return datasets

def set_log_level(level):
    levels = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    if level == "disabled":
        engine_logger.disabled = True
    else:
        engine_logger.setLevel(levels.get(level, logging.ERROR))

def get_cache_service(manager):
    cache_service_type = config.getValue("CACHE_TYPE")
    if cache_service_type == "redis":
        return manager.RedisCacheService(config.getValue("REDIS_HOST_NAME"), config.getValue("REDIS_ACCESS_KEY"))
    else:
        return manager.InMemoryCacheService()

def main():
    args = parse_arguments()
    set_log_level(args.log_level.lower())
    cache_path: str = (
        f"{os.path.dirname(__file__)}/{args.cache}"
    )
    data_path: str = (
        f"{os.path.dirname(__file__)}/{args.data}"
    )

    pool = Pool(args.pool_size)

    CacheManager.register("RedisCacheService", RedisCacheService)
    CacheManager.register("InMemoryCacheService", InMemoryCacheService)
    manager = CacheManager()
    manager.start()
    shared_cache = get_cache_service(manager)
    shared_cache = fill_cache(shared_cache, cache_path)
    rules = shared_cache.get_all_by_prefix("rules/")
    data_service = DataServiceFactory(config, shared_cache).get_data_service()
    datasets = get_datasets(data_service, data_path)
    start = time.time()
    #TODO: Convert results to excel output
    results = pool.map(partial(validate_single_rule, shared_cache, data_path, datasets, log_level=args.log_level), rules)
    pool.close()
    pool.join()
    end = time.time()
    print(f"Total time = {end-start} seconds")

if __name__ == "__main__":
    main()