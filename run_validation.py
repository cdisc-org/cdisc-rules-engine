import logging
logging.getLogger("asyncio").disabled = True
logging.getLogger('xmlschema').disabled = True
from engine.rules_engine import RulesEngine
from engine.services import logger as engine_logger
import logging
from engine.services.cache.cache_service_factory import CacheServiceFactory
from engine.services.data_service_factory import DataServiceFactory
from engine.services.base_data_service import BaseDataService
from engine.config import config
import asyncio
from engine.models.rule_conditions import ConditionCompositeFactory
import itertools
from concurrent.futures import ThreadPoolExecutor
import os
import time
import argparse
import pickle

async def validate(rule, dataset_path, dataset_list, domain):
    engine = RulesEngine()
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, engine.validate_single_rule, rule, dataset_path, dataset_list, domain)
    return data

async def validate_single_rule(rule, path, datasets):
    # call rule engine
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
    coroutines = [
        validate(rule, f"{path}/{dataset['filename']}", datasets, dataset["domain"]) for dataset in datasets
    ]
    results = await asyncio.gather(*coroutines)
    # show_elapsed_time(rule, end-start)
    results = list(itertools.chain(*results))

def run_rule(rule, dataset_path, datasets):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    data = loop.run_until_complete(validate_single_rule(rule, dataset_path, datasets))
    loop.close()
    return data

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ca", "--cache", help="Cache location", default="resources/cache")
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
    data_files = [f for f in next(os.walk(data_path), (None, None, []))[2] if f != "define.xml"]
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

def main():
    args = parse_arguments()
    set_log_level(args.log_level.lower())
    cache_path: str = (
        f"{os.path.dirname(__file__)}/{args.cache}"
    )
    data_path: str = (
        f"{os.path.dirname(__file__)}/{args.data}"
    )
    cache_service_obj = CacheServiceFactory(config).get_cache_service()
    cache_service_obj = fill_cache(cache_service_obj, cache_path)
    rules = cache_service_obj.get_all_by_prefix("rules/")
    data_service = DataServiceFactory(config, cache_service_obj).get_data_service()
    datasets = get_datasets(data_service, data_path)
    executor = ThreadPoolExecutor(10)
    jobs = []
    start = time.time()
    for rule in rules:
        future = executor.submit(run_rule, rule, data_path, datasets)
        jobs.append(future)
    executor.shutdown(wait=True)
    results = [f.result() for f in jobs]
    end = time.time()
    print(f"Total time = {end-start} seconds")

if __name__ == "__main__":
    main()