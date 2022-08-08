import itertools
import logging
import os
import pickle
import time
from functools import partial
from collections import namedtuple
from multiprocessing import Pool
from multiprocessing.managers import SyncManager

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True

from engine.config import config
from engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from engine.models.dictionaries import DictionaryTypes, TermsFactoryInterface
from engine.models.dictionaries.meddra import MedDRATermsFactory
from engine.models.dictionaries.whodrug import WhoDrugTermsFactory
from engine.models.rule_conditions import ConditionCompositeFactory
from engine.models.rule_validation_result import RuleValidationResult
from engine.rules_engine import RulesEngine
from engine.services import logger as engine_logger
from engine.services.cache.cache_service_interface import CacheServiceInterface
from engine.services.cache.in_memory_cache_service import InMemoryCacheService
from engine.services.cache.redis_cache_service import RedisCacheService
from engine.services.data_services import BaseDataService, DataServiceFactory
from engine.utilities.excel_report import ExcelReport
from engine.utilities.excel_writer import excel_workbook_to_stream
from engine.utilities.utils import get_rules_cache_key

"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def validate_single_rule(cache, path, datasets, args, rule: dict = None):
    set_log_level(args.log_level)
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
    # call rule engine
    engine = RulesEngine(
        cache=cache,
        standard=args.standard,
        standard_version=args.version.replace(".", "-"),
        ct_package=args.controlled_terminology_package,
        meddra_path=args.meddra,
        whodrug_path=args.whodrug,
    )
    results = [
        engine.validate_single_rule(
            rule, f"{path}/{dataset['filename']}", datasets, dataset["domain"]
        )
        for dataset in datasets
    ]
    results = list(itertools.chain(*results))
    return RuleValidationResult(rule, results)


def fill_cache_with_provided_data(cache, cache_path: str):
    cache_files = next(os.walk(cache_path), (None, None, []))[2]
    for file_name in cache_files:
        with open(f"{cache_path}/{file_name}", "rb") as f:
            data = pickle.load(f)
            cache.add_all(data)
    return cache


def fill_cache_with_dictionaries(cache: CacheServiceInterface, args):
    """
    Extracts file contents from provided dictionaries files
    and saves to cache (inmemory or redis).
    """
    data_service = DataServiceFactory(config, cache).get_data_service()
    dictionary_type_to_factory_map: dict = {
        DictionaryTypes.MEDDRA.value: MedDRATermsFactory,
        DictionaryTypes.WHODRUG.value: WhoDrugTermsFactory,
    }

    dictionary_type_to_path_map: dict = {
        DictionaryTypes.MEDDRA.value: args.meddra,
        DictionaryTypes.WHODRUG.value: args.whodrug,
    }

    for dictionary_type, dictionary_path in dictionary_type_to_path_map.items():
        if dictionary_path:
            factory: TermsFactoryInterface = dictionary_type_to_factory_map[
                dictionary_type
            ](data_service)
            terms: dict = factory.install_terms(dictionary_path)
            cache.add(dictionary_path, terms)


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


def set_log_level(level: str):
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


def run_validation(args: namedtuple):
    logger = logging.getLogger("validator")
    set_log_level(args.log_level.lower())
    cache_path: str = f"{os.path.dirname(__file__)}/../{args.cache}"
    data_path: str = f"{os.path.dirname(__file__)}/../{args.data}"

    # fill cache
    CacheManager.register("RedisCacheService", RedisCacheService)
    CacheManager.register("InMemoryCacheService", InMemoryCacheService)
    manager = CacheManager()
    manager.start()
    shared_cache = get_cache_service(manager)
    shared_cache = fill_cache_with_provided_data(shared_cache, cache_path)

    # install dictionaries if needed
    fill_cache_with_dictionaries(shared_cache, args)

    rules = shared_cache.get_all_by_prefix(
        get_rules_cache_key(args.standard, args.version.replace(".", "-"))
    )
    data_service = DataServiceFactory(config, shared_cache).get_data_service()
    datasets = get_datasets(data_service, data_path)

    start = time.time()
    pool = Pool(args.pool_size)
    results = pool.map(
        partial(validate_single_rule, shared_cache, data_path, datasets, args),
        rules,
    )
    pool.close()
    pool.join()
    end = time.time()
    elapsed_time = end - start
    report_template = data_service.read_data(args.report_template, "rb")
    try:
        report = ExcelReport(data_path, results, elapsed_time, report_template.read())
        report_data = report.get_excel_export(
            args.define_version,
            args.controlled_terminology_package,
            args.standard,
            args.version.replace("-", "."),
        )
        with open(args.output, "wb") as f:
            f.write(excel_workbook_to_stream(report_data))
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        report_template.close()
