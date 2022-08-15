from asyncore import write
import itertools
import logging
import os
import pickle
import time
from functools import partial
from multiprocessing import Pool
from multiprocessing.managers import SyncManager
from cdisc_rules_engine.config import config
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.models.dictionaries import (
    DictionaryTypes,
    TermsFactoryInterface,
)
from cdisc_rules_engine.models.dictionaries.meddra import MedDRATermsFactory
from cdisc_rules_engine.models.dictionaries.whodrug import WhoDrugTermsFactory
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.rules_engine import RulesEngine
from cdisc_rules_engine.services import logger as engine_logger
from cdisc_rules_engine.services.cache import (
    CacheServiceInterface,
    InMemoryCacheService,
    RedisCacheService,
)
from cdisc_rules_engine.services.data_services import (
    BaseDataService,
    DataServiceFactory,
)
from cdisc_rules_engine.utilities.utils import get_rules_cache_key
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.utilities.report_factory import ReportFactory

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True

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


def run_validation(args: Validation_args):
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

    reporting_service = ReportFactory(
        data_path, results, elapsed_time, args, data_service
    ).get_report_service()
    reporting_service.write_report()
