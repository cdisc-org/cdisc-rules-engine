import azure.functions as func
from cdisc_rule_tester.models.rule_tester import RuleTester
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.services.cache.cache_populator_service import CachePopulator
import json
import os
import asyncio


def validate_datasets_payload(datasets):
    required_keys = {"filename", "label", "domain", "records", "variables"}
    missing_keys = set()
    for dataset in datasets:
        for key in required_keys:
            if key not in dataset:
                missing_keys.add(key)

    if missing_keys:
        raise KeyError(
            f"one or more datasets missing the following keys {missing_keys}"
        )


def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    try:
        json_data = req.get_json()
        api_key = os.environ.get("LIBRARY_API_KEY")
        rule = json_data.get("rule")
        standards_data = json_data.get("standard", {})
        standard = standards_data.get("product")
        standard_version = standards_data.get("version")
        codelists = json_data.get("codelists", [])
        cache = InMemoryCacheService()
        if standards_data or codelists:
            library_service = CDISCLibraryService(api_key, cache)
            cache_populator: CachePopulator = CachePopulator(cache, library_service)
            if standards_data:
                asyncio.run(cache_populator.load_standard(standard, standard_version))
                asyncio.run(cache_populator.load_available_ct_packages())
            asyncio.run(cache_populator.load_codelists(codelists))
        if not rule:
            raise KeyError("'rule' required in request")
        datasets = json_data.get("datasets")
        if not datasets:
            raise KeyError("'datasets' required in request")
        validate_datasets_payload(datasets)
        define_xml = json_data.get("define_xml")
        tester = RuleTester(datasets, define_xml, cache, standard, standard_version)
        return func.HttpResponse(json.dumps(tester.validate(rule)))
    except KeyError as e:
        return func.HttpResponse(
            json.dumps({"error": "KeyError", "message": str(e)}), status_code=400
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps(
                {
                    "errror": "Unknown Exception",
                    "message": f"An unhandled exception occurred. {str(e)}",
                }
            ),
            status_code=500,
        )
