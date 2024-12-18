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


class BadRequestError(Exception):
    pass


def validate_datasets_payload(datasets):
    required_keys = {"filename", "label", "domain", "records", "variables"}
    missing_keys = set()
    for dataset in datasets:
        for key in required_keys:
            if key not in dataset:
                missing_keys.add(key)

        for var in dataset.get("variables", []):
            if var is None:
                raise BadRequestError(
                    f"Dataset: {dataset.get('label')} is missing variable metadata"
                )

    if missing_keys:
        raise KeyError(
            f"one or more datasets missing the following keys {missing_keys}"
        )


def handle_exception(e: Exception):
    if isinstance(e, KeyError):
        return func.HttpResponse(
            json.dumps({"error": "KeyError", "message": str(e)}), status_code=400
        )
    elif isinstance(e, BadRequestError):
        return func.HttpResponse(
            json.dumps({"error": "BadRequestError", "message": str(e)}), status_code=400
        )
    else:
        return func.HttpResponse(
            json.dumps(
                {
                    "errror": "Unknown Exception",
                    "message": f"An unhandled exception occurred. {str(e)}",
                }
            ),
            status_code=500,
        )


def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    try:
        json_data = req.get_json()
        api_key = os.environ.get("CDISC_LIBRARY_API_KEY")
        rule = json_data.get("rule")
        standards_data = json_data.get("standard", {})
        standard = standards_data.get("product")
        standard_version = standards_data.get("version")
        standard_substandard = None
        if standard and standard.lower() == "tig":
            standard_substandard = (
                rule.get("Authorities", [])[0]
                .get("Standards", [])[0]
                .get("Substandard")
            ).lower()
        codelists = json_data.get("codelists", [])
        cache = InMemoryCacheService()
        if standards_data or codelists:
            library_service = CDISCLibraryService(api_key, cache)
            cache_populator: CachePopulator = CachePopulator(cache, library_service)
            if standards_data:
                asyncio.run(
                    cache_populator.load_standard(
                        standard, standard_version, standard_substandard
                    )
                )
                asyncio.run(cache_populator.load_available_ct_packages())
            asyncio.run(cache_populator.load_codelists(codelists))
        if not rule:
            raise KeyError("'rule' required in request")
        datasets = json_data.get("datasets")
        if not datasets:
            raise KeyError("'datasets' required in request")
        validate_datasets_payload(datasets)
        define_xml = json_data.get("define_xml")
        tester = RuleTester(
            datasets,
            define_xml,
            cache,
            standard,
            standard_version,
            standard_substandard,
            codelists,
            rule,
        )
        result = tester.validate(rule)
        result_json = json.dumps(result)
        return func.HttpResponse(result_json)
    except Exception as e:
        return handle_exception(e)
