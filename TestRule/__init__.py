import azure.functions as func
from cdisc_rule_tester.services.config_service import ConfigService
from cdisc_rule_tester.models.rule_tester import RuleTester
import json

def validate_datasets_payload(datasets):
    required_keys = {"filename", "label", "domain", "records", "variables"}
    missing_keys = set()
    for dataset in datasets:
        for key in required_keys:
            if key not in dataset:
                missing_keys.add(key)

    if missing_keys:
        raise KeyError(f"one or more datasets missing the following keys {missing_keys}")


def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    try:
        json_data = req.get_json()
        rule = json_data.get("rule")
        if not rule:
            raise KeyError("'rule' required in request")
        datasets = json_data.get("datasets")
        if not datasets:
            raise KeyError("'datasets' required in request")
        validate_datasets_payload(datasets)
        tester = RuleTester(datasets)
        return func.HttpResponse(json.dumps(tester.validate(rule)))
    except KeyError as e:
        return func.HttpResponse(json.dumps({"error": "KeyError", "message": str(e)}), status_code=400)
    except Exception as e:
        return func.HttpResponse(json.dumps({"errror": "Unknown Exception", "message": f"An unhandled exception occurred. {str(e)}"}), status_code=500)
 
