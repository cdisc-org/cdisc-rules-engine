from cdisc_library_client import CDISCLibraryClient
from cdisc_rule_tester.services.rules_data_service import RuleDataService
from cdisc_rule_tester.models.package import Package
from cdisc_rule_tester.services.config_service import ConfigService
from cdisc_rule_tester.models.rule import Rule
from dotenv import load_dotenv
from azure.storage.blob import BlobClient
from copy import deepcopy
from cdisc_rule_tester.utilities.rule_utils import build_standards_links
import json

def process_rules(rules, library_data_service, packages):
    for rule in rules:
        try:
            rule_obj = Rule.from_json(rule)
        except ValueError as e:
            print(f"Error while processing rule {rule.get('Core', rule.get('CoreId'))} {e}")
            continue
        except Exception as e:
            print(f"Unexpected Error while processing rule {rule.get('Core', rule.get('CoreId'))} {e}")
            continue
        if rule_obj:
            standards = build_standards_links(rule_obj, library_data_service)
            print(standards)
            rule_obj.add_link("standards", standards)
            try:
                standards = rule_obj.get_standards()
            except Exception as e:
                print(f"Invalid package reference for rule: {rule_obj.core_id} {e}")
                continue

            for standard, version in standards.items():
                package_key = f"{standard}-{version}-cr"
                package = packages.get(package_key, Package(standard, version))
                rule_obj_copy = deepcopy(rule_obj)
                self_link = rule_obj_copy.build_self_link(package)
                rule_obj_copy.add_link("self", self_link)
                rule_obj_copy.add_link("package", package.links.get("self"))
                package.add_rule(rule_obj_copy)
                packages[package_key] = package

if __name__ == "__main__":
    packages = {}
    load_dotenv()
    config = ConfigService()
    rules_data_service = RuleDataService(config)
    library_data_service = CDISCLibraryClient(config.getValue("LIBRARY_API_KEY"), base_api_url=config.getValue("LIBRARY_API_URL"))
    json_rules = rules_data_service.get_rules(params="page[limit]=50")
    process_rules(json_rules, library_data_service, packages)
    while rules_data_service.has_more_rules:
        json_rules = rules_data_service.get_rules(rules_data_service.next_params)
        process_rules(json_rules, library_data_service, packages)
    for package in packages.values():
        print(f"Package {package.package_name} has {len(package.rules.keys())} published rules")
        blob = BlobClient.from_connection_string(
                conn_str=config.getValue('AZURE_CONNECTION_STRING'), 
                container_name=config.getValue('CONTAINER_NAME'), 
                blob_name=f"rules/{package.package_name}.json"
            )
        blob.upload_blob(json.dumps(package.to_json(), indent=4), overwrite=True)
