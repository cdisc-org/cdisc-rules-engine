import yaml
import json
from cdisc_rule_tester.models.enums.rule_types import RuleTypes
from cdisc_rule_tester.models.enums.optional_condition_parameters import OptionalConditionParameters

class Rule:

    def __init__(self):
        self.reference = None
        self.core_id = None
        self.version = None
        self.authority = None
        self.description = None
        self.sensitivity = None
        self.severity = None
        self.conditions = None
        self.standards = None
        self.classes = None
        self.domains = None
        self.document = None
        self.rule_type = None
        self.actions = None
        self.cited_guidance = None
        self.output_variables = None
        self.operations = {}
        self.id = None
        self.links = {}


    @staticmethod
    def from_json(json_rule):
        rule = Rule()
        rule.id = json_rule.get("id")
        yaml_data = json_rule.get("attributes", {}).get("body", {}).get("value", "")
        rule = Rule.from_yaml(yaml_data, rule)
        return rule

    @staticmethod
    def validate(rule_data):
        required_keys = ["Severity", "Core"]
        missing_keys = []
        for key in required_keys:
            if key not in rule_data:
                missing_keys.append(key)
        
        if rule_data.get("Core", {}).get("Id") is None:
            missing_keys.append("Core.Id")

        if missing_keys:
            raise ValueError(f"Provided rule is missing keys: {missing_keys}")

    @staticmethod
    def from_yaml(yaml_data, rule = None):
        rule = rule or Rule()
        data = yaml.safe_load(yaml_data)
        Rule.validate(data)
        rule.core_id = data.get("Core", {}).get("Id")
        rule.reference = data.get("References")
        rule.sensitivity = data.get("Sensitivity")
        rule.severity = data.get("Severity").lower()
        rule.description = data.get("Description")
        rule.authority = data.get("Authority")
        rule.standards = data.get("Scopes", {}).get("Standards")
        rule.classes = data.get("Scopes", {}).get("Classes")
        rule.domains = data.get("Scopes", {}).get("Domains")
        rule.rule_type = data.get("Rule Type", "")
        if not rule.isValidRuleType():
            raise ValueError("Invalid rule type provided.")
        rule.conditions = rule.parse_conditions(data.get("Check"))
        rule.actions = rule.parse_actions(data.get("Outcome"))
        rule.output_variables = rule.get_output_variables(data.get("Outcome", {}))
        rule.datasets = rule.parse_datasets(data.get("Match Datasets"))
        rule.operations = data.get("Operations")

        return rule

    def parse_conditions(self, conditions):
        if not conditions:
            raise ValueError("No check data provided")
        all_conditions = conditions.get("all")
        any_conditions = conditions.get("any")
        not_condition = conditions.get("not")
        conditions_json = {}
        if all_conditions:
            conditions_json["all"] = self.build_conditions(all_conditions)
        if any_conditions:
            conditions_json["any"] = self.build_conditions(any_conditions)
        if not_condition:
            conditions_json["not"] = self.parse_conditions(not_condition)
        return conditions_json
    
    def parse_datasets(self, match_key_data):
        # Defaulting to IDVAR and IDVARVAL as relationship columns. May change in the future
        # As more standard rules are written.
        relationship_columns = {
            "column_with_names": "IDVAR",
            "column_with_values": "IDVARVAL"
        }
        if not match_key_data:
            return None
        datasets = []
        for data in match_key_data:
            join_data = {
                "domain_name": data.get("Name"),
                "match_key": data.get("Keys"),
            }
            if data.get("Is Relationship", False):
                join_data["relationship_columns"] = relationship_columns
            datasets.append(join_data)
        return datasets
    
    def build_self_link(self, parent_package):
        return {
            "href": f"/mdr/rules/{parent_package.name.lower()}/{parent_package.version.replace('.', '-')}/rule/{self.id}",
            "title": self.core_id,
            "type": "Conformance Rule"
        }

    def add_link(self, key, value):
        self.links[key] = value
    
    def parse_actions(self, actions_data):
        if not actions_data:
            raise ValueError("No actions data provided")
        action = "generate_dataset_error_objects"
        return [{
            "name": action,
            "params": {
                "message": actions_data.get("Message")
            }
        }]
    
    def get_output_variables(self, outcome):
        return outcome.get("Output Variables", [])

    def to_json(self):
        json_data = {
            "id": self.id,
            "_links": self.links,
            "core_id": self.core_id,
            "reference": self.reference,
            "sensitivity": self.sensitivity,
            "severity": self.severity,
            "description": self.description,
            "authority": self.authority,
            "standards": self.standards,
            "classes": self.classes,
            "domains": self.domains,
            "datasets": self.datasets,
            "rule_type": self.rule_type,
            "conditions": self.conditions,
            "operations": self.operations,
            "output_variables": self.output_variables,
            "actions": self.actions
        }
        return self.remove_none(json_data)
    
    def remove_none(self, data):
        if isinstance(data, dict):
            return {k:self.remove_none(v) for k, v in data.items() if k is not None and v is not None}
        elif isinstance(data, list):
            return [self.remove_none(item) for item in data if item is not None]
        elif isinstance(data, tuple):
            return tuple(self.remove_none(item) for item in data if item is not None)
        elif isinstance(data, set):
            return {self.remove_none(item) for item in data if item is not None}
        else:
            return data

    def get_package_type(self):
        package_map = {
            "SDTM": "sdtm-cr"
        }
        package_type = self.reference.get("Origin").split()[0]
        return package_map.get(package_type)

    def get_package_version(self):
        return self.reference.get("Version").replace(".", "-")

    def get_standards(self):
        return {standard["Name"].lower() : standard["Version"].replace(".", "-") for standard in self.standards}

    def add_link(self, key, value):
        self.links[key] = value

    def write_to_file(self):
        file_name = f"{self.core_id}.json"
        with open(file_name, "w") as f:
            json.dump(self.to_json(), f, indent=4)

    def build_conditions(self, conditions_data):
        function = "get_dataset"
        conditions = []

        for condition in conditions_data:
            if "all" in condition:
                conditions.append({
                    "all": self.build_conditions(condition.get("all")) 
                }) 
            elif "any" in condition:
                conditions.append({
                    "any": self.build_conditions(condition.get("any"))
                })
            elif "not" in condition:
                conditions.append({
                    "not": self.parse_conditions(condition.get("not"))
                })
            else:
                conditions.append(self.build_condition(condition, function))

        return conditions

    def build_condition(self, condition, variable_function):
        data = {
            "name": variable_function,
            "operator": condition.get("operator"),
            "value": {
                "target": condition.get("name"),
                "comparator": condition.get("value")
            }
        }
        for optional_parameter in OptionalConditionParameters.values():
            if optional_parameter in condition:
                data["value"][optional_parameter] = condition.get(optional_parameter)
        return data

    def isValidRuleType(self):
        return self.rule_type in RuleTypes.values()

