from typing import List

from cdisc_rules_engine.enums.optional_condition_parameters import (
    OptionalConditionParameters,
)
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.enums.sensitivity import Sensitivity


class Rule:
    """
    This class represents a rule DB record.
    A rule DB record represents a proprietary rule, but
    the structure will match the CDISC rules format
    """

    def __init__(self, record_params: dict):
        self.core_id: str = record_params["core_id"]
        self.reference: List[dict] = record_params["reference"]
        self.sensitivity: Sensitivity = record_params["sensitivity"]
        self.executability: str = record_params["executability"]
        self.category: str = record_params["category"]
        self.author: str = record_params["author"]
        self.description: str = record_params["description"]
        self.authority: dict = record_params["authority"]
        self.standards: dict = record_params["standards"]
        self.classes: dict = record_params.get("classes")
        self.domains: dict = record_params.get("domains")
        self.datasets: dict = record_params.get("datasets")
        self.rule_type: RuleTypes = record_params["rule_type"]
        self.operations: List[dict] = record_params.get("operations")
        self.conditions: dict = record_params["conditions"]
        self.actions: dict = record_params["actions"]
        self.output_variables: dict = record_params.get("output_variables")

    @classmethod
    def from_cdisc_metadata(cls, rule_metadata: dict) -> dict:
        if cls.is_cdisc_rule_metadata(rule_metadata):
            authorities = rule_metadata.get("Authorities", [])
            executable_rule = {
                "core_id": rule_metadata.get("Core", {}).get("Id"),
                "author": "CDISC",
                "reference": cls.parse_references(authorities),
                "sensitivity": rule_metadata.get("Sensitivity"),
                "executability": rule_metadata.get("Executability", "").lower(),
                "description": rule_metadata.get("Description"),
                "authorities": authorities,
                "standards": cls.parse_standards(authorities),
                "classes": rule_metadata.get("Scope", {}).get("Classes"),
                "domains": rule_metadata.get("Scope", {}).get("Domains"),
                "rule_type": rule_metadata.get("Rule_Type"),
                "conditions": cls.parse_conditions(rule_metadata.get("Check")),
                "actions": cls.parse_actions(rule_metadata.get("Outcome")),
            }

            if "Operations" in rule_metadata:
                executable_rule["operations"] = rule_metadata.get("Operations")

            if "Match_Datasets" in rule_metadata:
                executable_rule["datasets"] = cls.parse_datasets(
                    rule_metadata.get("Match_Datasets")
                )

            if "Output_Variables" in rule_metadata.get("Outcome", {}):
                executable_rule["output_variables"] = rule_metadata.get("Outcome", {})[
                    "Output_Variables"
                ]
            return executable_rule
        else:
            return rule_metadata

    @classmethod
    def parse_standards(cls, authorities: List[dict]) -> List[dict]:
        standards = []
        for authority in authorities:
            for standard in authority.get("Standards", []):
                standards.append(
                    {"Name": standard.get("Name"), "Version": standard.get("Version")}
                )
        return standards

    @classmethod
    def parse_references(cls, authorities: List[dict]) -> list:
        references = []
        for authority in authorities:
            for standard in authority.get("Standards", []):
                references.append(standard.get("References"))
        return references

    @classmethod
    def is_cdisc_rule_metadata(cls, rule_metadata: dict) -> bool:
        return "Core" in rule_metadata

    @classmethod
    def parse_conditions(cls, conditions: dict) -> dict:
        if not conditions:
            raise ValueError("No check data provided")
        all_conditions = conditions.get("all")
        any_conditions = conditions.get("any")
        not_condition = conditions.get("not")
        conditions_json = {}
        if all_conditions:
            conditions_json["all"] = cls.build_conditions(all_conditions)
        if any_conditions:
            conditions_json["any"] = cls.build_conditions(any_conditions)
        if not_condition:
            conditions_json["not"] = cls.parse_conditions(not_condition)
        return conditions_json

    @classmethod
    def build_conditions(cls, conditions_data: List[dict]) -> dict:
        function = "get_dataset"
        conditions = []

        for condition in conditions_data:
            if "all" in condition:
                conditions.append({"all": cls.build_conditions(condition.get("all"))})
            elif "any" in condition:
                conditions.append({"any": cls.build_conditions(condition.get("any"))})
            elif "not" in condition:
                conditions.append({"not": cls.parse_conditions(condition.get("not"))})
            else:
                conditions.append(cls.build_condition(condition, function))

        return conditions

    @classmethod
    def build_condition(cls, condition: dict, variable_function: str) -> dict:
        data = {
            "name": variable_function,
            "operator": condition.get("operator"),
            "value": {
                "target": condition.get("name"),
                "comparator": condition.get("value"),
            },
        }
        if "variables" in condition:
            data["variables"] = condition["variables"]
        for optional_parameter in OptionalConditionParameters.values():
            if optional_parameter in condition:
                data["value"][optional_parameter] = condition.get(optional_parameter)
        return data

    @classmethod
    def parse_actions(cls, actions_data: dict) -> List[dict]:
        if not actions_data:
            raise ValueError("No actions data provided")
        action = "generate_dataset_error_objects"
        return [{"name": action, "params": {"message": actions_data.get("Message")}}]

    @classmethod
    def parse_datasets(cls, match_key_data: List[dict]) -> List[dict]:
        # Defaulting to IDVAR and IDVARVAL as relationship columns.
        # May change in the future as more standard rules are written.
        relationship_columns = {
            "column_with_names": "IDVAR",
            "column_with_values": "IDVARVAL",
        }
        if not match_key_data:
            return None
        datasets = []
        for data in match_key_data:
            join_data = {
                "domain_name": data.get("Name"),
                "match_key": data.get("Keys"),
            }
            if data.get("Is_Relationship", False):
                join_data["relationship_columns"] = relationship_columns
            datasets.append(join_data)
        return datasets
