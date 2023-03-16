import json

class Package:

    def __init__(self, name, version):
        self.package_name = f"{name.lower()}-{version.replace('.', '-')}-cr"
        self.name = name
        self.version = version
        self.links = {
            "self": {
                "href": f"/mdr/rules/{name.lower()}/{version.replace('.', '-')}",
                "title": f"{name} Conformance Rules v{version}",
                "type": "Conformance Rules Package"
            }
        }
        self.rules = {}
    
    def add_rule(self, rule):
        self.rules[rule.id] = rule
    
    def to_json(self):
        rule_json = {}
        for id, rule in self.rules.items():
            rule_json[id] = rule.to_json()
        return {
            "_links": self.links,
            "rules": rule_json
        }

    def write_to_file(self):
        file_name = f"{self.package_name}.json"
        with open(file_name, "w") as f:
            json.dump(self.to_json(), f, indent=4)