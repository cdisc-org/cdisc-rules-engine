import json

from jsonschema import validators

from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetJsonSchemaErrors(BaseOperation):
    def _execute_operation(self):
        # load schema from cache
        schema = self.library_metadata.standard_schema_definition
        cls = validators.validator_for(schema)
        cls.check_schema(schema)
        validator = cls(schema)

        errors = sorted(
            validator.iter_errors(self.data_service.json), key=lambda e: e.path
        )

        error_list = []
        for err in errors:
            path = ".".join(str(p) for p in err.absolute_path) or "(root)"
            error_info = {
                "path": path,
                "message": err.message,
                "validator": err.validator,
                "validator_value": err.validator_value,
                "schema_path": " > ".join(map(str, err.schema_path)),
            }
            error_list.append(json.dumps(error_info))

        return error_list
