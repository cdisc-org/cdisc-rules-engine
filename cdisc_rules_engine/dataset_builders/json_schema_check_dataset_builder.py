from jsonschema import validators
from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class JsonSchemaCheckDatasetBuilder(BaseDatasetBuilder):
    def get_dataset(self):
        """Return a dataset where each row represents a JSON Schema validation error.
        Columns: path, message, validator, validator_value, schema_path.
        Returns an empty dataset (with headers) if there are no errors.
        """
        schema = self.library_metadata.standard_schema_definition
        cls = validators.validator_for(schema)
        cls.check_schema(schema)
        validator = cls(schema)

        errors = sorted(
            validator.iter_errors(self.data_service.json), key=lambda e: e.path
        )

        error_rows = []
        for err in errors:
            path = ".".join(str(p) for p in err.absolute_path) or "(root)"
            error_rows.append(
                {
                    "path": path,
                    "message": err.message,
                    "validator": err.validator,
                    "validator_value": err.validator_value,
                    "schema_path": " > ".join(map(str, err.schema_path)),
                }
            )

        # Build dataset with the detected implementation (Pandas / Dask)
        if error_rows:
            return self.dataset_implementation.from_records(error_rows)
        else:
            # Return empty dataset with expected columns
            return self.dataset_implementation.from_records(
                [],
                columns=[
                    "path",
                    "message",
                    "validator",
                    "validator_value",
                    "schema_path",
                ],
            )
