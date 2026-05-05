from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlNameReferencedVariableMetadata(SqlBaseOperation):
    def _execute_operation(self):
        standard_metadata = self.params.standards_context.get_standard_metadata()
        attribute = getattr(self.params, "attribute_name", "role")

        mapping = []

        for cls in standard_metadata.get("classes", []):
            for dataset in cls.get("datasets", []):
                for var in dataset.get("datasetVariables", []):
                    name = var.get("name")
                    attr_val = var.get(attribute)

                    if not name or attr_val is None:
                        continue

                    mapping.append(f"('{name}', '{attr_val}')")

        if not mapping:
            return SqlOperationResult(query="SELECT NULL", type="constant", subtype="Char")

        values_clause = ", ".join(mapping)
        query = f"SELECT val FROM (VALUES {values_clause}) AS t(name, val) WHERE t.name = %target%"

        return SqlOperationResult(
            query=query,
            type="collection",
            subtype="Char",
            params={"%target%": self.params.target},
        )
