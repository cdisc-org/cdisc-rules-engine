from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlGetDefineVariablesMetadata(SqlBaseOperation):

    def _execute_operation(self):

        domain_name = self.params.domain.upper()
        try:
            define_variables_metadata = self.params.standards_context.get_define_xml_variables_metadata(
                self.data_service, domain_name
            )
        except Exception as e:
            raise ValueError(f"Error: Domain {domain_name} is not found in Define XML", e)

        value = next(
            (
                i.get(self.params.attribute_name)
                for i in define_variables_metadata
                if i.get("define_variable_name") == self.params.target
            ),
            "No Value Found",  # avoided None as that could be a valid value for the metadata and we want to be able to distinguish between not found and found with None value # noqa
        )

        if value == "No Value Found":
            raise Exception(f"Metadata extraction of {self.params.target} failed - metadata not found")

        # handle different types
        if isinstance(value, str):
            value = value.replace("'", "''")

        return SqlOperationResult(query=f"SELECT '{value}' AS value", type="constant", subtype="Char")
