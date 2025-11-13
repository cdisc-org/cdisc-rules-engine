from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.constants.metadata_mappings import METADATA_MAPPINGS


class SqlExtractMetadataOperation(SqlBaseOperation):
    def _execute_operation(self):

        domain_metadata = self.params.standards_context.get_domain_metadata(self.params.domain)

        # Map differing attribute names between rules and metadata
        mapped_target = METADATA_MAPPINGS.get(self.params.target, self.params.target)

        final_val = domain_metadata.get(mapped_target)

        if not final_val:
            raise Exception(f"Metadata extraction of {self.params.target} failed - metadata not found")

        return SqlOperationResult(
            query=f"SELECT '{final_val.replace('\'', '\'\'')}' AS value", type="constant", subtype="Char"
        )
