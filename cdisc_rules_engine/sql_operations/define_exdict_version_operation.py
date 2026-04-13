from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDefineExternalDictionaryVersionOperation(SqlBaseOperation):

    def _execute_operation(self):
        external_dictionary_type = self.params.external_dictionary_type

        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        version = define_xml_reader.get_external_dictionary_version(external_dictionary_type)
        if not version:
            raise Exception(
                f"Version for external dictionary type {external_dictionary_type} is not found in define.xml"
            )

        return SqlOperationResult(query=f"SELECT '{version}' AS value", type="constant", subtype="Char")
