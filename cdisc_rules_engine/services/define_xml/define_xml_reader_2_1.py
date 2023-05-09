from odmlib.define_2_1.rules.metadata_schema import MetadataSchema
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
    DefineXMLVersion,
)


class DefineXMLReader21(BaseDefineXMLReader):
    @staticmethod
    def class_define_xml_version() -> DefineXMLVersion:
        return DefineXMLVersion(
            version="2.1.0",
            namespace="http://www.cdisc.org/ns/def/v2.1",
            model_package="define_2_1",
        )

    @staticmethod
    def _meta_data_schema() -> type:
        return MetadataSchema

    def _get_origin_type(self, itemdef):
        return itemdef.Origin[0].Type if itemdef.Origin else None

    def _get_variable_is_collected(self, itemdef):
        return self._get_origin_type(itemdef) == "Collected" if itemdef.Origin else None
