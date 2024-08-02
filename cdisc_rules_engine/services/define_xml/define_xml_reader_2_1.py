from odmlib.define_2_1.rules.metadata_schema import MetadataSchema
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
    DefineXMLVersion,
)


class DefineXMLReader21(BaseDefineXMLReader):
    @staticmethod
    def class_define_xml_version() -> DefineXMLVersion:
        return DefineXMLVersion(
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

    def _get_metadata_representation(self, metadata) -> dict:
        """
        Returns metadata as dictionary.
        """
        return {
            "define_dataset_name": metadata.Name,
            "define_dataset_label": str(metadata.Description.TranslatedText[0]),
            "define_dataset_location": getattr(metadata.leaf, "href", None),
            "define_dataset_domain": metadata.Domain,
            "define_dataset_class": str(metadata.Class.Name),
            "define_dataset_structure": str(metadata.Structure),
            "define_dataset_is_non_standard": str(metadata.IsNonStandard or ""),
        }
