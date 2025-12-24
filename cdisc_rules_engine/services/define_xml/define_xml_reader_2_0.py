from odmlib.define_2_0.rules.metadata_schema import MetadataSchema
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
    DefineXMLVersion,
)


class DefineXMLReader20(BaseDefineXMLReader):
    @staticmethod
    def class_define_xml_version() -> DefineXMLVersion:
        return DefineXMLVersion(
            namespace="http://www.cdisc.org/ns/def/v2.0",
            model_package="define_2_0",
        )

    @staticmethod
    def _meta_data_schema() -> type:
        return MetadataSchema

    def _get_origin_type(self, itemdef):
        return itemdef.Origin.Type if itemdef.Origin else None

    def _get_variable_is_collected(self, itemdef):
        return self._get_origin_type(itemdef) == "CRF" if itemdef.Origin else None

    def _get_metadata_representation(self, metadata) -> dict:
        """
        Returns metadata as dictionary.
        """
        has_no_data: str | None = getattr(metadata, "HasNoData", "")
        has_no_data = has_no_data or ""
        return {
            "define_dataset_name": metadata.Name,
            "define_dataset_label": str(metadata.Description.TranslatedText[0]),
            "define_dataset_location": getattr(metadata.leaf, "href", None),
            "define_dataset_domain": metadata.Domain,
            "define_dataset_class": metadata.Class,
            "define_dataset_structure": str(metadata.Structure),
            # v2.0 does not support is_non_standard. Default to blank
            "define_dataset_is_non_standard": "",
            "define_dataset_has_no_data": bool(has_no_data.lower() == "yes"),
        }

    def get_extensible_codelist_mappings(self):
        metadata = self._odm_loader.MetaDataVersion()
        mappings = {}
        for codelist in metadata.CodeList:
            extended_values = []
            items = codelist.CodeListItem
            for item in items:
                if hasattr(item, "ExtendedValue") and item.ExtendedValue == "Yes":
                    extended_values.append(item.CodedValue)
            if (
                extended_values
                and hasattr(codelist, "Alias")
                and codelist.Alias is not None
            ):
                mappings[codelist.Name] = {
                    "codelist": codelist.Alias[0].Name,
                    "extended_values": extended_values,
                }
        return mappings
