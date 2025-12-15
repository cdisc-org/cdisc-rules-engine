from odmlib.define_2_1.rules.metadata_schema import MetadataSchema
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
    DefineXMLVersion,
    StandardsCTMetadata,
)
from typing import List
from collections import Counter


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
        has_no_data: str | None = getattr(metadata, "HasNoData", "")
        has_no_data = has_no_data or ""
        return {
            "define_dataset_name": metadata.Name,
            "define_dataset_label": str(metadata.Description.TranslatedText[0]),
            "define_dataset_location": getattr(metadata.leaf, "href", None),
            "define_dataset_domain": metadata.Domain,
            "define_dataset_class": str(metadata.Class.Name),
            "define_dataset_structure": str(metadata.Structure),
            "define_dataset_is_non_standard": str(metadata.IsNonStandard or ""),
            "define_dataset_has_no_data": bool(has_no_data.lower() == "yes"),
        }

    def get_ct_version(self):
        metadata = self._odm_loader.MetaDataVersion()
        standards = []
        for standard in metadata.Standards.Standard:
            if (
                standard.Type == "CT"
                and "define-xml" not in standard.PublishingSet.lower()
            ):
                standards.append(
                    f"{standard.PublishingSet.lower()}ct-{standard.Version}"
                )
        return standards

    def get_ct_standards_metadata(self) -> List[StandardsCTMetadata]:
        """Extract standards metadata from Define-XML 2.1"""
        metadata = self._odm_loader.MetaDataVersion()
        standards = []
        for standard in metadata.Standards:
            if standard.Type == "CT":
                standards.append(
                    StandardsCTMetadata(
                        name=standard.Name,
                        version=standard.Version,
                        type=standard.Type,
                        publishing_set=standard.PublishingSet,
                        oid=standard.OID,
                    )
                )
        publishing_set_counts = Counter(
            standard.PublishingSet
            for standard in metadata.Standards
            if standard.Type == "CT"
        )
        multiple_sets = {
            ps: count for ps, count in publishing_set_counts.items() if count > 1
        }
        if multiple_sets:
            multi_ct, extensible = self.get_multiple_standards_ct_mappings(
                metadata, standards
            )
            return standards, multi_ct, extensible, True
        else:
            return standards, {}, {}, False

    def get_multiple_standards_ct_mappings(self, metadata, standards):
        combined_CT_package = {}
        standard_oids = {standard.oid: standard for standard in standards}
        extensible = {}
        for codelist in metadata.CodeList:
            standard_oid = codelist.StandardOID
            standard = standard_oids[standard_oid]
            standard_key = f"{standard.publishing_set.lower()}ct-{standard.version}"
            codelist_code = codelist.Alias[0].Name if codelist.Alias else None
            codelist_value = codelist.Name if hasattr(codelist, "Name") else None

            extended_values = []
            items = codelist.CodeListItem
            for item in items:
                if hasattr(item, "ExtendedValue") and item.ExtendedValue == "Yes":
                    extended_values.append(item.CodedValue)
            if extended_values and codelist.Alias:
                extensible[codelist_value] = {
                    "codelist": codelist_code,
                    "extended_values": extended_values,
                }

            for item in codelist.CodeListItem:
                nci_code = None
                if hasattr(item, "Alias"):
                    for alias in item.Alias:
                        if hasattr(alias, "Name"):
                            nci_code = alias.Name
                            break
                if nci_code:
                    combined_CT_package[nci_code] = {
                        "conceptId": nci_code,
                        "submissionValue": item.CodedValue,
                        "extensible": item.ExtendedValue,
                        "standard": standard_key,
                    }

        return combined_CT_package, extensible

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
