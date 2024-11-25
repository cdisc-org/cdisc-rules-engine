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
        return {
            "define_dataset_name": metadata.Name,
            "define_dataset_label": str(metadata.Description.TranslatedText[0]),
            "define_dataset_location": getattr(metadata.leaf, "href", None),
            "define_dataset_domain": metadata.Domain,
            "define_dataset_class": str(metadata.Class.Name),
            "define_dataset_structure": str(metadata.Structure),
            "define_dataset_is_non_standard": str(metadata.IsNonStandard or ""),
        }

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
            map = self.get_multiple_standards_ct_mappings(metadata, standards)
            return standards, map, True
        else:
            return standards, {}, False

    def get_multiple_standards_ct_mappings(self, metadata, standards):
        combined_CT_package = {"submission_lookup": {}, "no_code": {}}
        standard_oids = {standard.oid: standard for standard in standards}
        for codelist in metadata.CodeList:
            standard_oid = codelist.get("StandardOID", None)
            standard = standard_oids[standard_oid]
            standard_key = f"{standard.name.lower()}ct-{standard.version}"
            if codelist.Alias:
                codelist_code = codelist.Alias[0].Name
            codelist_value = codelist.get("Name", None)
            combined_CT_package["submission_lookup"][codelist_value] = {
                "codelist": codelist_code,
                "term": "N/A",
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
                    combined_CT_package["submission_lookup"][item.CodedValue] = {
                        "codelist": codelist_code,
                        "term": nci_code,
                    }
                else:
                    combined_CT_package["no_code"][item.CodedValue] = {
                        "submissionValue": item.CodedValue,
                        "extensible": item.ExtendedValue,
                        "standard": standard_key,
                        "codelist": codelist_code,
                        "codelist_value": codelist_value,
                    }
        return combined_CT_package

    def get_extensible_codelist_mappings(self):  # noqa
        metadata = self._odm_loader.MetaDataVersion()
        submission_lookup = {}
        extended_values = {}
        for codelist in metadata.CodeList:
            codelist_nci = "N/A"
            if hasattr(codelist, "Alias") and codelist.Alias:
                codelist_nci = (
                    codelist.Alias[0].Name if codelist.Alias[0].Name else "N/A"
                )
            submission_lookup[codelist.Name] = {
                "codelist_code": codelist_nci,
                "term_code": "N/A",
            }
            # Check for extensible items
            for item in codelist.CodeListItem + codelist.EnumeratedItem:
                if item.ExtendedValue == "Yes":
                    # Get term NCI code
                    term_nci = "N/A"
                    if hasattr(item, "Alias") and item.Alias:
                        term_nci = item.Alias[0].Name if item.Alias[0].Name else "N/A"

                    # Add term to submission lookup
                    submission_lookup[item.CodedValue] = {
                        "codelist_code": codelist_nci,
                        "term_code": term_nci,
                    }

                    # Find extended values
                    domain = codelist.OID.split(".")[1]
                    valuelist = next(
                        (
                            vld
                            for vld in metadata.ValueListDef
                            if vld.OID == f"VL.SUPP{domain}"
                        ),
                        None,
                    )

                    if valuelist:
                        for itemref in valuelist.ItemRef:
                            itemdef = next(
                                (
                                    item
                                    for item in metadata.ItemDef
                                    if item.OID == itemref.ItemOID
                                ),
                                None,
                            )
                            if itemdef and hasattr(itemdef, "CodeListRef"):
                                extended_cl = next(
                                    (
                                        cl
                                        for cl in metadata.CodeList
                                        if cl.OID == itemdef.CodeListRef.CodeListOID
                                    ),
                                    None,
                                )
                                if extended_cl:
                                    ext_values = []
                                    for ext_item in extended_cl.CodeListItem:
                                        ext_nci = "N/A"
                                        if (
                                            hasattr(ext_item, "Alias")
                                            and ext_item.Alias
                                        ):
                                            ext_nci = (
                                                ext_item.Alias[0].Name
                                                if ext_item.Alias[0].Name
                                                else "N/A"
                                            )
                                        ext_values.append(
                                            {
                                                "coded_value": ext_item.CodedValue,
                                                "nci_code": ext_nci,
                                            }
                                        )
                                    extended_values[codelist_nci] = ext_values

        return {
            "submission_lookup": submission_lookup,
            "extended_values": extended_values,
        }
