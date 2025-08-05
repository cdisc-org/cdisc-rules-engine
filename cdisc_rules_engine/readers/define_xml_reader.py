import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime
from dataclasses import dataclass

from cdisc_rules_engine.readers.base_reader import BaseReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DefineXMLMetadata:
    """Metadata extracted from Define-XML files."""

    file_type: str
    odm_version: str
    file_oid: str
    creation_datetime: Optional[datetime]


class XMLReader(BaseReader):
    """
    Reader for CDISC Define-XML files.
    Reads define.xml files and extracts metadata structure.
    """

    NAMESPACES = {
        "odm": "http://www.cdisc.org/ns/odm/v1.3",
        "def": "http://www.cdisc.org/ns/def/v2.0",
        "xlink": "http://www.w3.org/1999/xlink",
        "arm": "http://www.cdisc.org/ns/arm/v1.0",
    }

    SUPPORTED_EXTENSIONS = [".xml"]

    def __init__(self, file_path: str):
        super().__init__(file_path)

    def _extract_metadata(self) -> DefineXMLMetadata:
        """Extract metadata from the Define-XML file."""
        try:
            tree = ET.parse(self.file_path)
            root = tree.getroot()

            odm_version = root.get("ODMVersion", "")
            file_oid = root.get("FileOID", "")
            creation_datetime_str = root.get("CreationDateTime")

            creation_datetime = self._parse_datetime(creation_datetime_str)

            return DefineXMLMetadata(
                file_type="define-xml", odm_version=odm_version, file_oid=file_oid, creation_datetime=creation_datetime
            )

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {str(e)}")
            raise ValueError(f"Invalid XML format: {str(e)}")
        except Exception as e:
            logger.error(f"Error extracting metadata: {str(e)}")
            raise

    def read(self) -> Dict[str, Union[List, Dict]]:
        """Read Define-XML file and extract all metadata"""

        data = {
            "studies": {},
            "metadata_versions": {},
            "datasets": [],
            "variables": [],
            "dataset_variables": [],
            "codelists": [],
            "codelist_items": [],
            "methods": [],
            "value_lists": [],
            "value_list_items": [],
            "where_clauses": [],
            "where_clause_conditions": [],
            "comments": [],
            "documents": [],
            "analysis_results": [],
            "variable_codelist_refs": [],
            "variable_value_lists": [],
        }

        self._id_counters = {table: 0 for table in data.keys()}
        self._oid_to_id_maps = {table: {} for table in data.keys()}

        try:
            tree = ET.parse(self.file_path)
            root = tree.getroot()

            for study in root.findall(".//odm:Study", self.NAMESPACES):
                self._process_study(study, data)

            return data

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {str(e)}")
            raise ValueError(f"Invalid XML format: {str(e)}")
        except Exception as e:
            logger.error(f"Error reading Define-XML: {str(e)}")
            raise

    def _get_next_id(self, table_name: str) -> int:
        """Get next ID for a table"""
        self._id_counters[table_name] += 1
        return self._id_counters[table_name]

    def _process_study(self, study_elem: ET.Element, data: Dict[str, Union[List, Dict]]) -> None:
        """Process study element and its children"""
        study_oid = study_elem.get("OID")

        global_vars = study_elem.find(".//odm:GlobalVariables", self.NAMESPACES)
        study_name = self._get_text(global_vars, "odm:StudyName")
        study_description = self._get_text(global_vars, "odm:StudyDescription")
        protocol_name = self._get_text(global_vars, "odm:ProtocolName")

        study_id = self._get_next_id("studies")
        data["studies"] = {
            "study_id": study_id,
            "study_oid": study_oid,
            "study_name": study_name,
            "study_description": study_description,
            "protocol_name": protocol_name,
        }

        for mv in study_elem.findall(".//odm:MetaDataVersion", self.NAMESPACES):
            self._process_metadata_version(mv, study_id, data)

    def _process_metadata_version(self, mv_elem: ET.Element, study_id: int, data: Dict[str, Union[dict, list]]) -> None:
        """Process MetaDataVersion element"""
        version_id = self._get_next_id("metadata_versions")
        version_oid = mv_elem.get("OID")

        self._oid_to_id_maps["metadata_versions"][version_oid] = version_id

        version_data = {
            "version_id": version_id,
            "study_id": study_id,
            "version_oid": version_oid,
            "version_name": mv_elem.get("Name"),
            "description": mv_elem.get("Description"),
            "define_version": mv_elem.get("{%s}DefineVersion" % self.NAMESPACES["def"]),
            "standard_name": mv_elem.get("{%s}StandardName" % self.NAMESPACES["def"]),
            "standard_version": mv_elem.get("{%s}StandardVersion" % self.NAMESPACES["def"]),
            "creation_datetime": self.metadata.creation_datetime,
        }
        data["metadata_versions"] = version_data

        self._process_item_groups(mv_elem, version_id, data)
        self._process_item_defs(mv_elem, version_id, data)
        self._process_codelists(mv_elem, version_id, data)
        self._process_methods(mv_elem, version_id, data)
        self._process_comments(mv_elem, version_id, data)
        self._process_value_lists(mv_elem, version_id, data)
        self._process_where_clauses(mv_elem, version_id, data)
        self._process_documents(mv_elem, version_id, data)
        self._process_analysis_results(mv_elem, version_id, data)

    def _process_item_groups(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process ItemGroupDef elements (datasets)"""
        for ig in parent.findall(".//odm:ItemGroupDef", self.NAMESPACES):
            dataset_id = self._get_next_id("datasets")
            dataset_oid = ig.get("OID")

            self._oid_to_id_maps["datasets"][f"{version_id}:{dataset_oid}"] = dataset_id

            desc_elem = ig.find(".//odm:Description/odm:TranslatedText", self.NAMESPACES)
            description = desc_elem.text if desc_elem is not None else None

            dataset_data = {
                "dataset_id": dataset_id,
                "version_id": version_id,
                "dataset_oid": dataset_oid,
                "domain": ig.get("Domain"),
                "dataset_name": ig.get("Name"),
                "description": description,
                "repeating": ig.get("Repeating") == "Yes",
                "purpose": ig.get("Purpose"),
                "is_reference_data": ig.get("IsReferenceData") == "Yes",
                "sas_dataset_name": ig.get("SASDatasetName"),
                "structure": ig.get("{%s}Structure" % self.NAMESPACES["def"]),
                "class": ig.get("{%s}Class" % self.NAMESPACES["def"]),
                "comment_oid": ig.get("{%s}CommentOID" % self.NAMESPACES["def"]),
                "archive_location_id": ig.get("{%s}ArchiveLocationID" % self.NAMESPACES["def"]),
            }
            data["datasets"].append(dataset_data)

            self._process_item_refs(ig, dataset_id, version_id, data)

    def _process_item_refs(
        self, parent: ET.Element, dataset_id: int, version_id: int, data: Dict[str, Union[List, Dict]]
    ) -> None:
        """Process ItemRef elements (dataset variables)"""
        for item_ref in parent.findall(".//odm:ItemRef", self.NAMESPACES):
            variable_oid = item_ref.get("ItemOID")

            variable_id = self._oid_to_id_maps["variables"].get(f"{version_id}:{variable_oid}")

            if variable_id:
                dataset_var_data = {
                    "dataset_var_id": self._get_next_id("dataset_variables"),
                    "dataset_id": dataset_id,
                    "variable_id": variable_id,
                    "order_number": self._safe_int(item_ref.get("OrderNumber")),
                    "mandatory": item_ref.get("Mandatory") == "Yes",
                    "key_sequence": self._safe_int(item_ref.get("KeySequence")),
                    "role": item_ref.get("Role"),
                    "role_codelist_oid": item_ref.get("RoleCodeListOID"),
                    "method_oid": item_ref.get("MethodOID"),
                }
                data["dataset_variables"].append(dataset_var_data)

    def _process_item_defs(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process ItemDef elements (variables)"""
        for item_def in parent.findall(".//odm:ItemDef", self.NAMESPACES):
            variable_id = self._get_next_id("variables")
            variable_oid = item_def.get("OID")

            self._oid_to_id_maps["variables"][f"{version_id}:{variable_oid}"] = variable_id

            desc_elem = item_def.find(".//odm:Description/odm:TranslatedText", self.NAMESPACES)
            description = desc_elem.text if desc_elem is not None else None

            origin_elem = item_def.find(".//def:Origin", self.NAMESPACES)
            origin_type = origin_elem.get("Type") if origin_elem is not None else None
            origin_desc_elem = (
                origin_elem.find(".//odm:Description/odm:TranslatedText", self.NAMESPACES)
                if origin_elem is not None
                else None
            )
            origin_description = origin_desc_elem.text if origin_desc_elem is not None else None

            variable_data = {
                "variable_id": variable_id,
                "version_id": version_id,
                "variable_oid": variable_oid,
                "variable_name": item_def.get("Name"),
                "sas_field_name": item_def.get("SASFieldName"),
                "data_type": item_def.get("DataType"),
                "length": self._safe_int(item_def.get("Length")),
                "significant_digits": self._safe_int(item_def.get("SignificantDigits")),
                "display_format": item_def.get("{%s}DisplayFormat" % self.NAMESPACES["def"]),
                "description": description,
                "origin_type": origin_type,
                "origin_description": origin_description,
                "comment_oid": item_def.get("{%s}CommentOID" % self.NAMESPACES["def"]),
            }
            data["variables"].append(variable_data)

            codelist_ref = item_def.find(".//odm:CodeListRef", self.NAMESPACES)
            if codelist_ref is not None:
                codelist_oid = codelist_ref.get("CodeListOID")
                if codelist_oid:
                    self._add_variable_codelist_ref(variable_id, codelist_oid, version_id, data)

            value_list_ref = item_def.find(".//def:ValueListRef", self.NAMESPACES)
            if value_list_ref is not None:
                value_list_oid = value_list_ref.get("ValueListOID")
                if value_list_oid:
                    self._add_variable_value_list_ref(variable_id, value_list_oid, version_id, data)

    def _process_codelists(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process CodeList elements"""
        for codelist in parent.findall(".//odm:CodeList", self.NAMESPACES):
            codelist_id = self._get_next_id("codelists")
            codelist_oid = codelist.get("OID")

            self._oid_to_id_maps["codelists"][f"{version_id}:{codelist_oid}"] = codelist_id

            external_ref = codelist.find(".//odm:ExternalCodeList", self.NAMESPACES)
            is_external = external_ref is not None

            codelist_data = {
                "codelist_id": codelist_id,
                "version_id": version_id,
                "codelist_oid": codelist_oid,
                "codelist_name": codelist.get("Name"),
                "data_type": codelist.get("DataType"),
                "is_external": is_external,
                "external_dictionary": external_ref.get("Dictionary") if external_ref is not None else None,
                "external_version": external_ref.get("Version") if external_ref is not None else None,
            }
            data["codelists"].append(codelist_data)

            # Process codelist items
            for item in codelist.findall(".//odm:CodeListItem", self.NAMESPACES):
                decode_elem = item.find(".//odm:Decode/odm:TranslatedText", self.NAMESPACES)

                item_data = {
                    "item_id": self._get_next_id("codelist_items"),
                    "codelist_id": codelist_id,
                    "coded_value": item.get("CodedValue"),
                    "decode_text": decode_elem.text if decode_elem is not None else None,
                    "rank": self._safe_int(item.get("Rank")),
                    "order_number": self._safe_int(item.get("OrderNumber")),
                }
                data["codelist_items"].append(item_data)

    def _process_methods(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process MethodDef elements"""
        for method in parent.findall(".//odm:MethodDef", self.NAMESPACES):
            method_id = self._get_next_id("methods")
            method_oid = method.get("OID")

            self._oid_to_id_maps["methods"][f"{version_id}:{method_oid}"] = method_id

            desc_elem = method.find(".//odm:Description/odm:TranslatedText", self.NAMESPACES)

            method_data = {
                "method_id": method_id,
                "version_id": version_id,
                "method_oid": method_oid,
                "method_name": method.get("Name"),
                "method_type": method.get("Type"),
                "description": desc_elem.text if desc_elem is not None else None,
            }
            data["methods"].append(method_data)

    def _process_value_lists(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process ValueListDef elements"""
        for vl in parent.findall(".//def:ValueListDef", self.NAMESPACES):
            value_list_id = self._get_next_id("value_lists")
            value_list_oid = vl.get("OID")

            self._oid_to_id_maps["value_lists"][f"{version_id}:{value_list_oid}"] = value_list_id

            value_list_data = {
                "value_list_id": value_list_id,
                "version_id": version_id,
                "value_list_oid": value_list_oid,
            }
            data["value_lists"].append(value_list_data)

            for item_ref in vl.findall(".//odm:ItemRef", self.NAMESPACES):
                item_data = {
                    "value_item_id": self._get_next_id("value_list_items"),
                    "value_list_id": value_list_id,
                    "item_oid": item_ref.get("ItemOID"),
                    "order_number": self._safe_int(item_ref.get("OrderNumber")),
                    "mandatory": item_ref.get("Mandatory") == "Yes",
                    "method_oid": item_ref.get("MethodOID"),
                    "where_clause_oid": self._get_where_clause_ref(item_ref),
                }
                data["value_list_items"].append(item_data)

    def _process_where_clauses(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process WhereClauseDef elements"""
        for wc in parent.findall(".//def:WhereClauseDef", self.NAMESPACES):
            where_clause_id = self._get_next_id("where_clauses")
            where_clause_oid = wc.get("OID")

            self._oid_to_id_maps["where_clauses"][f"{version_id}:{where_clause_oid}"] = where_clause_id

            where_clause_data = {
                "where_clause_id": where_clause_id,
                "version_id": version_id,
                "where_clause_oid": where_clause_oid,
            }
            data["where_clauses"].append(where_clause_data)

            for range_check in wc.findall(".//odm:RangeCheck", self.NAMESPACES):
                check_value_elem = range_check.find(".//odm:CheckValue", self.NAMESPACES)

                condition_data = {
                    "condition_id": self._get_next_id("where_clause_conditions"),
                    "where_clause_id": where_clause_id,
                    "item_oid": range_check.get("{%s}ItemOID" % self.NAMESPACES["def"]),
                    "comparator": range_check.get("Comparator"),
                    "check_value": check_value_elem.text if check_value_elem is not None else None,
                    "soft_hard": range_check.get("SoftHard"),
                }
                data["where_clause_conditions"].append(condition_data)

    def _process_comments(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process CommentDef elements"""
        for comment in parent.findall(".//def:CommentDef", self.NAMESPACES):
            comment_id = self._get_next_id("comments")
            comment_oid = comment.get("OID")

            self._oid_to_id_maps["comments"][f"{version_id}:{comment_oid}"] = comment_id

            desc_elem = comment.find(".//odm:Description/odm:TranslatedText", self.NAMESPACES)

            comment_data = {
                "comment_id": comment_id,
                "version_id": version_id,
                "comment_oid": comment_oid,
                "comment_text": desc_elem.text if desc_elem is not None else None,
            }
            data["comments"].append(comment_data)

    def _process_documents(self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]) -> None:
        """Process leaf elements (documents)"""
        for leaf in parent.findall(".//def:leaf", self.NAMESPACES):
            document_id = self._get_next_id("documents")
            leaf_id = leaf.get("ID")

            self._oid_to_id_maps["documents"][f"{version_id}:{leaf_id}"] = document_id

            title_elem = leaf.find(".//def:title", self.NAMESPACES)

            document_data = {
                "document_id": document_id,
                "version_id": version_id,
                "leaf_id": leaf_id,
                "href": leaf.get("{%s}href" % self.NAMESPACES["xlink"]),
                "title": title_elem.text if title_elem is not None else None,
            }
            data["documents"].append(document_data)

    def _process_analysis_results(
        self, parent: ET.Element, version_id: int, data: Dict[str, Union[List, Dict]]
    ) -> None:
        """Process AnalysisResult elements (ADaM specific)"""
        for result in parent.findall(".//arm:AnalysisResult", self.NAMESPACES):
            result_id = self._get_next_id("analysis_results")

            desc_elem = result.find(".//odm:Description/odm:TranslatedText", self.NAMESPACES)
            doc_elem = result.find(".//arm:Documentation/odm:Description/odm:TranslatedText", self.NAMESPACES)
            code_elem = result.find(".//arm:ProgrammingCode/arm:Code", self.NAMESPACES)

            result_data = {
                "result_id": result_id,
                "version_id": version_id,
                "result_oid": result.get("OID"),
                "display_oid": result.getparent().get("OID") if result.getparent() is not None else None,
                "result_identifier": result.get("ResultIdentifier"),
                "parameter_oid": result.get("ParameterOID"),
                "analysis_reason": result.get("AnalysisReason"),
                "analysis_purpose": result.get("AnalysisPurpose"),
                "description": desc_elem.text if desc_elem is not None else None,
                "documentation": doc_elem.text if doc_elem is not None else None,
                "programming_code": code_elem.text if code_elem is not None else None,
                "programming_context": (
                    result.find(".//arm:ProgrammingCode", self.NAMESPACES).get("Context")
                    if result.find(".//arm:ProgrammingCode", self.NAMESPACES) is not None
                    else None
                ),
            }
            data["analysis_results"].append(result_data)

    def _get_text(self, parent: Optional[ET.Element], path: str) -> Optional[str]:
        """Get text from element path"""
        if parent is None:
            return None
        elem = parent.find(path, self.NAMESPACES)
        return elem.text if elem is not None else None

    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """Convert string to int safely"""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string"""
        if not dt_str:
            return None
        try:
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(dt_str, fmt)
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    def _add_variable_codelist_ref(
        self, variable_id: int, codelist_oid: str, version_id: int, data: Dict[str, Union[List, Dict]]
    ) -> None:
        """Add variable-codelist reference"""
        codelist_id = self._oid_to_id_maps["codelists"].get(f"{version_id}:{codelist_oid}")
        if codelist_id:
            ref_data = {
                "ref_id": self._get_next_id("variable_codelist_refs"),
                "variable_id": variable_id,
                "codelist_id": codelist_id,
            }
            data["variable_codelist_refs"].append(ref_data)

    def _add_variable_value_list_ref(
        self, variable_id: int, value_list_oid: str, version_id: int, data: Dict[str, Union[List, Dict]]
    ) -> None:
        """Add variable-value list reference"""
        value_list_id = self._oid_to_id_maps["value_lists"].get(f"{version_id}:{value_list_oid}")
        if value_list_id:
            ref_data = {
                "ref_id": self._get_next_id("variable_value_lists"),
                "variable_id": variable_id,
                "value_list_id": value_list_id,
            }
            data["variable_value_lists"].append(ref_data)

    def _get_where_clause_ref(self, item_ref: ET.Element) -> Optional[str]:
        """Get where clause OID from ItemRef"""
        wc_ref = item_ref.find(".//def:WhereClauseRef", self.NAMESPACES)
        return wc_ref.get("WhereClauseOID") if wc_ref is not None else None
