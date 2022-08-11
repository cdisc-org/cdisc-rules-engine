from typing import List, Union

from odmlib.define_2_1.rules.metadata_schema import MetadataSchema
from odmlib.define_loader import XMLDefineLoader
from odmlib.loader import ODMLoader

from cdisc_rules_engine.constants.define_xml_constants import (
    DEFINE_XML_MODEL_PACKAGE,
    DEFINE_XML_VERSION,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DomainNotFoundInDefineXMLError,
)
from cdisc_rules_engine.models.define import ValueLevelMetadata
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.decorators import cached


class DefineXMLReader:
    """
    This class is responsible for extracting
    metadata from a define XML file.
    Uses odmlib library under the hood and
    represents a facade over the library.

    The class has 2 constructors: from filename and
    from file contents.
    Ex. 1:
        filename = "define.xml"
        reader = DefineXMLReader.from_filename(filename)
        reader.read()

    Ex. 2:
        file_contents: bytes = b"...."
        reader = DefineXMLReader.from_file_contents(file_contents)
        reader.read()
    """

    @classmethod
    def from_filename(cls, filename: str):
        """
        Inits a DefineXMLReader object from file.
        """
        logger.info(f"Reading Define-XML from file name. filename={filename}")
        reader = cls()
        reader._odm_loader.open_odm_document(filename)
        return reader

    @classmethod
    def from_file_contents(
        cls,
        file_contents: Union[str, bytes],
        cache_service_obj=None,
        study_id=None,
        data_bundle_id=None,
    ):
        """
        Inits a DefineXMLReader object from file contents.
        """
        logger.info(f"Reading Define-XML from file contents")
        reader = cls(cache_service_obj, study_id, data_bundle_id)
        reader._odm_loader.load_odm_string(file_contents)
        return reader

    def __init__(self, cache_service_obj=None, study_id=None, data_bundle_id=None):
        self._odm_loader = ODMLoader(
            XMLDefineLoader(
                model_package=DEFINE_XML_MODEL_PACKAGE,
                ns_uri=f"http://www.cdisc.org/ns/def/v{DEFINE_XML_VERSION}",
            )
        )
        self.cache_service = cache_service_obj
        self.study_id = study_id
        self.data_bundle_id = data_bundle_id

    def read(self) -> List[dict]:
        """
        Reads Define XML metadata and returns it as a list of dicts.
        The output contains metadata for all datasets.
        """
        metadata = self._odm_loader.MetaDataVersion()
        output = []
        for domain_metadata in metadata.ItemGroupDef:
            output.append(self._get_metadata_representation(domain_metadata))
        return output

    @cached("define-domain-metadata")
    def extract_domain_metadata(self, domain_name: str = None) -> dict:
        """
        Reads Define XML metadata and filters by domain name.
        """
        logger.info(
            f"Extracting domain metadata from Define-XML. domain_name={domain_name}"
        )
        metadata = self._odm_loader.MetaDataVersion()
        domain_metadata = self._get_domain_metadata(metadata, domain_name)
        domain_metadata_dict: dict = self._get_metadata_representation(domain_metadata)
        logger.info(f"Extracted domain metadata = {domain_metadata_dict}")
        return domain_metadata_dict

    @cached("define-variables-metadata")
    def extract_variables_metadata(self, domain_name: str = None) -> List[dict]:
        logger.info(
            f"Extracting variables metadata from Define-XML. domain_name={domain_name}"
        )
        metadata = self._odm_loader.MetaDataVersion()
        domain_metadata = self._get_domain_metadata(metadata, domain_name)
        variables_metadata = []
        codelist_map = self._get_codelist_def_map(metadata.CodeList)
        for itemref in domain_metadata.ItemRef:
            itemdef = [item for item in metadata.ItemDef if item.OID == itemref.ItemOID]
            if itemdef:
                variables_metadata.append(
                    self._get_item_def_representation(itemdef[0], itemref, codelist_map)
                )
        logger.info(f"Extracted variables metadata = {variables_metadata}")
        return variables_metadata

    @cached("define-value-level-metadata")
    def extract_value_level_metadata(self, domain_name: str = None) -> List[dict]:
        """
        Extracts all value level metadata for each variable in a given domain.
        Returns: A list of dictionaries containing value level metadata corresponding to the given domain.
        ex:
        [
            {
                filter: <function to filter dataframe to rows that the value level metadata applies to>
                type_check: <function to check the type of the target variable matches the type of the value level metadata
                length_check: <function to check whether the length of the target variables value matches the length specified in the vlm
            }...
        ]
        """
        logger.info(
            f"Extracting value level metadata from Define-XML. domain_name={domain_name}"
        )
        metadata = self._odm_loader.MetaDataVersion()
        item_def_map = {item_def.OID: item_def for item_def in metadata.ItemDef}
        codelist_map = self._get_codelist_def_map(metadata.CodeList)
        domain_metadata = self._get_domain_metadata(metadata, domain_name)
        value_level_metadata_map = {}
        value_level_metadata = []
        for where_clause in metadata.WhereClauseDef:
            vlm = ValueLevelMetadata.from_where_clause_def(where_clause, item_def_map)
            value_level_metadata_map[vlm.id] = vlm
        domain_variables = [
            item_def_map[item_ref.ItemOID] for item_ref in domain_metadata.ItemRef
        ]
        referenced_value_list_ids = [
            item.ValueListRef.ValueListOID
            for item in domain_variables
            if item.ValueListRef
        ]
        value_lists = {
            value_list_def.OID: value_list_def
            for value_list_def in metadata.ValueListDef
        }
        for value_list_id in referenced_value_list_ids:
            value_list = value_lists.get(value_list_id)
            for item_ref in value_list.ItemRef:
                vlm = value_level_metadata_map.get(
                    item_ref.WhereClauseRef[0].WhereClauseOID
                )
                item_data = self._get_item_def_representation(
                    item_def_map.get(item_ref.ItemOID), item_ref, codelist_map
                )
                if vlm:
                    item_data["filter"] = vlm.get_filter_function()
                    item_data["type_check"] = vlm.get_type_check_function()
                    item_data["length_check"] = vlm.get_length_check_function()
                    value_level_metadata.append(item_data)
        logger.info(f"Extracted value level metadata = {value_level_metadata}")
        return value_level_metadata

    def _get_domain_metadata(self, metadata, domain_name):
        try:
            domain_metadata = next(
                item for item in metadata.ItemGroupDef if item.Domain == domain_name
            )
            return domain_metadata
        except StopIteration:
            raise DomainNotFoundInDefineXMLError(
                f"Domain {domain_name} is not found in Define XML"
            )

    def _get_codelist_def_map(self, codelist_defs):
        """
        Method for extracting codelists into a map for faster access when generating variable codelist metadata
        """
        codelists = {}
        for codelist in codelist_defs:
            codelists[codelist.OID] = codelist
        return codelists

    def _get_item_def_representation(self, itemdef, itemref, codelists) -> dict:
        """
        Returns item def as a dictionary
        """
        data = {
            "define_variable_name": "",
            "define_variable_label": "",
            "define_variable_data_type": "",
            "define_variable_role": "",
            "define_variable_size": "",
            "define_variable_ccode": "",
            "define_variable_format": "",
            "define_variable_allowed_terms": [],
            "define_variable_origin_type": "",
        }
        if itemdef:
            data["define_variable_name"] = itemdef.Name
            data["define_variable_size"] = itemdef.Length
            data["define_variable_role"] = itemref.Role
            data["define_variable_data_type"] = self._get_variable_datatype(
                itemdef.DataType
            )
            if itemdef.Description:
                data["define_variable_label"] = str(
                    itemdef.Description.TranslatedText[0]
                )
            if itemdef.CodeListRef:
                oid = itemdef.CodeListRef.CodeListOID
                data["define_variable_ccode"] = self._get_codelist_ccode(
                    codelists.get(oid)
                )
                data["define_variable_allowed_terms"].extend(
                    self._get_codelist_allowed_terms(codelists.get(oid))
                )
            if itemdef.Origin:
                data["define_variable_origin_type"] = itemdef.Origin[0].Type

        return data

    def _get_codelist_ccode(self, codelist):
        if codelist:
            ccode_ref = [
                alias for alias in codelist.Alias if alias.Context == "nci:ExtCodeID"
            ]
            if ccode_ref:
                return ccode_ref[0].Name
        return ""

    def _get_codelist_allowed_terms(self, codelist):
        if codelist:
            for codelist_item in codelist.CodeListItem:
                yield codelist_item.Decode.TranslatedText[0]._content

    def _get_variable_datatype(self, data_type):
        variable_type_map = {
            "text": "Char",
            "integer": "Num",
            "float": "Num",
            "datetime": "Char",
            "date": "Char",
            "time": "Char",
            "partialDate": "Char",
            "partialTime": "Char",
            "partialDatetime": "Char",
            "incompleteDatetime": "Char",
            "durationDatetime": "Char",
            "intervalDatetime": "Char",
        }
        return variable_type_map.get(data_type, data_type)

    def _get_metadata_representation(self, metadata) -> dict:
        """
        Returns metadata as dictionary.
        """
        return {
            "dataset_name": metadata.Domain,
            "dataset_label": str(metadata.Description.TranslatedText[0]),
            "dataset_location": getattr(metadata.leaf, "href", None),
            "dataset_class": str(metadata.Class.Name),
            "dataset_structure": str(metadata.Structure),
            "dataset_is_non_standard": str(metadata.IsNonStandard),
        }

    def validate_schema(self) -> bool:
        """
        Validates Define XML Schema.
        """
        logger.info(f"Validating Define-XML schema.")
        schema_validator = MetadataSchema()
        study = self._odm_loader.Study()
        is_valid: bool = schema_validator.check_conformance(study.to_dict(), "Study")
        logger.info(f"Validated Define-XML schema. is_valid={is_valid}")
        return is_valid
