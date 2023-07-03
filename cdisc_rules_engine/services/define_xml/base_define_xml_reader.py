from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import List, Optional
from functools import cache


from odmlib.define_loader import XMLDefineLoader
from odmlib.loader import ODMLoader

# Import these models to accommodate the inline import found here:
# https://github.com/swhume/odmlib/blob/master/odmlib/define_loader.py#L12
import odmlib.define_2_1.model  # noqa F401
import odmlib.define_2_0.model  # noqa F401

from cdisc_rules_engine.exceptions.custom_exceptions import (
    DomainNotFoundInDefineXMLError,
)
from cdisc_rules_engine.models.define import ValueLevelMetadata
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.decorators import cached


@dataclass
class DefineXMLVersion:
    version: str
    namespace: str
    model_package: str


class BaseDefineXMLReader(ABC):
    """
    This class is responsible for extracting
    metadata from a define XML file.
    Uses odmlib library under the hood and
    represents a facade over the library.
    """

    @staticmethod
    @abstractmethod
    def class_define_xml_version() -> DefineXMLVersion:
        pass

    @staticmethod
    @abstractmethod
    def _meta_data_schema() -> type:
        pass

    def __init__(
        self,
        cache_service_obj=None,
        study_id=None,
        data_bundle_id=None,
        validate_xml=None,
    ):
        self._odm_loader = ODMLoader(
            XMLDefineLoader(
                model_package=self.class_define_xml_version().model_package,
                ns_uri=self.class_define_xml_version().namespace,
            )
        )
        self.cache_service = cache_service_obj
        self.study_id = study_id
        self.data_bundle_id = data_bundle_id
        self.validate_xml = validate_xml

    def read(self, **kwargs) -> List[dict]:
        """
        Reads Define XML metadata and returns it as a list of dicts.
        The output contains metadata for all datasets.
        """
        metadata = self.get_metadata_version(**kwargs)
        output = []
        for domain_metadata in metadata.ItemGroupDef:
            output.append(self._get_metadata_representation(domain_metadata))
        return output

    def is_validate_xml(self):
        return True if self.validate_xml in ("Y", "YES") else False

    @cached("define-domain-metadata")
    def extract_domain_metadata(self, domain_name: str = None) -> dict:
        """
        Reads Define XML metadata and filters by domain name.
        """
        logger.info(
            f"Extracting domain metadata from Define-XML. domain_name={domain_name}"
        )
        metadata = self._odm_loader.MetaDataVersion()
        item_mapping = {item.OID: item for item in metadata.ItemDef}
        domain_metadata = self._get_domain_metadata(metadata, domain_name)
        domain_metadata_dict: dict = self._get_metadata_representation(domain_metadata)
        domain_metadata_dict["define_dataset_variables"] = [
            item_mapping.get(item.ItemOID).Name
            for item in domain_metadata.ItemRef
            if item.ItemOID in item_mapping
        ]
        logger.info(f"Extracted domain metadata = {domain_metadata_dict}")
        return domain_metadata_dict

    @cached("define-variables-metadata")
    def extract_variables_metadata(self, domain_name: str = None) -> List[dict]:
        logger.info(
            f"Extracting variables metadata from Define-XML. domain_name={domain_name}"
        )
        domain_metadata = None
        variables_metadata = []
        metadata = self.get_metadata_version()
        domain_metadata = self._get_domain_metadata(metadata, domain_name)
        codelist_map = self._get_codelist_def_map(metadata.CodeList)
        for itemref in domain_metadata.ItemRef:
            itemdef = [item for item in metadata.ItemDef if item.OID == itemref.ItemOID]
            if itemdef:
                variables_metadata.append(
                    self._get_item_def_representation(itemdef[0], itemref, codelist_map)
                )
        logger.info(f"Extracted variables metadata = {variables_metadata}")

        return variables_metadata

    def get_metadata_version(self):
        metadata = None
        try:
            metadata = self._odm_loader.MetaDataVersion()
            logger.info(f"Define Metadata Version: {metadata}")
        except Exception as e:
            logger.trace(e, __name__)
            logger.error(f"{__name__}(VX={self.validate_xml})")
            if self.is_validate_xml:
                logger.info(
                    f"Validate XML = {self.validate_xml}: continue to next step."
                )
        return metadata

    @cached("define-value-level-metadata")
    def extract_value_level_metadata(
        self, domain_name: str = None, **kwargs
    ) -> List[dict]:
        """
        Extracts all value level metadata for each variable in a given domain.
        Returns: A list of dictionaries containing value level metadata corresponding
         to the given domain.
        ex:
        [
            {
                filter: <function to filter dataframe to rows that the value
                 level metadata applies to>
                type_check: <function to check the type of the target variable
                 matches the type of the value level metadata
                length_check: <function to check whether the length of the target
                 variables value matches the length specified in the vlm
            }...
        ]
        """
        logger.info(
            f"Extracting value level metadata from Define-XML. "
            f"domain_name={domain_name}"
        )
        metadata = self.get_metadata_version(**kwargs)
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
        Method for extracting codelists into a map for faster access
         when generating variable codelist metadata
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
            "define_variable_is_collected": "",
            "define_variable_role": "",
            "define_variable_size": "",
            "define_variable_ccode": "",
            "define_variable_format": "",
            "define_variable_allowed_terms": [],
            "define_variable_origin_type": "",
            "define_variable_has_no_data": "",
            "define_variable_order_number": None,
            "define_variable_has_codelist": False,
            "define_variable_codelist_coded_values": [],
        }
        if itemdef:
            data["define_variable_name"] = itemdef.Name
            data["define_variable_size"] = itemdef.Length
            data["define_variable_role"] = itemref.Role
            data["define_variable_data_type"] = self._get_variable_datatype(
                itemdef.DataType
            )
            data["define_variable_is_collected"] = self._get_variable_is_collected(
                itemdef
            )
            if itemdef.Description:
                data["define_variable_label"] = str(
                    itemdef.Description.TranslatedText[0]
                )
            if itemdef.CodeListRef:
                data["define_variable_has_codelist"] = True
                oid = itemdef.CodeListRef.CodeListOID
                codelist = codelists.get(oid)
                data["define_variable_ccode"] = self._get_codelist_ccode(codelist)
                data["define_variable_allowed_terms"].extend(
                    self._get_codelist_allowed_terms(codelist)
                )
                data["define_variable_codelist_coded_values"].extend(
                    self._get_codelist_coded_values(codelist)
                )
            if itemdef.Origin:
                data["define_variable_origin_type"] = self._get_origin_type(itemdef)
            data["define_variable_has_no_data"] = getattr(itemref, "HasNoData", "")
            data["define_variable_order_number"] = itemref.OrderNumber

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

    def _get_codelist_coded_values(self, codelist):
        if codelist:
            for codelist_item in codelist.CodeListItem + codelist.EnumeratedItem:
                yield codelist_item.CodedValue

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

    @abstractmethod
    def _get_origin_type(self, itemdef):
        pass

    @abstractmethod
    def _get_variable_is_collected(self, itemdef):
        pass

    def _get_metadata_representation(self, metadata) -> dict:
        """
        Returns metadata as dictionary.
        """
        return {
            "define_dataset_name": metadata.Domain,
            "define_dataset_label": str(metadata.Description.TranslatedText[0]),
            "define_dataset_location": getattr(metadata.leaf, "href", None),
            "define_dataset_class": str(metadata.Class.Name),
            "define_dataset_structure": str(metadata.Structure),
            "define_dataset_is_non_standard": str(metadata.IsNonStandard or ""),
        }

    def validate_schema(self) -> bool:
        """
        Validates Define XML Schema.
        """
        logger.info("Validating Define-XML schema.")
        schema_validator = self._meta_data_schema()()
        study = self._odm_loader.Study()
        is_valid: bool = schema_validator.check_conformance(study.to_dict(), "Study")
        logger.info(f"Validated Define-XML schema. is_valid={is_valid}")
        return is_valid

    @cache
    def get_define_version(self, **kwargs) -> Optional[str]:
        """Use to extract DefineVersion from file"""
        self.read(**kwargs)
        mdv_attrib: dict = self._odm_loader.loader.parser.mdv[0].attrib
        for key, val in mdv_attrib.items():
            if key.endswith("DefineVersion"):
                return val
        return None
