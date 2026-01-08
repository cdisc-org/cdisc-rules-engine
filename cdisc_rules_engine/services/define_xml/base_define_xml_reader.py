from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from functools import cache
from heapq import heappush, heappop

from odmlib.define_loader import XMLDefineLoader
from odmlib.loader import ODMLoader

# Import these models to accommodate the inline import found here:
# https://github.com/swhume/odmlib/blob/master/odmlib/define_loader.py#L12
import odmlib.define_2_1.model  # noqa F401
import odmlib.define_2_0.model  # noqa F401

from cdisc_rules_engine.exceptions.custom_exceptions import (
    DomainNotFoundInDefineXMLError,
    FailedSchemaValidation,
)
from cdisc_rules_engine.models.define import ValueLevelMetadata
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.decorators import cached
from cdisc_rules_engine.utilities.utils import is_supp_domain


@dataclass
class DefineXMLVersion:
    namespace: str
    model_package: str


@dataclass
class StandardsCTMetadata:
    name: str
    version: str
    oid: str

    type: str = None
    publishing_set: str = None


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

    @abstractmethod
    def get_extensible_codelist_mappings() -> Dict:
        pass

    def __init__(
        self,
        cache_service_obj=None,
        study_id=None,
        data_bundle_id=None,
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

    @cache
    def get_item_def_map(self) -> dict:
        metadata = self._odm_loader.MetaDataVersion()
        return {item.OID: item for item in metadata.ItemDef}

    @cache
    def get_value_list_def_map(self) -> dict:
        metadata = self._odm_loader.MetaDataVersion()
        return {value_list.OID: value_list for value_list in metadata.ValueListDef}

    def read(self) -> List[dict]:
        """
        Reads Define XML metadata and returns it as a list of dicts.
        The output contains metadata for all datasets.
        """
        try:
            metadata = self._odm_loader.MetaDataVersion()
            output = []
            for domain_metadata in metadata.ItemGroupDef:
                output.append(self._get_metadata_representation(domain_metadata))
            return output
        except Exception as e:
            raise FailedSchemaValidation(str(e))

    @cached("define-dataset-metadata")
    def extract_dataset_metadata(self, dataset_name: Union[str, None] = None) -> dict:
        """
        Reads Define XML metadata and filters by dataset name.
        """
        logger.info(
            f"Extracting dataset metadata from Define-XML. dataset_name={dataset_name}"
        )
        try:
            metadata = self._odm_loader.MetaDataVersion()
            item_mapping = {item.OID: item for item in metadata.ItemDef}
            dataset_metadata = self._get_dataset_metadata(metadata, dataset_name)
            dataset_metadata_dict: dict = self._get_metadata_representation(
                dataset_metadata
            )
            dataset_metadata_dict["define_dataset_variables"] = [
                item_mapping.get(item.ItemOID).Name
                for item in dataset_metadata.ItemRef
                if item.ItemOID in item_mapping
            ]
            dataset_metadata_dict["define_dataset_key_sequence"] = (
                self.get_dataset_key_sequence(dataset_name)
            )
        except ValueError as e:
            raise FailedSchemaValidation(str(e))
        logger.info(f"Extracted domain metadata = {dataset_metadata_dict}")
        return dataset_metadata_dict

    @cached("define-domain-metadata")
    def extract_domain_metadata(self, domain_name: str = None) -> dict:
        """
        Reads Define XML metadata and filters by domain name.
        """
        logger.info(
            f"Extracting domain metadata from Define-XML. domain_name={domain_name}"
        )
        try:
            metadata = self._odm_loader.MetaDataVersion()
            item_mapping = {item.OID: item for item in metadata.ItemDef}
            domain_metadata = self._get_domain_metadata(metadata, domain_name)
            domain_metadata_dict: dict = self._get_metadata_representation(
                domain_metadata
            )
            domain_metadata_dict["define_dataset_variables"] = [
                item_mapping.get(item.ItemOID).Name
                for item in domain_metadata.ItemRef
                if item.ItemOID in item_mapping
            ]
            domain_metadata_dict["define_dataset_key_sequence"] = (
                self.get_domain_key_sequence(domain_name)
            )
        except ValueError as e:
            raise FailedSchemaValidation(str(e))
        logger.info(f"Extracted domain metadata = {domain_metadata_dict}")
        return domain_metadata_dict

    @cached("define-variables-metadata")
    def extract_variables_metadata(
        self, domain_name: str = None, name: str = None
    ) -> List[dict]:
        logger.info(
            f"Extracting variables metadata from Define-XML. domain_name={domain_name}"
        )
        try:
            metadata = self._odm_loader.MetaDataVersion()
            domain_metadata = self._get_domain_metadata(metadata, domain_name, name)
            variables_metadata = []
            codelist_map = self._get_codelist_def_map(metadata.CodeList)
            for index, itemref in enumerate(domain_metadata.ItemRef):
                itemdef = [
                    item for item in metadata.ItemDef if item.OID == itemref.ItemOID
                ]
                if itemdef:
                    variables_metadata.append(
                        self._get_item_def_representation(
                            itemdef[0], itemref, codelist_map, index
                        )
                    )
        except ValueError as e:
            raise FailedSchemaValidation(str(e))
        logger.info(f"Extracted variables metadata = {variables_metadata}")
        return variables_metadata

    def extract_value_level_metadata(self, domain_name: str = None) -> List[dict]:
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
        try:
            metadata = self._odm_loader.MetaDataVersion()
            item_def_map = {item_def.OID: item_def for item_def in metadata.ItemDef}
            codelist_map = self._get_codelist_def_map(metadata.CodeList)
            domain_metadata = self._get_domain_metadata(metadata, domain_name)
            value_level_metadata_map = {}
            value_level_metadata = []

            for where_clause in metadata.WhereClauseDef:
                vlm = ValueLevelMetadata.from_where_clause_def(
                    where_clause, item_def_map
                )
                value_level_metadata_map[vlm.id] = vlm
            domain_variables = [
                item_def_map[item_ref.ItemOID] for item_ref in domain_metadata.ItemRef
            ]
            referenced_value_list_ids = [
                (item.Name, item.ValueListRef.ValueListOID)
                for item in domain_variables
                if item.ValueListRef
            ]

            value_lists = {
                value_list_def.OID: value_list_def
                for value_list_def in metadata.ValueListDef
            }
            for define_variable_name, value_list_id in referenced_value_list_ids:
                value_list = value_lists.get(value_list_id)
                for index, item_ref in enumerate(value_list.ItemRef):
                    vlm = value_level_metadata_map.get(
                        item_ref.WhereClauseRef[0].WhereClauseOID
                    )
                    item_data = self._get_item_def_representation(
                        item_def_map.get(item_ref.ItemOID),
                        item_ref,
                        codelist_map,
                        index,
                    )
                    # Replace all`define_variable_...` names with `define_vlm_...` names
                    item_data = {
                        k.replace("define_variable_", "define_vlm_"): v
                        for k, v in item_data.items()
                    }
                    if vlm:
                        item_data["define_variable_name"] = define_variable_name
                        item_data["filter"] = vlm.get_filter_function()
                        item_data["type_check"] = vlm.get_type_check_function()
                        item_data["length_check"] = vlm.get_length_check_function()
                        value_level_metadata.append(item_data)
        except ValueError as e:
            raise FailedSchemaValidation(str(e))
        logger.info(f"Extracted value level metadata = {value_level_metadata}")
        return value_level_metadata

    def _get_dataset_metadata(self, metadata, dataset_name):
        try:
            dataset_metadata = next(
                item for item in metadata.ItemGroupDef if item.Name == dataset_name
            )
            return dataset_metadata
        except StopIteration:
            raise DomainNotFoundInDefineXMLError(
                f"Dataset {dataset_name} is not found in Define XML"
            )

    def _get_domain_metadata(self, metadata, domain_name, name: str = None):
        try:
            if name:
                domain_metadata = next(
                    item
                    for item in metadata.ItemGroupDef
                    if item.Domain == domain_name and item.Name == name
                )
            else:
                domain_metadata = next(
                    item for item in metadata.ItemGroupDef if item.Domain == domain_name
                )
            return domain_metadata
        except StopIteration:
            raise DomainNotFoundInDefineXMLError(
                f"Domain {domain_name} is not found in Define XML"
            )

    def _get_all_dataset_and_supp_metadata(self, metadata, dataset_name):
        # Returns all itemgroupdefs such that one of the following conditions is met:
        # - dataset_name matches itemgroupdef name
        # - dataset's domain matches itemgroupdef domain and itemgroupdef is a supp
        # This will include SUPP-- datasets but NOT split datasets
        domain = next(
            (
                item.Domain
                for item in metadata.ItemGroupDef
                if item.Name == dataset_name
            ),
            None,
        )
        return [
            item
            for item in metadata.ItemGroupDef
            if item.Name == dataset_name
            or (
                (not is_supp_domain(dataset_name))
                and item.Domain == domain
                and is_supp_domain(item.Name)
            )
        ]

    def _get_all_domain_metadata(self, metadata, domain_name):
        # Returns all itemgroupdefs with domain = domain name.
        # This will include and SUPP-- datasets and split datasets
        # If the domain_name is SUPP-- the Domain value will not equal domain_name.
        # The additional check `item.Name == domain_name` is meant to account for this.
        return [
            item
            for item in metadata.ItemGroupDef
            if item.Domain == domain_name or item.Name == domain_name
        ]

    def _get_codelist_def_map(self, codelist_defs):
        """
        Method for extracting codelists into a map for faster access
         when generating variable codelist metadata
        """
        codelists = {}
        for codelist in codelist_defs:
            codelists[codelist.OID] = codelist
        return codelists

    def _get_order_number(self, itemref, index):
        if itemref.OrderNumber is not None:
            return itemref.OrderNumber
        else:
            return index + 1

    def _get_item_def_representation(self, itemdef, itemref, codelists, index) -> dict:
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
            "define_variable_length": None,
            "define_variable_has_codelist": False,
            "define_variable_codelist_coded_values": [],
            "define_variable_codelist_coded_codes": [],
            "define_variable_mandatory": None,
            "define_variable_has_comment": False,
        }
        if itemdef:
            data["define_variable_mandatory"] = itemref.Mandatory
            data["define_variable_name"] = itemdef.Name
            data["define_variable_size"] = itemdef.Length
            data["define_variable_role"] = itemref.Role
            data["define_variable_length"] = itemdef.Length
            data["define_variable_data_type"] = itemdef.DataType
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
                data["define_variable_codelist_coded_codes"].extend(
                    self._get_codelist_coded_codes(codelist)
                )
            if itemdef.Origin:
                data["define_variable_origin_type"] = self._get_origin_type(itemdef)
            data["define_variable_has_no_data"] = getattr(itemref, "HasNoData", "")
            data["define_variable_order_number"] = self._get_order_number(
                itemref, index
            )
            data["define_variable_has_comment"] = itemdef.CommentOID is not None
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

    def _get_codelist_coded_codes(self, codelist):
        if codelist:
            for codelist_item in codelist.CodeListItem + codelist.EnumeratedItem:
                if hasattr(codelist_item, "Alias") and codelist_item.Alias:
                    for alias in codelist_item.Alias:
                        if hasattr(alias, "Name"):
                            yield alias.Name

    @abstractmethod
    def _get_origin_type(self, itemdef):
        pass

    @abstractmethod
    def _get_variable_is_collected(self, itemdef):
        pass

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
    def get_define_version(self) -> Optional[str]:
        """Use to extract DefineVersion from file"""
        self.read()
        mdv_attrib: dict = self._odm_loader.loader.parser.mdv[0].attrib
        for key, val in mdv_attrib.items():
            if key.endswith("DefineVersion"):
                return val
        return None

    def get_dataset_key_sequence(self, dataset_name: str) -> List[str]:
        """Given a dataset name, this function returns the key sequence variable names specific to that dataset

        Key Sequence variable names are returned in order of KeySequence

        In the case that Supplemental qualifiers apply to a domain:

        1. If the dataset is not a supp dataset: add the VLM key variables to the list
        2. Otherwise handle the supp dataset as a normal dataset.

        Args:
            dataset_name: Name of the target dataset

        Returns:
            list: List of key variables ordered by key sequence attribute
        """
        try:
            heap = []
            existing_key_variables = set()
            metadata = self._odm_loader.MetaDataVersion()
            item_mapping = self.get_item_def_map()
            value_list_mapping = self.get_value_list_def_map()
            datasets = self._get_all_dataset_and_supp_metadata(metadata, dataset_name)
            for dataset_metadata in datasets:
                if (
                    is_supp_domain(dataset_metadata.Name)
                    and dataset_name != dataset_metadata.Name
                ):
                    # handle supp vlm
                    key_variables = self._get_key_variables_for_supp_dataset(
                        dataset_metadata, item_mapping, value_list_mapping
                    )
                    [
                        heappush(heap, key_variable)
                        for key_variable in key_variables
                        if key_variable[1] not in existing_key_variables
                    ]
                else:
                    key_variables = self._get_key_variables_for_domain(
                        dataset_metadata, item_mapping
                    )
                    [
                        heappush(heap, key_variable)
                        for key_variable in key_variables
                        if key_variable[1] not in existing_key_variables
                    ]
        except ValueError as e:
            raise FailedSchemaValidation(str(e))

        return [heappop(heap)[1] for _ in range(len(heap))]

    def get_domain_key_sequence(self, domain_name: str) -> List[str]:
        """Given a domain name, this function returns the key sequence variable names.

        Key Sequence variable names are returned in order of KeySequence

        In the case that Supplemental qualifiers apply to a domain:

        1. If the dataset is not a supp dataset: add the VLM key variables to the list
        2. Otherwise handle the supp dataset as a normal dataset.

        Args:
            domain_name: Name of the target domain/dataset

        Returns:
            list: List of key variables ordered by key sequence attribute
        """
        try:
            heap = []
            existing_key_variables = set()
            metadata = self._odm_loader.MetaDataVersion()
            item_mapping = self.get_item_def_map()
            value_list_mapping = self.get_value_list_def_map()
            datasets = self._get_all_domain_metadata(metadata, domain_name)
            for domain_metadata in datasets:
                if (
                    is_supp_domain(domain_metadata.Name)
                    and domain_name != domain_metadata.Name
                ):
                    # handle supp vlm
                    key_variables = self._get_key_variables_for_supp_dataset(
                        domain_metadata, item_mapping, value_list_mapping
                    )
                    [
                        heappush(heap, key_variable)
                        for key_variable in key_variables
                        if key_variable[1] not in existing_key_variables
                    ]
                else:
                    key_variables = self._get_key_variables_for_domain(
                        domain_metadata, item_mapping
                    )
                    [
                        heappush(heap, key_variable)
                        for key_variable in key_variables
                        if key_variable[1] not in existing_key_variables
                    ]
        except ValueError as e:
            raise FailedSchemaValidation(str(e))

        return [heappop(heap)[1] for _ in range(len(heap))]

    def get_external_dictionary_version(self, external_dictionary_type: str) -> str:
        metadata = self._odm_loader.MetaDataVersion()
        for codelist in metadata.CodeList:
            if codelist.ExternalCodeList and codelist.ExternalCodeList.Dictionary:
                if (
                    codelist.ExternalCodeList.Dictionary.lower()
                    == external_dictionary_type.lower()
                ):
                    return codelist.ExternalCodeList.Version
        return ""

    def _get_key_variables_for_domain(self, domain_metadata, item_mapping):
        key_variables = []
        for item in domain_metadata.ItemRef:
            if item.KeySequence:
                item_def = item_mapping.get(item.ItemOID)
                if item:
                    key_variables.append((item.KeySequence, item_def.Name))
        return key_variables

    def _get_key_variables_for_supp_dataset(
        self, domain_metadata, item_mapping, value_list_mapping
    ):
        key_variables = []
        for item in domain_metadata.ItemRef:
            item_def = item_mapping.get(item.ItemOID)
            if item_def and item_def.ValueListRef:
                value_list = value_list_mapping.get(item_def.ValueListRef.ValueListOID)
                if value_list:
                    key_variables.extend(
                        self._get_key_variables_from_valuelist(value_list, item_mapping)
                    )
        return key_variables

    def _get_key_variables_from_valuelist(self, value_list, item_mapping):
        key_variables = []
        for item_ref in value_list.ItemRef:
            if item_ref.KeySequence:
                key_item_def = item_mapping.get(item_ref.ItemOID)
                if key_item_def:
                    key_variables.append((item_ref.KeySequence, key_item_def.Name))
        return key_variables
