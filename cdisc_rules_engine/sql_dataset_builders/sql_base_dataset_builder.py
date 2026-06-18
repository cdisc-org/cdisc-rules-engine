from abc import ABC, abstractmethod
from typing import Dict, List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundInDefineXMLError
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory, BaseDefineXMLReader
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext

LIBRARY_VARIABLES_TYPE = {
    "library_variable_name": "Char",
    "library_variable_label": "Char",
    "library_variable_data_type": "Char",
    "library_variable_role": "Char",
    "library_variable_core": "Char",
    "library_variable_ccode": "Char",
    "library_variable_order_number": "Num",
}

DEFINE_VARIABLES_TYPE = {
    "define_variable_name": "Char",
    "define_variable_label": "Char",
    "define_variable_data_type": "Char",
    "define_variable_is_collected": "Bool",
    "define_variable_role": "Char",
    "define_variable_length": "Num",
    "define_variable_ccode": "Char",
    "define_variable_format": "Char",
    "define_variable_allowed_terms": "Char",
    "define_variable_origin_type": "Char",
    "define_variable_source_type": "Char",
    "define_variable_has_no_data": "Bool",
    "define_variable_order_number": "Num",
    "define_variable_length": "Num",
    "define_variable_has_codelist": "Bool",
    "define_variable_codelist_coded_values": "Char",
    "define_variable_mandatory": "Bool",
    "define_variable_has_comment": "Bool",
}
DEFINE_DATASETS_TYPE = {
    "define_dataset_name": "Char",
    "define_dataset_label": "Char",
    "define_dataset_location": "Char",
    "define_dataset_domain": "Char",
    "define_dataset_class": "Char",
    "define_dataset_structure": "Char",
    "define_dataset_is_non_standard": "Char",
    "define_dataset_has_no_data": "Bool",
    "define_dataset_variables": "Char",
    "define_dataset_key_sequence": "Char",
    "define_dataset_variable_order": "Char",
}
DEFINE_VLM_TYPE = {
    "define_vlm_name": "Char",
    "define_vlm_label": "Char",
    "define_vlm_data_type": "Char",
    "define_vlm_is_collected": "Bool",
    "define_vlm_role": "Char",
    "define_vlm_size": "Num",
    "define_vlm_ccode": "Char",
    "define_vlm_format": "Char",
    "define_vlm_allowed_terms": "Char",
    "define_vlm_origin_type": "Char",
    "define_vlm_has_no_data": "Bool",
    "define_vlm_order_number": "Num",
    "define_vlm_length": "Num",
    "define_vlm_has_codelist": "Bool",
    "define_vlm_codelist_coded_values": "Char",
    "define_vlm_mandatory": "Bool",
    "define_vlm_has_comment": "Bool",
}


class SqlBaseDatasetBuilder(ABC):
    """
    Base class for SQL dataset builders.
    """

    def __init__(
        self,
        rule: dict,
        data_service: PostgresQLDataService,
        dataset_metadata: BaseDatasetMetadata,
        standards_context: BaseStandardsContext,
        datasets: List[BaseDatasetMetadata] = None,
        **kwargs,
    ):
        self.rule = rule
        self.data_service = data_service
        self.dataset_metadata = dataset_metadata
        self.standards_context = standards_context
        self.datasets = datasets or []
        # Store any additional kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def build(self) -> str:
        """
        Build and return the table/view name for this rule type.

        For mini tables: just return the pre-existing table name.
        Regular builders return DatasetInterface, we return table name string.
        """
        pass

    def get_dataset_id(self) -> str:
        """
        Main entrypoint - equivalent to get_dataset() in regular builders.
        Returns the table/view name to validate against.
        """
        return self.build()

    def get_define_vars(self) -> List[dict]:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        domain = self.dataset_metadata.domain or self.dataset_metadata.name
        metadata = define_reader.extract_variables_metadata(domain)
        for i, var in enumerate(metadata):
            metadata[i] = self._format_metadata_dict(var)
        return metadata

    def get_define_dataset(self) -> dict:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        try:
            metadata = define_reader.extract_dataset_metadata(self.dataset_metadata.domain)
            metadata = self._format_metadata_dict(metadata)
            domain = self.standards_context.derive_rdomain(self.dataset_metadata.name)
            if "define_dataset_variable_order" not in metadata.keys():
                metadata["define_dataset_variable_order"] = self._get_define_dataset_variable_order(
                    reader=define_reader,
                    domain=domain,
                    name=self.dataset_metadata.name,
                )
        except DomainNotFoundInDefineXMLError:
            metadata = {}
        return metadata

    def get_define_all_datasets(self) -> Dict[str, dict]:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        all_ds_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        define_ds_metadata = {}

        for ds_metadata in all_ds_metadata:
            try:
                metadata = define_reader.extract_dataset_metadata(ds_metadata.domain)
                metadata = self._format_metadata_dict(metadata)
                if "define_dataset_variable_order" not in metadata.keys():
                    metadata["define_dataset_variable_order"] = self._get_define_dataset_variable_order(
                        reader=define_reader,
                        domain=ds_metadata.domain,
                    )
                define_ds_metadata[ds_metadata.domain] = metadata
            except DomainNotFoundInDefineXMLError:
                continue

        return define_ds_metadata

    def get_define_metadata(self):
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        return define_xml_reader.read()

    def get_define_vlms(self) -> List[dict]:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        metadata = define_reader.extract_value_level_metadata()
        for i, var in enumerate(metadata):
            metadata[i] = self._format_metadata_dict(var)
        return metadata

    def get_library_vars(self) -> List[dict]:
        library_metadata = self.standards_context.get_library_variables_metadata(self.dataset_metadata)
        for i, var in enumerate(library_metadata):
            library_metadata[i] = self._filter_library_vars_dict(var)
            library_metadata[i] = self._format_metadata_dict(library_metadata[i])
        return library_metadata

    def _get_define_dataset_variable_order(self, reader: BaseDefineXMLReader, domain: str, name: str = None) -> str:
        metadata = reader.extract_variables_metadata(domain, name)
        vars_order = {var["define_variable_name"]: var["define_variable_order_number"] for var in metadata}
        sorted_vars = sorted(vars_order.items(), key=lambda item: item[1])
        return ",".join([var[0].upper() for var in sorted_vars])

    @staticmethod
    def _filter_library_vars_dict(library_var: dict) -> dict:
        new_var_dict = {}

        for key in library_var.keys():
            if f"library_variable_{key}" in LIBRARY_VARIABLES_TYPE.keys():
                new_var_dict[f"library_variable_{key}"] = library_var[key]
            elif key in LIBRARY_VARIABLES_TYPE.keys():
                new_var_dict[key] = library_var[key]

        codelist = library_var.get("_links", {}).get("codelist")
        if codelist:
            ccodes = set()
            for ccode in codelist:
                if ccode.get("href"):
                    ccodes.add(ccode["href"].split("/")[-1])
            new_var_dict["library_variable_ccode"] = ",".join(ccodes) if ccodes else None
        return new_var_dict

    @staticmethod
    def _format_metadata_dict(metadata: dict) -> dict:
        for k, v in metadata.items():
            if v == "":
                metadata[k] = None
            elif isinstance(v, list):
                metadata[k] = ",".join(map(str, v))
        return metadata

    def _has_empty_values(self, source_table_id: str, source_table_hash: str, var, table_is_empty: bool) -> bool:
        if table_is_empty or not var:
            return True

        var_name = var.name.upper()
        col_hash = self.data_service.pgi.schema.get_column_hash(source_table_id, var_name)
        if not col_hash:
            return True

        val_query = (
            f"SELECT COUNT(*) as cnt FROM {source_table_hash} "
            f"WHERE TRIM(COALESCE(CAST({col_hash} AS TEXT), '')) != ''"
        )
        self.data_service.pgi.execute_sql(val_query)
        val_res = self.data_service.pgi.fetch_one()

        return not (val_res and val_res["cnt"] > 0)

    def _value_count(self, source_table_id: str, source_table_hash: str, var) -> int:
        var_name = var.name.upper()
        col_hash = self.data_service.pgi.schema.get_column_hash(source_table_id, var_name)
        if not col_hash:
            return 0

        val_query = (
            f"SELECT COUNT(*) as cnt FROM {source_table_hash} "
            f"WHERE TRIM(COALESCE(CAST({col_hash} AS TEXT), '')) != ''"
        )
        self.data_service.pgi.execute_sql(val_query)
        val_res = self.data_service.pgi.fetch_one()

        return val_res["cnt"] if val_res and "cnt" in val_res else 0
