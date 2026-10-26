"""
Study-level metadata cache: define.xml, library and friends :).
Read once and shared by every rule.
"""

import os
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Tuple

from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundInDefineXMLError
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import BaseDefineXMLReader, DefineXMLReaderFactory
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
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
    "define_variable_has_codelist": "Bool",
    "define_variable_codelist_coded_values": "Char",
    "define_variable_codelist_coded_codes": "Char",
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


class MetadataStore:

    def __init__(self, data_service, standards_context: BaseStandardsContext):
        self.data_service = data_service
        self.standards_context = standards_context
        self._cache: Dict[tuple, object] = {}

    @classmethod
    def of(cls, data_service, standards_context: BaseStandardsContext) -> "MetadataStore":
        """
        Get the store for whatever study the data service currently holds.
        The key is a cheap fingerprint of the study.
        If anything in it changes we start a fresh store rather than risk serving the previous study's define.
        """
        key = (
            tuple(sorted(dataset.name for dataset in data_service.datasets)),
            getattr(data_service, "define_xml_path", None),
            getattr(data_service, "stf_file_path", None),
            data_service.pgi.sql_namespace,
            id(standards_context),
        )
        store = getattr(data_service, "_metadata_store", None)
        if store is None or store._key != key:
            store = cls(data_service, standards_context)
            store._key = key
            data_service._metadata_store = store
        return store

    def _cached(self, key: tuple, compute):
        # deepcopy on the way out
        # A caller mutating its result can't corrupt the cached reading for the next rule
        if key not in self._cache:
            self._cache[key] = compute()
        return deepcopy(self._cache[key])

    @property
    def datasets(self) -> List[BaseDatasetMetadata]:
        return [
            self.data_service.get_dataset_metadata(dataset_id)
            for dataset_id in self.data_service.get_uploaded_dataset_ids()
        ]

    def _define_reader(self) -> BaseDefineXMLReader:
        # TODO: could cache the reader too? Cheap compared to the extraction calls, so not bothered yet.
        return DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )

    def define_variables(self, dataset: BaseDatasetMetadata) -> List[dict]:
        """The define.xml variables of a dataset (of its parent domain for a SUPP/relationship dataset)."""
        return self._cached(("define_variables", dataset.name), lambda: self._read_define_variables(dataset))

    @staticmethod
    def _define_domain(dataset: BaseDatasetMetadata) -> str:
        """The domain a dataset is defined under: its parent's for a SUPP/relationship dataset."""
        return getattr(dataset, "rdomain", None) or dataset.domain

    @staticmethod
    def _define_names(dataset: BaseDatasetMetadata) -> List[str]:
        """
        Names the dataset may have in the define. A split dataset (e.g. QS) is defined under the
        names of its parts (QSCG, QSMM...), so each part is tried in turn.
        """
        split_parts = getattr(dataset, "split_part_filenames", None) or []
        names = [os.path.splitext(os.path.basename(part))[0].upper() for part in split_parts]
        return names or [dataset.name.upper()]

    def _extract_from_define(self, dataset: BaseDatasetMetadata, extract: Callable[[str], Any]) -> Tuple[str, Any]:
        """
        extract() under each of the dataset's define names; the first name found, with its result.
        Raises DomainNotFoundInDefineXMLError if the define describes the dataset under none of them.
        """
        error = None
        for name in self._define_names(dataset):
            try:
                return name, extract(name)
            except DomainNotFoundInDefineXMLError as e:
                error = e
        raise error

    def _read_define_variables(self, dataset: BaseDatasetMetadata) -> List[dict]:
        reader = self._define_reader()
        domain = self._define_domain(dataset)
        _, metadata = self._extract_from_define(
            dataset, lambda name: reader.extract_variables_metadata(domain_name=domain, name=name)
        )
        return [format_metadata_dict(dict(variable)) for variable in deepcopy(metadata)]

    def define_dataset(self, dataset: BaseDatasetMetadata) -> dict:
        """The define.xml dataset metadata of one dataset, or {} when the define doesn't describe it."""
        return self._cached(("define_dataset", dataset.name), lambda: self._read_define_dataset(dataset))

    def _read_define_dataset(self, dataset: BaseDatasetMetadata) -> dict:
        define_reader = self._define_reader()
        try:
            define_name, metadata = self._extract_from_define(dataset, define_reader.extract_dataset_metadata)
        except DomainNotFoundInDefineXMLError:
            return {}
        metadata = format_metadata_dict(dict(deepcopy(metadata)))
        if "define_dataset_variable_order" not in metadata.keys():
            metadata["define_dataset_variable_order"] = define_dataset_variable_order(
                define_reader, domain=self._define_domain(dataset), name=define_name
            )
        return metadata

    def define_all_datasets(self) -> Dict[str, dict]:
        """The define.xml dataset metadata of every uploaded dataset's domain by domain."""
        return self._cached(("define_all_datasets",), self._read_define_all_datasets)

    def _read_define_all_datasets(self) -> Dict[str, dict]:
        define_reader = self._define_reader()
        define_ds_metadata = {}
        for dataset in self.datasets:
            try:
                metadata = format_metadata_dict(deepcopy(define_reader.extract_dataset_metadata(dataset.domain)))
                if "define_dataset_variable_order" not in metadata.keys():
                    metadata["define_dataset_variable_order"] = define_dataset_variable_order(
                        define_reader, domain=dataset.domain
                    )
                define_ds_metadata[dataset.domain] = metadata
            except DomainNotFoundInDefineXMLError:
                continue
        return define_ds_metadata

    def define_datasets(self) -> List[dict]:
        return self._cached(("define_datasets",), lambda: self._define_reader().read())

    def define_vlms(self, dataset: BaseDatasetMetadata) -> List[dict]:
        return self._cached(("define_vlms", dataset.name), lambda: self._read_define_vlms(dataset))

    def _read_define_vlms(self, dataset: BaseDatasetMetadata) -> List[dict]:
        reader = self._define_reader()
        domain = self._define_domain(dataset)
        try:
            _, metadata = self._extract_from_define(
                dataset, lambda name: reader.extract_value_level_metadata(domain_name=domain, name=name)
            )
        except DomainNotFoundInDefineXMLError as e:
            logger.warning(f"No value level metadata for dataset '{dataset.name}': {e}")
            return []

        vlms = []
        for vlm in deepcopy(metadata):
            # where_clause is a list of range checks - pulled out first so format_metadata_dict
            # doesn't flatten it into a comma-joined string
            where_clause = vlm.pop("where_clause", [])
            # the reader's callables, not metadata
            for key in ["filter", "type_check", "length_check"]:
                vlm.pop(key, None)
            vlm = format_metadata_dict(vlm)
            vlm["where_clause"] = where_clause
            vlms.append(vlm)
        return vlms

    def library_variables(self, dataset: BaseDatasetMetadata) -> List[dict]:
        """The library's variables for a dataset."""
        return self._cached(("library_variables", dataset.name), lambda: self._read_library_variables(dataset))

    def _read_library_variables(self, dataset: BaseDatasetMetadata) -> List[dict]:
        library_metadata = self.standards_context.get_library_variables_metadata(dataset)
        return [format_metadata_dict(filter_library_vars_dict(variable)) for variable in deepcopy(library_metadata)]

    def record_count(self, table_id: str) -> int:
        # not cached - it's a single COUNT(*)
        table_hash = self.data_service.pgi.schema.get_table_hash(table_id)
        self.data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {table_hash};")
        result = self.data_service.pgi.fetch_all()
        return result[0]["count"] if result else 0


def define_dataset_variable_order(reader: BaseDefineXMLReader, domain: str, name: Optional[str] = None) -> str:
    metadata = reader.extract_variables_metadata(domain, name)
    vars_order = {var["define_variable_name"]: var["define_variable_order_number"] for var in metadata}
    sorted_vars = sorted(vars_order.items(), key=lambda item: item[1])
    return ",".join([var[0].upper() for var in sorted_vars])


def filter_library_vars_dict(library_var: dict) -> dict:
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


def format_metadata_dict(metadata: dict) -> dict:
    """In place: "" -> None, lists -> comma-joined strings."""
    for k, v in metadata.items():
        if v == "":
            metadata[k] = None
        elif isinstance(v, list):
            metadata[k] = ",".join(map(str, v))
    return metadata
