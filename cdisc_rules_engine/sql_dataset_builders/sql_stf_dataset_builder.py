import os
import json
from xml.etree import ElementTree

from cdisc_rules_engine.enums.dataformat_types import DataFormatTypes
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services.data_readers.data_reader_factory import DataReaderFactory
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder

STF_METADATA_TYPE = {
    "study_id": "Char",
    "study_title": "Char",
    "dtd_version": "Char",
    "language": "Char",
    "categories_summary": "Char",
    "doc_index": "Num",
    "document_title": "Char",
    "document_href": "Char",
    "document_href_path": "Char",
    "document_href_fragment": "Char",
    "document_properties_json": "Char",
    "document_properties_summary": "Char",
    "document_operation": "Char",
    "file_tag_name": "Char",
    "file_tag_info_type": "Char",
}


class SqlSTFDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for STF metadata checks.
    Creates one row per STF document/file-tag so file tags and operations are easy to query.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_stf_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        stf_file_path = self._resolve_stf_file_path()
        stf_contents = self.data_service.get_define_xml_contents(dataset_name=stf_file_path)

        reader = DataReaderFactory().get_service(DataFormatTypes.STF.value)
        stf_metadata = reader.read(stf_contents)
        rows = self._build_rows(stf_metadata, stf_file_path)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, col_type in STF_METADATA_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, col_type))

        self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name

    def _resolve_stf_file_path(self) -> str:
        stf_file_path = getattr(self, "stf_file_path", None)
        if stf_file_path:
            return stf_file_path

        service_level_stf = getattr(self.data_service, "stf_file_path", None)
        if service_level_stf:
            return service_level_stf

        raise ValueError("No STF file path provided. Set stf_file_path or define_xml_path.")

    def _build_rows(self, stf_metadata: dict, stf_file_path: str) -> list[dict]:
        categories_summary = "|".join(
            f"{category.get('name')}:{category.get('value')}"
            for category in stf_metadata.get("categories", [])
            if category.get("name")
        )

        rows = []
        documents = stf_metadata.get("documents", [])
        for doc_index, document in enumerate(documents, start=1):
            href = document.get("href")
            href_path, href_fragment = self._split_href(href)
            operation = self._resolve_document_operation(stf_file_path, href_path, href_fragment)
            properties_json, properties_summary = self._serialize_document_properties(document.get("properties", []))

            file_tags = document.get("file_tags", [])
            if not file_tags:
                file_tags = [{"name": None, "info_type": None}]

            for file_tag in file_tags:
                rows.append(
                    {
                        "study_id": stf_metadata.get("study_id"),
                        "study_title": stf_metadata.get("study_title"),
                        "dtd_version": stf_metadata.get("dtd_version"),
                        "language": stf_metadata.get("language"),
                        "categories_summary": categories_summary or None,
                        "doc_index": doc_index,
                        "document_title": document.get("title"),
                        "document_href": href,
                        "document_href_path": href_path,
                        "document_href_fragment": href_fragment,
                        "document_properties_json": properties_json,
                        "document_properties_summary": properties_summary,
                        "document_operation": operation,
                        "file_tag_name": file_tag.get("name"),
                        "file_tag_info_type": file_tag.get("info_type"),
                    }
                )

        return rows

    @staticmethod
    def _split_href(href: str) -> tuple[str | None, str | None]:
        if not href:
            return None, None
        if "#" not in href:
            return href, None
        href_path, href_fragment = href.split("#", 1)
        return href_path, href_fragment

    @staticmethod
    def _serialize_document_properties(properties: list[dict]) -> tuple[str | None, str | None]:
        if not properties:
            return None, None

        normalized = [
            {
                "name": prop.get("name"),
                "info_type": prop.get("info_type"),
                "value": prop.get("value"),
            }
            for prop in properties
        ]

        json_payload = json.dumps(normalized, separators=(",", ":"), ensure_ascii=True, sort_keys=True)

        summary_tokens = []
        for prop in normalized:
            name = prop.get("name") or ""
            info_type = prop.get("info_type") or ""
            value = prop.get("value") or ""
            summary_tokens.append(f"{name}@{info_type}={value}")

        summary_payload = "|".join(summary_tokens) if summary_tokens else None
        return json_payload, summary_payload

    def _resolve_document_operation(
        self,
        stf_file_path: str,
        href_path: str | None,
        href_fragment: str | None,
    ) -> str | None:
        if not href_path or not href_fragment:
            return None

        index_path = os.path.normpath(os.path.join(os.path.dirname(stf_file_path), href_path))
        if not os.path.exists(index_path):
            return None

        try:
            root = ElementTree.parse(index_path).getroot()
        except ElementTree.ParseError:
            return None

        for element in root.iter():
            if element.tag.split("}")[-1] == "leaf" and element.get("ID") == href_fragment:
                return element.get("operation")
        return None
