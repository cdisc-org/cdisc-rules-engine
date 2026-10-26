"""
The STF file's documents, one row per document file tag.
This way, file tags and operations are easy to query.
"""

import json
import os
from xml.etree import ElementTree

from cdisc_rules_engine.enums.dataformat_types import DataFormatTypes
from cdisc_rules_engine.services.data_readers.data_reader_factory import DataReaderFactory

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


def stf_rows(data_service, stf_file_path: str) -> list[dict]:
    """The STF file's rows."""
    stf_contents = data_service.get_define_xml_contents(dataset_name=stf_file_path)
    reader = DataReaderFactory().get_service(DataFormatTypes.STF.value)
    return _build_rows(reader.read(stf_contents), stf_file_path)


def _build_rows(stf_metadata: dict, stf_file_path: str) -> list[dict]:
    categories_summary = "|".join(
        f"{category.get('name')}:{category.get('value')}"
        for category in stf_metadata.get("categories", [])
        if category.get("name")
    )

    rows = []
    documents = stf_metadata.get("documents", [])
    for doc_index, document in enumerate(documents, start=1):
        href = document.get("href")
        href_path, href_fragment = _split_href(href)
        operation = _resolve_document_operation(stf_file_path, href_path, href_fragment)
        properties_json, properties_summary = _serialize_document_properties(document.get("properties", []))

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


def _split_href(href: str) -> tuple[str | None, str | None]:
    if not href:
        return None, None
    if "#" not in href:
        return href, None
    href_path, href_fragment = href.split("#", 1)
    return href_path, href_fragment


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
