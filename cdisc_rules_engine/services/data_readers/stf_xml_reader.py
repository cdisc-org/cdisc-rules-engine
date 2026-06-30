from typing import Union
from xml.etree import ElementTree

from cdisc_rules_engine.interfaces import DataReaderInterface


class STFXMLReader(DataReaderInterface):
    XLINK_HREF = "{http://www.w3.org/1999/xlink}href"

    def read(self, data: Union[str, bytes]) -> dict:
        root = ElementTree.fromstring(data)
        return self._parse_root(root)

    def from_file(self, file_path: str) -> dict:
        root = ElementTree.parse(file_path).getroot()
        return self._parse_root(root)

    def _parse_root(self, root: ElementTree.Element) -> dict:
        study_identifier = root.find("study-identifier")
        study_document = root.find("study-document")

        categories = []
        documents = []

        if study_identifier is not None:
            for category in study_identifier.findall("category"):
                categories.append(
                    {
                        "name": category.get("name"),
                        "info_type": category.get("info-type"),
                        "value": (category.text or "").strip(),
                    }
                )

        if study_document is not None:
            # doc-content elements can appear nested within content-block structures.
            for doc_content in study_document.findall(".//doc-content"):
                file_tags = []
                properties = []

                for property_node in doc_content.findall("property"):
                    properties.append(
                        {
                            "name": property_node.get("name"),
                            "info_type": property_node.get("info-type"),
                            "value": (property_node.text or "").strip(),
                        }
                    )

                for file_tag in doc_content.findall("file-tag"):
                    file_tags.append(
                        {
                            "name": file_tag.get("name"),
                            "info_type": file_tag.get("info-type"),
                        }
                    )

                title_node = doc_content.find("title")
                documents.append(
                    {
                        "href": doc_content.get(self.XLINK_HREF),
                        "title": "" if title_node is None else (title_node.text or "").strip(),
                        "properties": properties,
                        "file_tags": file_tags,
                    }
                )

        title_node = None if study_identifier is None else study_identifier.find("title")
        study_id_node = None if study_identifier is None else study_identifier.find("study-id")

        return {
            "dtd_version": root.get("dtd-version"),
            "language": root.get("{http://www.w3.org/XML/1998/namespace}lang"),
            "study_title": "" if title_node is None else (title_node.text or "").strip(),
            "study_id": "" if study_id_node is None else (study_id_node.text or "").strip(),
            "categories": categories,
            "documents": documents,
        }
