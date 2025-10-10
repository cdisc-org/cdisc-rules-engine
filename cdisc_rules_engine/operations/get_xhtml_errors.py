import io
from typing import List, Optional, Tuple

import html5lib
from lxml import etree
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Steps:
      1. Make sure the column is a valid HTML -> on failure emit "Invalid HTML fragment:".
      2. Make sure the column is a valid XML -> on failure emit "Invalid XML fragment:".
      3. DTD validation for <usdm:ref> (attributes: attribute, id, klass) -> on failure emit "Invalid XML fragment:".

    Empty / None values return an empty list.
    """

    usdm_dtd = """\
    <!ELEMENT ref EMPTY>
    <!ATTLIST ref
        attribute CDATA #REQUIRED
        id        ID    #REQUIRED
        klass     CDATA #REQUIRED
    >
    """

    def _execute_operation(self):
        dataset = self.evaluation_dataset
        target = self.params.target
        if target not in dataset:
            error_list = [f"Target column '{target}' not found"]
            return dataset.get_series_from_value(error_list)
        return dataset[target].apply(self._ensure_dataset_is_valid_xhtml)

    def _ensure_dataset_is_valid_xhtml(self, value: str) -> List[str]:
        if value is None:
            return []

        text = value.strip()
        if not text:
            return []

        html_error = self._validate_html(text)
        if html_error:
            return [html_error]

        xml_text = text
        if "usdm:ref" in text and "xmlns:usdm" not in text:
            xml_text = f'<root xmlns:usdm="usdm">{text}</root>'  # noqa: E231
        root, xml_error = self._validate_xml(xml_text)
        if xml_error:
            return [xml_error]

        dtd_error = self._validate_refs(root)
        if dtd_error:
            return [dtd_error]
        return []

    def _validate_html(self, text: str) -> Optional[str]:
        try:
            html5lib.parse(text)
            return None
        except Exception as e:
            return f"Invalid HTML fragment: {e}"

    def _validate_xml(
        self, xml_text: str
    ) -> Tuple[Optional[etree._Element], Optional[str]]:
        try:
            parser = etree.XMLParser(ns_clean=True)
            root = etree.fromstring(xml_text.encode("utf-8"), parser=parser)
            return root, None
        except etree.XMLSyntaxError as e:
            return None, f"Invalid XML fragment: {e}"

    def _validate_refs(self, root: etree._Element) -> Optional[str]:
        try:
            refs = root.xpath("//usdm:ref", namespaces={"usdm": "usdm"})
        except Exception:
            refs = []
        if not refs:
            return None
        dtd = etree.DTD(io.StringIO(self.usdm_dtd))
        for ref in refs:
            ref_copy = etree.Element("ref", attrib=ref.attrib)
            if not dtd.validate(ref_copy):
                return f"Invalid XML fragment: {dtd.error_log.filter_from_errors()}"
        return None
