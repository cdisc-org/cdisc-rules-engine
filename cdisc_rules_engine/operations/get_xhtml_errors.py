import io
from typing import List, Optional, Tuple

import html5lib
from lxml import etree
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Steps:
      1. Use evaluation_dataset.
      2. HTML lenient parse (html5lib) -> on failure emit "Invalid HTML fragment:".
      3. XML well-formedness (lxml.etree) -> on failure emit "Invalid XML fragment:".
      4. DTD validation for <usdm:ref> (attributes: attribute, id, klass) -> on failure emit "Invalid XML fragment:".

    Empty / None values return an empty error list.
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
        return dataset[target].apply(self._validate_fragment)

    # --- helpers (only multi-step logic kept) ---
    def _validate_html(self, text: str) -> Optional[str]:
        try:
            html5lib.parse(text)
            return None
        except Exception as e:  # noqa: BLE001
            return f"Invalid HTML fragment: {e}"

    def _parse_xml(self, xml_text: str) -> Tuple[Optional[etree._Element], Optional[str]]:  # type: ignore[name-defined]
        try:
            parser = etree.XMLParser(ns_clean=True)
            root = etree.fromstring(xml_text.encode("utf-8"), parser=parser)
            return root, None
        except etree.XMLSyntaxError as e:  # noqa: BLE001
            return None, f"Invalid XML fragment: {e}"

    def _validate_refs(self, root: etree._Element) -> Optional[str]:  # type: ignore[name-defined]
        try:
            refs = root.xpath("//usdm:ref", namespaces={"usdm": "usdm"})
        except Exception:  # noqa: BLE001
            refs = []
        if not refs:
            return None
        dtd = etree.DTD(io.StringIO(self.usdm_dtd))
        for ref in refs:
            ref_copy = etree.Element("ref", attrib=ref.attrib)
            if not dtd.validate(ref_copy):
                return f"Invalid XML fragment: {dtd.error_log.filter_from_errors()}"
        return None

    # --- main ---
    def _validate_fragment(self, value: str) -> List[str]:
        if value is None:
            return []
        text = value.strip()
        if not text:
            return []
        html_error = self._validate_html(text)
        if html_error:
            return [html_error]
        # inline namespace wrapping logic (no separate one-liner helper)
        xml_text = text
        if "usdm:ref" in text and "xmlns:usdm" not in text:
            xml_text = f'<root xmlns:usdm="usdm">{text}</root>'
        root, xml_error = self._parse_xml(xml_text)
        if xml_error:
            return [xml_error]
        dtd_error = self._validate_refs(root)
        if dtd_error:
            return [dtd_error]
        return []
