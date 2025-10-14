import io

import html5lib
from lxml import etree

from cdisc_rules_engine.exceptions.custom_exceptions import ColumnNotFoundError
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Steps:
      1. Make sure the column is a valid HTML -> on failure generate a list of HTML validation errors
      2. Make sure the column is a valid XML -> on failure generate a list of XML validation errors
      3. DTD validation for <usdm:ref> and <usdm:tag> -> on failure generate a list of DTD validation errors
      4. Return all validation errors in one go

    Empty / None values return an empty list.
    """

    usdm_ref_dtd = """\
    <!ELEMENT ref EMPTY>
    <!ATTLIST ref
        attribute CDATA #REQUIRED
        id        ID    #REQUIRED
        klass     CDATA #REQUIRED
    >
    """

    usdm_tag_dtd = """\
    <!ELEMENT tag EMPTY>
    <!ATTLIST tag
        name CDATA #REQUIRED
    >
    """

    def _execute_operation(self):
        dataset = self.evaluation_dataset
        target = self.params.target
        if target not in dataset:
            raise ColumnNotFoundError

        return dataset[target].apply(self._ensure_dataset_is_valid_xhtml)

    def _ensure_dataset_is_valid_xhtml(self, value: str) -> list[str]:
        if value is None:
            return []

        text = value.strip()
        if not text:
            return []

        html_errors: list = self._validate_html(text)

        xml_text = text
        if ("usdm:ref" in text or "usdm:tag" in text) and "xmlns:usdm" not in text:
            xml_text = f'<root xmlns:usdm="usdm">{text}</root>'
        root, xml_errors = self._validate_xml(xml_text)

        if not root:
            # xml itself is invalid and we can't even start the DTD validation
            return [*html_errors, *xml_errors]

        dtd_errors = self._validate_usdm_elements(root)

        return [*html_errors, *xml_errors, *dtd_errors]

    def _validate_html(self, text: str) -> list:
        parser = html5lib.HTMLParser(namespaceHTMLElements=False)
        parser.parse(text)
        errors = []

        for error in getattr(parser, "errors", []):
            pos, code, datavars = error
            line_col = f"line {pos[0]}, col {pos[1]}" if pos else "unknown pos"
            if datavars:
                errors.append(f"{code} at {line_col} — details: {datavars}")
            else:
                errors.append(f"{code} at {line_col}")

        return errors

    def _validate_xml(self, xml_text: str) -> tuple[etree._Element | None, list]:
        parser = etree.XMLParser(recover=False, ns_clean=True)
        errors = []

        try:
            root = etree.fromstring(xml_text.encode("utf-8"), parser)
        except etree.XMLSyntaxError:
            root = None

        for error in parser.error_log:
            line_col = (
                f"line {error.line}, col {error.column}"
                if error.line
                else "unknown pos"
            )
            errors.append(
                f"{error.type_name} at {line_col} — details: {error.message.strip()}"
            )

        return root, errors

    def _validate_usdm_elements(self, root: etree._Element) -> list[str]:
        """Validate <usdm:ref> and <usdm:tag> elements against their DTDs."""
        errors: list[str] = []

        try:
            refs = root.xpath("//usdm:ref", namespaces={"usdm": "usdm"})
            tags = root.xpath("//usdm:tag", namespaces={"usdm": "usdm"})
        except Exception as e:
            return [f"XPath evaluation error: {e}"]

        errors.extend(self._validate_elements(refs, self.usdm_ref_dtd, "ref"))
        errors.extend(self._validate_elements(tags, self.usdm_tag_dtd, "tag"))

        return errors

    def _validate_elements(
        self, elements, dtd_str: str, element_name: str
    ) -> list[str]:
        """Validate a set of elements against the given DTD and return list of errors."""
        errors: list[str] = []

        if not elements:
            return errors

        try:
            dtd = etree.DTD(io.StringIO(dtd_str))
        except Exception as e:
            errors.append(f"Failed to parse DTD for {element_name}: {e}")
            return errors

        for element in elements:
            element_copy = etree.Element(element_name, attrib=element.attrib)
            if not dtd.validate(element_copy):
                errors.append(
                    f"Invalid <usdm:{element_name}>: {dtd.error_log.filter_from_errors()}"
                )

        return errors
