import io
from typing import List

import html5lib
from lxml import etree
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Order:
      1. Work on evaluation_dataset.
      2. Lenient HTML validation (html5lib). If fails -> Invalid HTML fragment.
      3. XML well-formedness validation (lxml.etree).
      4. Optional DTD validation for <usdm:ref> elements (attributes: attribute, id, klass).

    Empty / None => no errors.
    Returns Series[ list[str] ]. Empty list => ok.
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

    def _validate_fragment(self, value: str) -> List[str]:
        errors: List[str] = []
        if value is None:
            return errors
        text = value.strip()
        if not text:
            return errors
        # 1. HTML validation (lenient)
        try:
            html5lib.parse(text)
        except Exception as e:
            errors.append(f"Invalid HTML fragment: {e}")
            return errors
        xhtml_str = text
        # If custom usdm:ref prefix used without a namespace declaration, wrap to inject declaration.
        # This wrapper is internal only for parsing; it does not modify source data.
        if "usdm:ref" in text and "xmlns:usdm" not in text:
            text = f'<root xmlns:usdm="usdm">{xhtml_str}</root>'
        # 2. XML well-formedness
        try:
            parser = etree.XMLParser(ns_clean=True)
            root = etree.fromstring(text.encode("utf-8"), parser=parser)
        except etree.XMLSyntaxError as e:
            errors.append(f"Invalid XML fragment: {e}")
            return errors
        # 3. DTD validation for <usdm:ref>
        refs = []
        try:
            refs = root.xpath("//usdm:ref", namespaces={"usdm": "usdm"})
        except Exception:
            # If prefix unbound and we failed to wrap (edge case), fallback silently
            refs = []
        if refs:
            dtd = etree.DTD(io.StringIO(self.usdm_dtd))
            for ref in refs:
                ref_copy = etree.Element("ref", attrib=ref.attrib)
                if not dtd.validate(ref_copy):
                    errors.append(
                        f"Invalid XML fragment: {dtd.error_log.filter_from_errors()}"
                    )
        return errors
