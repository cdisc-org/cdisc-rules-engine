from typing import List, Set
import re
from lxml import etree, html
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Order:
      1. Work on evaluation_dataset.
      2. Heuristic: must contain at least one <...> tag pattern else Not HTML fragment.
      3. Lenient HTML parse (lxml.html.fragments_fromstring). On failure -> Invalid HTML fragment.
      4. Strict XML parse attempts:
         a) raw
         b) wrapped in <root> (multi-root support)
         c) wrapped in <root> with auto-added namespace declarations for undefined prefixes
            (handles custom prefixed tags like <usdm:ref .../>)
         First success ends validation.

    Empty / None => no errors.
    Returns Series[ list[str] ]. Empty list => ok.
    """

    TAG_REGEX = re.compile(r"<[^>]+>")
    PREFIX_TAG_REGEX = re.compile(r"</?([A-Za-z_][\w.-]*):[A-Za-z_][\w.-]*")
    NS_DECL_REGEX = re.compile(r"xmlns:([A-Za-z_][\w.-]*)=")

    def _execute_operation(self):
        dataset = self.evaluation_dataset
        target = self.params.target
        if target not in dataset:
            error_list = [f"Target column '{target}' not found"]
            return dataset.get_series_from_value(error_list)
        return dataset[target].apply(self._validate_fragment)

    def _looks_like_html(self, text: str) -> bool:
        return bool(self.TAG_REGEX.search(text))

    def _parse_html(self, text: str):  # may raise ParserError / ValueError
        return html.fragments_fromstring(text)

    def _parse_xml(self, xml_text: str):
        try:
            etree.fromstring(xml_text.encode("utf-8"))
            return None
        except etree.XMLSyntaxError as e:
            return str(e)

    def _extract_prefixes(self, text: str) -> Set[str]:
        prefixes = {m.group(1) for m in self.PREFIX_TAG_REGEX.finditer(text)}
        return {p for p in prefixes if p not in {"xml"}}

    def _existing_declared_prefixes(self, text: str) -> Set[str]:
        return {m.group(1) for m in self.NS_DECL_REGEX.finditer(text)}

    def _parse_with_auto_namespaces(self, text: str):
        # Build root with declarations for any undeclared prefixes
        detected = self._extract_prefixes(text)
        declared = self._existing_declared_prefixes(text)
        missing = detected - declared
        if not missing:
            # nothing new to declare, skip
            return self._parse_xml(f"<root>{text}</root>")
        decls_parts = []
        for prefix in sorted(missing):
            # Build declaration without f-string colon formatting ambiguity to satisfy flake8 E231
            decls_parts.append("xmlns:" + prefix + "='urn:auto:" + prefix + "'")
        decls = " ".join(decls_parts)
        wrapped = f"<root {decls}>{text}</root>"
        return self._parse_xml(wrapped)

    def _validate_fragment(self, value: str) -> List[str]:
        errors: List[str] = []
        if value is None:
            return errors
        text = value.strip()
        if not text:
            return errors
        if not self._looks_like_html(text):
            errors.append("Not HTML fragment")
            return errors
        # HTML lenient parse
        try:
            self._parse_html(text)
        except (etree.ParserError, ValueError) as e:
            errors.append(f"Invalid HTML fragment: {e}")
            return errors
        # XML strict attempts
        xml_error = self._parse_xml(text)
        if xml_error is None:
            return errors
        wrapped_error = self._parse_xml(f"<root>{text}</root>")
        if wrapped_error is None:
            return errors
        # Namespace-aware retry if namespace prefix issues
        if ("Namespace prefix" in xml_error) or ("Namespace prefix" in wrapped_error):
            ns_error = self._parse_with_auto_namespaces(text)
            if ns_error is None:
                return errors
            errors.append(f"Invalid XHTML fragment: {ns_error}")
            return errors
        # Final failure
        errors.append(f"Invalid XHTML fragment: {wrapped_error}")
        return errors
