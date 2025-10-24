import os
from lxml import etree
import re

from cdisc_rules_engine.exceptions.custom_exceptions import (
    SchemaNotFoundError,
    InvalidSchemaProvidedError,
)
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Steps:
      1. Make sure the column is a valid XML -> on failure generate a list of XML validation errors
      2. XMLSchema validation -> on failure generate a list of XMLSchema validation errors
      3. Return all validation errors in one go

    Empty / None values return an empty list.
    """

    def _execute_operation(self):
        dataset = self.evaluation_dataset
        target = self.params.target
        if target not in dataset:
            raise KeyError(target)
        # The XSD should be referenced as specified in DOCTYPE (so that external entities can be resolved correctly).
        # For example:
        #
        # namespaces:
        #  - uri: http://www.w3.org/1999/xhtml
        #  - uri: http://www.cdisc.org/ns/usdm/xhtml/v1.0
        #    prefix: usdm
        #  - uri: http://www.w3.org/2000/svg
        #    prefix: svg
        #  - uri: http://www.w3.org/1998/Math/MathML
        #    prefix: math
        #
        # The schemaLocation values would probably needed to be configurable as well.
        try:
            self.schema = etree.XMLSchema(file=os.path.abspath(self.params.xsd_path))
        except OSError as e:
            raise SchemaNotFoundError(f"XSD file could not be found: {e}")
        except (etree.XMLSchemaParseError, etree.XMLSyntaxError) as e:
            raise InvalidSchemaProvidedError(
                f"Failed to parse XMLSchema: {getattr(e, 'error_log', str(e))}"
            )

        # Build namespace declaration string from self.params.namespaces
        ns_list = getattr(self.params, "namespaces", [])
        nsdec_parts = []
        for ns in ns_list:
            uri = ns.get("uri")
            prefix = ns.get("prefix")
            if prefix:
                nsdec_parts.append(f'xmlns:{prefix}="{uri}"')
            else:
                nsdec_parts.append(f'xmlns="{uri}"')
        self.nsdec = " ".join(nsdec_parts)
        self.line_pattern = re.compile(r"line (\d+)")

        return dataset[target].apply(self._ensure_dataset_is_valid_xhtml)

    def _ensure_dataset_is_valid_xhtml(self, value: str) -> list[str]:
        value: str = value.strip()
        if not value:
            return []

        text = value.strip()
        if not text:
            return []

        errors = []

        xhtml_mod, text = self._wrap_xhtml(text)

        line_labels = (
            {
                1: "(wrapper start)",
                len(text.split("\n")): "(wrapper end)",
            }
            if xhtml_mod
            else {}
        )

        parser = etree.XMLParser(recover=True, ns_clean=True)
        xhtml_to_validate = etree.XML(text.encode("utf-8"), parser)

        self._report_errors(
            xhtml_to_validate, parser.error_log, errors, xhtml_mod, line_labels
        )

        if not self.schema.validate(xhtml_to_validate):
            self._report_errors(
                xhtml_to_validate,
                self.schema.error_log,
                errors,
                xhtml_mod,
                line_labels,
            )
        return errors

    def _wrap_xhtml(self, text: str) -> tuple[bool, str]:
        """Wraps the input text in <html><head><title></title></head><body>...</body></html> if not already present."""
        if not text.startswith("<"):
            return (
                True,
                f"<html {self.nsdec}><head><title></title></head><body><div>\n{text}\n</div></body></html>",
            )
        if "<body>" not in text:
            return (
                True,
                f"<html {self.nsdec}><head><title></title></head><body>\n{text}\n</body></html>",
            )
        if "<head>" not in text:
            return True, (
                text.replace("<body>", "<head><title></title></head><body>")
                if text.startswith("<html")
                else f"<html {self.nsdec}><head><title></title></head><body>\n{text}\n</body></html>"
            )
        return False, text

    def _report_errors(
        self,
        xhtml: etree.ElementTree,
        error_log: etree._ErrorLog,
        errors: list[str],
        xhtml_mod: bool = False,
        line_lbls: dict = {},
    ) -> list[str]:
        for error in error_log:
            msg = error.message.strip()
            if xhtml_mod and re.search(self.line_pattern, msg):
                # Adjust line numbers in message
                msg = self.line_pattern.sub(
                    lambda x: self._get_line_name(line_lbls, int(x.groups()[0])),
                    msg,
                )

            line_col = (
                (
                    f"{self._get_line_name(line_lbls, error.line, error.column)}"
                    if xhtml_mod
                    else f"line {error.line}"
                )
                if error.line
                else "unknown pos"
            )

            if xhtml.nsmap:
                for k, v in xhtml.nsmap.items():
                    if v in msg:
                        prefix = f"{k}:" if k else ""
                        msg = re.sub(r"\{" + re.escape(v) + r"\}", prefix, msg)

            errors.append(f"Invalid XHTML {line_col} [{error.level_name}]: {msg}")

    def _get_line_name(self, line_labels, line: int, col: int | None = None) -> str:
        return line_labels.get(
            line, f"line {line - 1}" + (f", col {col}" if col else "")
        )
