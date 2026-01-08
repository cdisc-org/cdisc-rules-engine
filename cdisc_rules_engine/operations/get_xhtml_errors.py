import os
from lxml import etree
import re

from cdisc_rules_engine.exceptions.custom_exceptions import (
    SchemaNotFoundError,
    InvalidSchemaProvidedError,
)
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Steps:
      1. Retrieve local XSD based on self.params.namespace
      2. Make sure the column is a valid XML -> on failure generate a list of XML validation errors
      2. XMLSchema validation -> on failure generate a list of XMLSchema validation errors
      3. Return all validation errors in one go

    Empty / None values return an empty list.
    """

    def _execute_operation(self):
        dataset = self.evaluation_dataset
        target = self.params.target
        if target not in dataset:
            raise KeyError(target)
        try:
            self.schema_xml = etree.parse(
                os.path.join(
                    DefaultFilePaths.LOCAL_XSD_FILE_DIR.value,
                    DefaultFilePaths.LOCAL_XSD_FILE_MAP.value[self.params.namespace],
                )
            )
            self.schema = etree.XMLSchema(self.schema_xml)
        except (KeyError, OSError) as e:
            raise SchemaNotFoundError(f"XSD file could not be found: {e}")
        except (etree.XMLSchemaParseError, etree.XMLSyntaxError) as e:
            raise InvalidSchemaProvidedError(
                f"Failed to parse XMLSchema: {getattr(e, 'error_log', str(e))}"
            )

        # Build namespace declaration string from XSD namespaces
        schema_nsmap = self.schema_xml.getroot().nsmap
        schema_nsmap.pop("xs")
        self.nsdec = " ".join(
            k and f'xmlns:{k}="{v}"' or f'xmlns="{v}"' for k, v in schema_nsmap.items()
        )
        if any(
            k in DefaultFilePaths.LOCAL_XSD_FILE_MAP.value
            for k in schema_nsmap.values()
        ):
            self.nsdec += ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="'
            for k, v in DefaultFilePaths.LOCAL_XSD_FILE_MAP.value.items():
                if k in schema_nsmap.values():
                    self.nsdec += "{} ../{} ".format(
                        k,
                        # Using join because schemaLocation always uses forward slashes regardless of OS
                        "/".join(
                            DefaultFilePaths.LOCAL_XSD_FILE_DIR.value.split(os.sep)
                            + v.split(os.sep)
                        ),
                    )
            self.nsdec += '"'

        self.line_pattern = re.compile(r"line (\d+)")

        return dataset[target].apply(self._ensure_dataset_is_valid_xhtml)

    def _ensure_dataset_is_valid_xhtml(self, value: str) -> list[str]:
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
