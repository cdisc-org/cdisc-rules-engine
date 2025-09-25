from typing import List, Optional
import pandas as pd
import xml.etree.ElementTree as ET
from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetXhtmlErrors(BaseOperation):
    """Validate XHTML fragments in the target column.

    Each cell is validated for:
      - XML well-formedness (single root)
      - Or multiple top-level elements (valid when wrapped)

    Returns a Series of lists: empty list => no errors.
    """

    def _execute_operation(self):
        dataframe = self.params.dataframe
        target = self.params.target

        if target not in dataframe:
            error_list = [f"Target column '{target}' not found"]
            return self.evaluation_dataset.get_series_from_value(error_list)

        return dataframe[target].apply(self._validate_fragment)

    @staticmethod
    def _normalize_value(value) -> Optional[str]:
        """Return stripped string or None if treat-as-empty / skip."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, (list, dict, set, tuple)):
            return "__ITERABLE__"
        if not isinstance(value, str):
            return "__NON_STRING__"
        text = value.strip()
        return text or None

    @staticmethod
    def _try_parse(xml_text: str) -> Optional[str]:
        """Return None if parse OK, else error string."""
        try:
            ET.fromstring(xml_text)
            return None
        except ET.ParseError as e:
            return str(e)

    def _validate_fragment(self, value) -> List[str]:
        errors: List[str] = []
        norm = self._normalize_value(value)

        if norm is None:
            return errors
        if norm == "__ITERABLE__":
            errors.append("Value is not a string (got iterable)")
            return errors
        if norm == "__NON_STRING__":
            errors.append("Value is not a string")
            return errors

        first_error = self._try_parse(norm)
        if first_error is None:
            return errors

        wrapped_error = self._try_parse(f"<root>{norm}</root>")
        if wrapped_error is not None:
            errors.append(f"Invalid XHTML fragment: {wrapped_error}")

        return errors
