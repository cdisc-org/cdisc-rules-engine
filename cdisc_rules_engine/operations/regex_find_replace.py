import re
import pandas as pd

from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import OperationError


class RegexFindReplace(BaseOperation):
    _NO_MATCH_POLICIES = {"keep_original", "set_null", "set_empty", "error"}
    _FLAG_MAP = {
        "i": re.IGNORECASE,
        "m": re.MULTILINE,
        "s": re.DOTALL,
    }

    def _execute_operation(self):
        operation_id = self.params.operation_id
        target = self.params.target
        find = getattr(self.params, "find", None) or getattr(self.params, "regex", None)
        replace = getattr(self.params, "replace", None)
        on_no_match = getattr(self.params, "on_no_match", "keep_original")
        flags_str = getattr(self.params, "flags", "")

        self._validate_required(
            operation_id, target, find, replace, on_no_match, flags_str
        )

        if target not in self.evaluation_dataset.columns:
            raise OperationError(f"Target column not found: {target}")

        flags = self._parse_flags(flags_str)
        pattern = self._compile_pattern(find, flags)

        source = self.evaluation_dataset[target]
        transformed = source.map(
            lambda value: self._transform_value(
                value=value,
                pattern=pattern,
                replace=replace,
                on_no_match=on_no_match,
            )
        )

        return transformed

    def _validate_required(
        self, operation_id, target, find, replace, on_no_match, flags_str
    ):
        if not operation_id:
            raise OperationError("regex_find_replace requires id (operation_id)")
        if not target:
            raise OperationError("regex_find_replace requires name (target)")
        if not find:
            raise OperationError("regex_find_replace requires find (or regex)")
        if replace is None:
            raise OperationError("regex_find_replace requires replace")
        if on_no_match not in self._NO_MATCH_POLICIES:
            raise OperationError(
                f"Invalid on_no_match: {on_no_match}. "
                f"Must be one of {sorted(self._NO_MATCH_POLICIES)}"
            )
        invalid_flags = [f for f in flags_str if f not in self._FLAG_MAP]
        if invalid_flags:
            raise OperationError(
                f"Invalid flags: {''.join(invalid_flags)}. "
                f"Allowed flags: {''.join(sorted(self._FLAG_MAP.keys()))}"
            )

    def _parse_flags(self, flags_str):
        flags = 0
        for ch in flags_str:
            flags |= self._FLAG_MAP[ch]
        return flags

    def _compile_pattern(self, find, flags):
        try:
            return re.compile(find, flags)
        except re.error as exc:
            raise OperationError(f"Invalid regex pattern '{find}': {exc}") from exc

    def _transform_value(self, value, pattern, replace, on_no_match):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None

        text = str(value)
        match = pattern.search(text)
        if match:
            return pattern.sub(replace, text)

        if on_no_match == "keep_original":
            return text
        if on_no_match == "set_null":
            return None
        if on_no_match == "set_empty":
            return ""
        raise OperationError(
            f"No match found for value '{text}' and on_no_match='error'"
        )
