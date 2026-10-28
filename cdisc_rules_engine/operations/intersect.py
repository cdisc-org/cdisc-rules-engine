from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.operations.minus import _normalize_to_list


def _normalize_case(value, case_sensitive: bool):
    if not case_sensitive and isinstance(value, str):
        return value.upper()
    return value


def _set_intersection(list_a, list_b, case_sensitive: bool = True) -> list:
    """
    Compute set intersection A ∩ B (elements in A that are also in B).
    Preserves first-occurrence order from list_a and removes duplicates,
    matching the SQL engine's EXISTS + GROUP BY/MIN(ord) behaviour.
    When case_sensitive is False, values are compared and returned upper-cased.
    """
    set_b = {
        _normalize_case(x, case_sensitive)
        for x in _normalize_to_list(list_b)
        if x is not None
    }
    result = []
    seen = set()
    for item in _normalize_to_list(list_a):
        if item is None:
            continue
        value = _normalize_case(item, case_sensitive)
        if value in set_b and value not in seen:
            seen.add(value)
            result.append(value)
    return result


class Intersect(BaseOperation):
    """
    Operation that computes set intersection of name and subtract.
    name and subtract reference other operation results.
    Returns elements in name that are also present in subtract.
    """

    def _execute_operation(self):
        name_ref = self.params.target
        subtract_ref = self.params.subtract

        if not name_ref or name_ref not in self.evaluation_dataset.columns:
            return []
        if not subtract_ref or subtract_ref not in self.evaluation_dataset.columns:
            return []
        list_a = self.evaluation_dataset[name_ref].iloc[0]
        list_b = self.evaluation_dataset[subtract_ref].iloc[0]
        return _set_intersection(list_a, list_b, self.params.case_sensitive)
