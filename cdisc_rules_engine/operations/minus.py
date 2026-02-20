"""
Set difference operation: name minus subtract.
Returns elements in name that are not in subtract, preserving order from name.
"""

from cdisc_rules_engine.operations.base_operation import BaseOperation


def _normalize_to_list(val):
    """Convert value to a list for set operations."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, (set, tuple)):
        return list(val)
    return [val]


def _set_difference_preserve_order(list_a: list, list_b: list) -> list:
    """
    Compute set difference A \\ B (elements in A not in B).
    Preserves order from list_a.
    """
    set_b = set(_normalize_to_list(list_b))
    return [x for x in _normalize_to_list(list_a) if x not in set_b]


class Minus(BaseOperation):
    """
    Operation that computes set difference: name minus subtract.
    name (minuend) and subtract (subtrahend) reference other operation results.
    Returns elements in name that are not in subtract.
    """

    def _execute_operation(self):
        name_ref = self.params.target
        subtract_ref = self.params.subtract

        if not name_ref or name_ref not in self.evaluation_dataset.columns:
            return []
        list_a = self.evaluation_dataset[name_ref].iloc[0]
        if not subtract_ref or subtract_ref not in self.evaluation_dataset.columns:
            return _normalize_to_list(list_a)
        list_b = self.evaluation_dataset[subtract_ref].iloc[0]
        return _set_difference_preserve_order(list_a, list_b)
