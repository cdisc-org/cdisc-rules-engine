"""
Set difference operation: name minus value.
Returns elements in name that are not in value, preserving order from name.
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
    Operation that computes set difference: name minus value.
    name (minuend) and value (subtrahend) reference other operation results.
    Returns elements in name that are not in value.
    """

    def _execute_operation(self):
        name_ref = self.params.target  # minuend (list A)
        value_ref = self.params.value  # subtrahend (list B)

        if not name_ref or name_ref not in self.evaluation_dataset.columns:
            return []
        list_a = self.evaluation_dataset[name_ref].iloc[0]
        if not value_ref or value_ref not in self.evaluation_dataset.columns:
            # listA minus empty = all of listA (per Sam)
            return _normalize_to_list(list_a)
        list_b = self.evaluation_dataset[value_ref].iloc[0]
        return _set_difference_preserve_order(list_a, list_b)
