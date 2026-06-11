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


def _set_difference_order_insensitive(list_a: list, list_b: list) -> list:
    """
    Compute set difference A \\ B (elements in A not in B).
    Preserves order from list_a.
    """
    set_b = set(_normalize_to_list(list_b))
    return [x for x in _normalize_to_list(list_a) if x not in set_b]


def _set_difference_order_sensitive(list_a: list, list_b: list) -> list:
    """
    Compute set difference A \\ B (elements in A not in B).
    Take into account order of elements
    Preserves order from list_a.
    """
    result = []
    a_start_index = 0
    list_a_normalized = _normalize_to_list(list_a)
    for b_item in _normalize_to_list(list_b):
        # Check if b_item is in the remaining part of A
        set_a = set(list_a_normalized[a_start_index:])
        if b_item in set_a:
            match_found = False
            # Iterate through A starting from last matched index
            for i in range(a_start_index, len(list_a_normalized)):
                a_item = list_a_normalized[i]
                if a_item != b_item:
                    if match_found:
                        break
                    else:
                        result.append(a_item)
                else:
                    # Move start index to next position after matched item
                    a_start_index = i + 1
                    # We have to continue checking for duplicates of b_item in A, so we don't break here
                    match_found = True
        else:
            # If B item is not in A, ignore it since there is nothing to subtract from A
            continue

    # Add any remaining items in A after last matched index
    result.extend(list_a_normalized[a_start_index:])

    return result


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
        if self.params.order_insensitive:
            return _set_difference_order_insensitive(list_a, list_b)
        else:
            return _set_difference_order_sensitive(list_a, list_b)
