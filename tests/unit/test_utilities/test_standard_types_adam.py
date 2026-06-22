from cdisc_rules_engine.constants.adam_products import ADAM_PRODUCTS
from cdisc_rules_engine.enums.standard_types import StandardTypes


def test_standard_types_includes_all_adam_products():
    """Every ADaM product handled by normalize_standard_input must be a valid
    CLI standard. If it is not, ``-s <product>`` is rejected by the standards
    gate in core.py before the engine runs.
    """
    supported = set(StandardTypes.values())
    missing = [p for p in ADAM_PRODUCTS if p not in supported]
    assert not missing, f"ADAM products absent from StandardTypes: {missing}"
