from .base_enum import BaseEnum


class VariableRoles(BaseEnum):
    """
    This enum holds possible values of "role"
    attribute of CDISC Library variable metadata.
    """

    IDENTIFIER = "Identifier"
    TIMING = "Timing"
