from .base_enum import BaseEnum


class ProgressParameterOptions(BaseEnum):
    BAR = "bar"
    PERCENTS = "percents"
    DISABLED = "disabled"
    VERBOSE_OUTPUT = "verbose_output"
