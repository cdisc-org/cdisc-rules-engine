from collections import namedtuple

Validation_args = namedtuple(
    "Validation_args",
    [
        "cache",
        "pool_size",
        "data",
        "log_level",
        "report_template",
        "standard",
        "version",
        "controlled_terminology_package",
        "output",
        "output_format",
        "raw_report",
        "define_version",
        "whodrug",
        "meddra",
        "disable_progressbar",
    ],
)
