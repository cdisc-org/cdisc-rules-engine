from collections import namedtuple

Validation_args = namedtuple(
    "Validation_args",
    [
        "cache",
        "pool_size",
        "dataset_paths",
        "log_level",
        "report_template",
        "standard",
        "version",
        "substandard",
        "controlled_terminology_package",
        "output",
        "output_format",
        "raw_report",
        "define_version",
        "external_dictionaries",
        "rules",
        "local_rules",
        "local_rules_cache",
        "local_rules_id",
        "progress",
        "define_xml_path",
    ],
)
