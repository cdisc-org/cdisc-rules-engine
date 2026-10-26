from collections import namedtuple

from cdisc_rules_engine.constants.rule_constants import MAX_ERRORS_PER_DATASET

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
        "custom_standard",
        "progress",
        "define_xml_path",
        "stf_file_path",
        "validate_xml",
        "sql_namespace",
        "collapse_duplicate_issues",
        "max_errors_per_dataset",
    ],
    defaults=[False, MAX_ERRORS_PER_DATASET],
)
