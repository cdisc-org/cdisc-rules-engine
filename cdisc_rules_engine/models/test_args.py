from collections import namedtuple

TestArgs = namedtuple(
    "TestArgs",
    [
        "cache",
        "dataset_paths",
        "log_level",
        "rule",
        "standard",
        "version",
        "substandard",
        "external_dictionaries",
        "controlled_terminology_package",
        "define_version",
        "define_xml_path",
        "validate_xml",
    ],
)
