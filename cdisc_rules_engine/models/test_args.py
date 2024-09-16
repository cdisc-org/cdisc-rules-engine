from collections import namedtuple

TestArgs = namedtuple(
    "TestArgs",
    [
        "cache",
        "dataset_path",
        "rule",
        "standard",
        "version",
        "whodrug",
        "meddra",
        "loinc",
        "medrt",
        "controlled_terminology_package",
        "define_version",
        "define_xml_path",
        "validate_xml",
    ],
)
