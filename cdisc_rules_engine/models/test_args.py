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
        "controlled_terminology_package",
        "define_version",
    ],
)
