import numpy as np

# a message like: [INFO 2021-12-29 17:10:26,575 - module.py:44] - Log Message
LOG_FORMAT: str = "[%(levelname)s %(asctime)s - %(filename)s:%(lineno)s] - %(message)s"

NULL_FLAVORS = ["", None, {}, {None}, [], [None], np.nan]

KNOWN_REPORT_EXTENSIONS = [".json", ".xlsx", ".xls"]

VALIDATION_FORMATS_MESSAGE = (
    "SAS V5 XPT, Dataset-JSON (JSON or NDJSON), or Excel (XLSX)"
)

DEFAULT_ENCODING: str = "utf-8"
