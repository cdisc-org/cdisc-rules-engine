import re

NO_MATCH_POLICIES = {"keep_original", "set_null", "set_empty", "error"}

REGEX_FLAG_MAP = {
    "i": re.IGNORECASE,
    "m": re.MULTILINE,
    "s": re.DOTALL,
}