ALL_KEYWORD: str = "ALL"

COMPLETE_DATE_REGEX = "^\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}(:\\d{2})?(\\.\\d+)?([+-]\\d{2}:?\\d{2}|Z)?)?$"
YEAR_MONTH_REGEX = "^-?[0-9]{4}-[0-9]{2}$"
YEAR_REGEX = "^-?[0-9]{4}$"

# most record/group errors a rule reports per dataset
# the report says how many there were in total
MAX_ERRORS_PER_DATASET: int = 50000
