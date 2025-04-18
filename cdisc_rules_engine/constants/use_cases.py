"""
Constants for use cases and their allowed domains.
"""

# Substandard constants
SDTM = "SDTM"
SEND = "SEND"
ADAM = "ADaM"
CDASH = "CDASH"

# Use case constants
INDH = "INDH"
PROD = "PROD"
NONCLIN = "NONCLIN"
ANALYSIS = "ANALYSIS"

# Dictionary mapping use cases and substandards to their allowed domains
USE_CASE_DOMAINS = {
    SDTM: {
        INDH: [],
        PROD: [],
        NONCLIN: [],  # Empty as NONCLIN may not apply to SDTM
        ANALYSIS: [],  # Empty as ANALYSIS may not apply to SDTM
    },
    SEND: {
        INDH: [],  # Empty as INDH may not apply to SEND
        PROD: [],  # Empty as PROD may not apply to SEND
        NONCLIN: [
            "BG",
            "BL",
            "BW",
            "CL",
            "DD",
            "DM",
            "EX",
            "FW",
            "LB",
            "TS",
            "PC",
            "PP",
            "TX",
            "VS",
        ],
        ANALYSIS: [],  # Empty as ANALYSIS may not apply to SEND
    },
    ADAM: {
        INDH: [],  # Empty as INDH may not have specific ADaM domains
        PROD: [],  # Empty as PROD may not have specific ADaM domains
        NONCLIN: [],  # Empty as NONCLIN may not have specific ADaM domains
        ANALYSIS: [
            "ADAE",
            "ADSL",
            "ADVS",
            "ADLB",
            "ADEG",
            "ADQSCISR",
            "ADPP",
            "ADPC",
            "ADTTE",
        ],
    },
    # no conformance ruels for CDASH Presently
    CDASH: {
        INDH: [],
        PROD: [],
        NONCLIN: [],
        ANALYSIS: [],  # Empty as ANALYSIS may not apply to CDASH
    },
}
