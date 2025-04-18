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
        NONCLIN: [],
        ANALYSIS: [],
    },
    SEND: {
        INDH: [],
        PROD: [],
        NONCLIN: [],
        ANALYSIS: [],
    },
    ADAM: {
        INDH: [],
        PROD: [],
        NONCLIN: [],
        ANALYSIS: [],
    },
    # no conformance rules for CDASH Presently
    CDASH: {
        INDH: [],
        PROD: [],
        NONCLIN: [],
        ANALYSIS: [],
    },
}
