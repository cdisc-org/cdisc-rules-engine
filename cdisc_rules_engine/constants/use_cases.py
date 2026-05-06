"""
Constants for use cases and their allowed domains.
"""

SDTM = "SDTM"
SEND = "SEND"
ADAM = "ADAM"
CDASH = "CDASH"

INDH = "INDH"
PROD = "PROD"
NONCLIN = "NONCLIN"
ANALYSIS = "ANALYSIS"

# NOTE: this may need to be expanded after the pilot re: custom domains, other applicable domains, etc.  The
USE_CASE_DOMAINS = {
    SDTM: {  # only prod and individual health are allowed for sdtm
        INDH: [
            "AE",
            "CO",
            "CM",
            "DM",
            "DI",
            "DU",
            "DO",
            "DS",
            "EG",
            "EX",
            "EC",
            "FA",
            "IE",
            "LB",
            "MH",
            "PC",
            "PP",
            "DA",
            "DV",
            "QS",
            "RELREC",
            "RE",
            "SC",
            "SE",
            "SV",
            "SU",
            "EM",
            "TA",
            "TE",
            "TI",
            "TS",
            "TV",
            "VS",
        ],
        PROD: ["TO", "PD", "PT", "IT", "IN", "IQ", "ES"],
        NONCLIN: [],
        ANALYSIS: [],
    },
    SEND: {  # only nonclin allowed for send
        INDH: [],
        PROD: [],
        NONCLIN: [
            "BW",
            "CV",
            "CL",
            "CO",
            "DD",
            "DM",
            "DI",
            "DU",
            "DS",
            "EG",
            "EX",
            "FW",
            "GT",
            "LB",
            "MA",
            "MI",
            "OM",
            "PM",
            "PK",
            "PP",
            "POOLDEF",
            "RELREC",
            "RELREF",
            "RE",
            "SC",
            "SE",
            "TA",
            "TE",
            "TF",
            "TX",
            "TS",
            "VS",
        ],
        ANALYSIS: [],
    },
    ADAM: {  # only analysis allowed for adam, ADAM AD-- prefix check is done elsewhere.  This is here for completeness.
        INDH: [],
        PROD: [],
        NONCLIN: [],
        ANALYSIS: [],
    },
    CDASH: {  # no conformance rules for CDASH Presently
        INDH: [],
        PROD: [],
        NONCLIN: [],
        ANALYSIS: [],
    },
}
