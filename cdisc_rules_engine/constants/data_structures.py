ADSL: str = "SUBJECT LEVEL ANALYSIS DATASET"
BDS: str = "BASIC DATA STRUCTURE"
OCCDS: str = "OCCURRENCE DATA STRUCTURE"
OTHER: str = "ADAM OTHER"

# technically PARAM can be found in ADaM OTHER
bds_indicators = ["ARAMCD", "PARAM", "AVAL", "AVALC"]
# --TRT and --TERM are not OCCDS exclusive but there is very little reason for them to be in a BDS dataset
occds_indicators = ["--TERM", "--TRT"]
