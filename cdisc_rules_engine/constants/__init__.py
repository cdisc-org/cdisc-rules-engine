# a message like: [INFO 2021-12-29 17:10:26,575 - module.py:44] - Log Message
LOG_FORMAT: str = "[%(levelname)s %(asctime)s - %(filename)s:%(lineno)s] - %(message)s"
XPT_LABEL_PATTERN: str = (
    "HEADER RECORD\\*{7}MEMBER {2}HEADER RECORD!{7}0{17}160{8}140 "
    "{2}HEADER RECORD\\*{7}DSCRPTR HEADER RECORD!{7}0{30}"
    "  SAS\\s{5}.{8}SASDATA .{16}\\s{24}.{16}.{16}\\s{16}(?P<label>.{40})"
)

XPT_MODIFIED_DATE_PATTERN: str = (
    "HEADER RECORD\\*{7}MEMBER {2}HEADER RECORD!{7}0{17}160{8}140 "
    "{2}HEADER RECORD\\*{7}DSCRPTR HEADER RECORD!{7}0{30}"
    "  SAS\\s{5}.{8}SASDATA .{16}\\s{24}.{16}(?P<modified_date>.{16})\\s{16}.{40}"
)

REPORT_TEMPLATE_PATH = "/templates/report-template.xlsx"
