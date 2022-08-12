import re

XX_PREFIXES = [
    "P",
    "TR",
    "TRT",
    "AP",
    "EOT",
    "DCT",
    "EOP",
    "DCP",
    "TREM",
    "TRTEM",
    "ONTR",
]
ZZ_PREFIXES = ["ANL", "SMQ", "CQ", "AOCC"]


class AdamVariableReader:
    def __init__(self):
        self.alphanum_cols = []
        self.categorization_scheme = {}  # y variable
        self.w_indexes = {}  # w variable
        self.period = {}  # xx variable
        self.selection_algorithm = {}  # zz variable

    def extract_columns(self, columns_list: list):
        for value in columns_list:
            if True in [char.isdigit() for char in value]:
                self.alphanum_cols.append(value)
        return self.alphanum_cols

    def check_y(self, name: str):
        ends_with_y = re.search(r"(\d+$|\d+N)", name)
        if ends_with_y is not None:
            string_yn = ends_with_y.group()
            if string_yn.endswith("N"):
                string_yn = string_yn[:-1]
            self.categorization_scheme[name] = int(string_yn)

    def check_w(self, name: str):
        for i in name:
            if i.isdigit() and name.index(i) + 1 == int(i):
                self.w_indexes[name] = name.index(i) + 1

    def check_xx_zz(self, name: str):
        split_name = re.findall(r"(\d+|[A-Za-z]+)", name)  # split when number comes
        if split_name[0] in XX_PREFIXES:
            if len(split_name[1]) == 2 and split_name[1].isdigit():
                self.period[name] = int(split_name[1])
        elif split_name[0] in ZZ_PREFIXES:
            if len(split_name[1]) == 2 and split_name[1].isdigit():
                self.selection_algorithm[name] = int(split_name[1])
