import csv
from functools import partial

import pytest

from cdisc_rules_engine.readers.external_dictionary_readers.whodrug_reader import WhoDrugReader

VERSION_LINE = "WHODRUG DUMMY B3 January 1, 2000 DUMMYB3Jan00"

INA_ROWS = [
    ("X", "1", "DUMMYLEVEL1"),
    ("X99", "2", "DUMMYLEVEL2"),
    ("X99X", "3", "DUMMYLEVEL3, WITH COMMA"),
    ("X99XX", "4", "DUMMYLEVEL4"),
]
DDA_ROWS = [("000001", "01", "001", "6", "X99XX"), ("000001", "02", "001", "5", "X99XX")]
DD_ROWS = [
    ("000001", "01", "001", "6", '"DUMMY" DRUG, WITH COMMA'),
    ("000001", "02", "001", "5", '"DUMMY" DRUG, WITH COMMA'),
]

EXPECTED = {
    "term_code": "X99XX",
    "level_1": "DUMMYLEVEL1",
    "level_2": "DUMMYLEVEL2",
    "level_3": "DUMMYLEVEL3, WITH COMMA",
    "level_4": "DUMMYLEVEL4",
    "drug_rec_num": "000001",
    "term_name": '"DUMMY" DRUG, WITH COMMA',
}


def write_fixed_width(directory):
    (directory / "Version.txt").write_text(VERSION_LINE + "\n")
    (directory / "INA.txt").write_text("".join(f"{code:<7}{level}{text:<110}\n" for code, level, text in INA_ROWS))
    (directory / "DDA.txt").write_text(
        "".join(f"{rec}{seq1}{seq2}{check}{atc:<7}  111*\n" for rec, seq1, seq2, check, atc in DDA_ROWS)
    )
    (directory / "DD.txt").write_text(
        "".join(f"{rec}{seq1}{seq2}{check}N  001      01 854{name}\n" for rec, seq1, seq2, check, name in DD_ROWS)
    )


def write_tab_delimited(directory):
    (directory / "Version.txt").write_text(VERSION_LINE + "\n")
    (directory / "INA.txt").write_text("".join("\t".join(row) + "\n" for row in INA_ROWS))
    (directory / "DDA.txt").write_text("".join("\t".join(row) + "\n" for row in DDA_ROWS))
    (directory / "DD.txt").write_text(
        "".join(
            "\t".join([rec, seq1, seq2, check] + [""] * 7 + [name]) + "\n" for rec, seq1, seq2, check, name in DD_ROWS
        )
    )


def write_comma_delimited(directory, quoting=csv.QUOTE_MINIMAL, encoding="utf-8"):
    def write_rows(file_name, rows):
        with open(directory / file_name, mode="w", newline="", encoding=encoding) as f:
            csv.writer(f, quoting=quoting).writerows(rows)

    write_rows("version.txt", [[VERSION_LINE.rsplit(" ", 1)[0], "DUMMYB3Jan00"]])
    write_rows("ina.txt", INA_ROWS)
    write_rows("dda.txt", DDA_ROWS)
    write_rows("dd.txt", [[rec, seq1, seq2, check] + [""] * 7 + [name] for rec, seq1, seq2, check, name in DD_ROWS])


@pytest.mark.parametrize(
    "write_files",
    [
        write_fixed_width,
        write_tab_delimited,
        write_comma_delimited,
        partial(write_comma_delimited, quoting=csv.QUOTE_ALL),
        partial(write_comma_delimited, encoding="utf-8-sig"),
    ],
    ids=["fixed_width", "tab", "comma", "comma_quoted", "comma_bom"],
)
def test_whodrug_reader_b3_txt(tmp_path, write_files):
    write_files(tmp_path)
    reader = WhoDrugReader(None, str(tmp_path))

    metadata = reader._extract_version_metadata()
    assert (metadata.product_type, metadata.format, metadata.version_date) == ("DUMMY", "B3", "20000101")

    df = reader.process_data(metadata)
    assert df.to_dict(orient="records") == [EXPECTED]


def test_whodrug_reader_raises_when_no_level_4_codes(tmp_path):
    write_fixed_width(tmp_path)
    (tmp_path / "INA.txt").write_text("".join(f"{code:<7}{level}{text:<110}\n" for code, level, text in INA_ROWS[:3]))
    reader = WhoDrugReader(None, str(tmp_path))

    with pytest.raises(ValueError, match=r"No level 4 ATC codes found .* \(read as fixed-width\)"):
        reader.process_data(reader._extract_version_metadata())


@pytest.mark.parametrize("missing_file", ["INA", "DDA", "DD"])
def test_whodrug_reader_raises_when_file_missing(tmp_path, missing_file):
    write_fixed_width(tmp_path)
    (tmp_path / f"{missing_file}.txt").unlink()
    reader = WhoDrugReader(None, str(tmp_path))

    with pytest.raises(FileNotFoundError, match=rf"WHODrug file\(s\) {missing_file} not found"):
        reader.process_data(reader._extract_version_metadata())
