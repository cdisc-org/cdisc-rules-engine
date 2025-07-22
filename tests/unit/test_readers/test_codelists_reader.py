from cdisc_rules_engine.readers.codelist_reader import CodelistReader

TEST_XLSX_PATH = "tests/resources/codelists/ADaM_CT_2014-09-26.xlsx"


def test_read_codelist_xlsx():
    """Test reading a codelist xlsx file."""
    reader = CodelistReader(TEST_XLSX_PATH)
    data = reader.read()
    raise ValueError(
        f"Data read from codelist: {data}"
    )  # debugging line dont freak out
    assert isinstance(data, list)
    assert len(data) > 0
    assert all(isinstance(row, dict) for row in data)
