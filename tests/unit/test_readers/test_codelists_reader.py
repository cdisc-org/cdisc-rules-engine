import pytest
from cdisc_rules_engine.readers.codelist_reader import CodelistReader

CODELIST_KEYS = [
    "standard_type",
    "version_date",
    "item_code",
    "codelist_code",
    "extensible",
    "name",
    "value",
    "synonym",
    "definition",
    "term",
    "standard_and_date",
]

EXPECTED_ROW_COUNTS = {
    "ADaM_CT_2014-09-26.xlsx": 37,
    "ADaM_CT_2015-12-18.xlsx": 40,
    "ADaM_CT_2016-03-25.xlsx": 42,
    "ADaM_CT_2016-09-30.xlsx": 42,
    "ADaM_CT_2016-12-16.xlsx": 41,
    "ADaM_CT_2017-03-31.xlsx": 45,
    "ADaM_CT_2017-09-29.xlsx": 46,
    "ADaM_CT_2018-12-21.xlsx": 46,
    "ADaM_CT_2019-03-29.xlsx": 50,
    "ADaM_CT_2019-12-20.xlsx": 43,
    "ADaM_CT_2020-03-27.xlsx": 46,
    "ADaM_CT_2020-06-26.xlsx": 50,
    "ADaM_CT_2020-11-06.xlsx": 52,
    "ADaM_CT_2021-12-17.xlsx": 53,
    "ADaM_CT_2022-06-24.xlsx": 115,
    "ADaM_CT_2023-03-31.xlsx": 121,
    "ADaM_CT_2023-06-30.xlsx": 121,
    "ADaM_CT_2024-03-29.xlsx": 122,
    "ADaM_CT_2024-09-27.xlsx": 158,
    "ADaM_CT_2025-03-28.xlsx": 163,
    "SDTM_CT_2014-09-26.xlsx": 8994,
    "SDTM_CT_2014-12-19.xlsx": 9524,
    "SDTM_CT_2015-03-27.xlsx": 10131,
    "SDTM_CT_2015-06-26.xlsx": 10544,
    "SDTM_CT_2015-09-25.xlsx": 10731,
    "SDTM_CT_2015-12-18.xlsx": 17356,
    "SDTM_CT_2016-06-24.xlsx": 18575,
    "SDTM_CT_2016-09-30.xlsx": 18546,
    "SDTM_CT_2016-12-16.xlsx": 20131,
    "SDTM_CT_2017-03-31.xlsx": 20582,
    "SDTM_CT_2017-06-30.xlsx": 21977,
    "SDTM_CT_2017-09-29.xlsx": 23552,
    "SDTM_CT_2017-12-22.xlsx": 24831,
    "SDTM_CT_2018-03-30.xlsx": 25980,
    "SDTM_CT_2018-06-29.xlsx": 27105,
    "SDTM_CT_2018-09-28.xlsx": 28129,
    "SDTM_CT_2018-12-21.xlsx": 28397,
    "SDTM_CT_2019-03-29.xlsx": 28590,
    "SDTM_CT_2019-06-28.xlsx": 29095,
    "SDTM_CT_2019-09-27.xlsx": 29684,
    "SDTM_CT_2019-12-20.xlsx": 30278,
    "SDTM_CT_2020-03-27.xlsx": 30600,
    "SDTM_CT_2020-06-26.xlsx": 30995,
    "SDTM_CT_2020-11-06.xlsx": 31198,
    "SDTM_CT_2021-03-26.xlsx": 32354,
    "SDTM_CT_2021-06-25.xlsx": 33462,
    "SDTM_CT_2021-09-24.xlsx": 34014,
    "SDTM_CT_2021-12-17.xlsx": 36786,
    "SDTM_CT_2022-03-25.xlsx": 37497,
    "SDTM_CT_2022-06-24.xlsx": 38333,
    "SDTM_CT_2022-09-30.xlsx": 38953,
    "SDTM_CT_2022-12-16.xlsx": 39612,
    "SDTM_CT_2023-03-31.xlsx": 40155,
    "SDTM_CT_2023-06-30.xlsx": 40870,
    "SDTM_CT_2023-09-29.xlsx": 41393,
    "SDTM_CT_2023-12-15.xlsx": 40816,
    "SDTM_CT_2024-03-29.xlsx": 41232,
    "SDTM_CT_2024-09-27.xlsx": 43976,
    "SDTM_CT_2025-03-28.xlsx": 44856,
}


def get_all_codelist_files(directory):
    """Get all codelist files from the directory."""
    files = []
    files.extend(directory.glob("*_CT_*.xlsx"))
    return sorted(files)


def test_all_files_readable(resources_directory):
    """Test that all codelist files can be read without errors."""
    files = get_all_codelist_files(resources_directory / "codelists")

    assert len(files) > 0, "No codelist files found in test directory"

    for file_path in files:
        try:
            reader = CodelistReader(str(file_path))
            data = reader.read()
            assert isinstance(data, list), f"Failed to read {file_path.name}"
        except Exception as e:
            pytest.fail(f"Failed to read {file_path.name}: {str(e)}")


def test_row_counts(resources_directory):
    """Test that each file returns the expected number of rows."""
    files = get_all_codelist_files(resources_directory / "codelists")

    for file_path in files:
        reader = CodelistReader(str(file_path))
        data = reader.read()

        expected_count = EXPECTED_ROW_COUNTS.get(file_path.name)
        if expected_count is not None:
            assert len(data) == expected_count, (
                f"{file_path.name}: Expected {expected_count} rows, "
                f"got {len(data)} rows"
            )
        else:
            print(f"{file_path.name}: {len(data)} rows")


def test_keys_structure(resources_directory):
    """Test that all rows have the correct keys in the correct order."""
    files = get_all_codelist_files(resources_directory / "codelists")

    for file_path in files:
        reader = CodelistReader(str(file_path))
        data = reader.read()

        if len(data) == 0:
            continue

        rows_to_check = min(10, len(data))
        for i in range(rows_to_check):
            row = data[i]
            actual_keys = list(row.keys())

            assert actual_keys == CODELIST_KEYS, (
                f"{file_path.name} row {i}: Keys don't match.\n"
                f"Expected: {CODELIST_KEYS}\n"
                f"Actual: {actual_keys}"
            )


def test_metadata_extraction(resources_directory):
    """Test that metadata is correctly extracted from all files."""
    files = get_all_codelist_files(resources_directory / "codelists")

    for file_path in files:
        reader = CodelistReader(str(file_path))
        assert reader.metadata.standard_type in ["ADaM", "SDTM"]
        assert reader.metadata.version_date is not None
        assert reader.metadata.extension in ["csv", "xlsx", "xls"]


def test_data_values_populated(resources_directory):
    """Test that data values are correctly populated."""
    files = get_all_codelist_files(resources_directory / "codelists")

    for file_path in files:
        reader = CodelistReader(str(file_path))
        data = reader.read()

        if len(data) == 0:
            continue

        first_row = data[0]

        # Fully populated columns
        assert first_row["standard_type"] is not None
        assert first_row["version_date"] is not None
        assert first_row["name"] is not None
        assert first_row["value"] is not None

        # Partially populated columns
        optional_fields = ["synonym", "definition", "term"]
        for field in optional_fields:
            if field in first_row:
                non_none_count = sum(
                    1 for row in data[:100] if row.get(field) is not None
                )
                assert (
                    non_none_count > 0
                ), f"All {field} values are None in {file_path.name}"
