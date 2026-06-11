import pytest
from pathlib import Path
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
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
    "sdtmct-2014-09-26.pkl": 8994,
    "sdtmct-2014-12-19.pkl": 9524,
    "sdtmct-2015-03-27.pkl": 10131,
    "sdtmct-2015-06-26.pkl": 10544,
    "sdtmct-2015-09-25.pkl": 10731,
    "sdtmct-2015-12-18.pkl": 17356,
    "sdtmct-2016-06-24.pkl": 18575,
    "sdtmct-2016-09-30.pkl": 18546,
    "sdtmct-2016-12-16.pkl": 20131,
    "sdtmct-2017-03-31.pkl": 20582,
    "sdtmct-2017-06-30.pkl": 21977,
    "sdtmct-2017-09-29.pkl": 23552,
    "sdtmct-2017-12-22.pkl": 24831,
    "sdtmct-2018-03-30.pkl": 25980,
    "sdtmct-2018-06-29.pkl": 27105,
    "sdtmct-2018-09-28.pkl": 28129,
    "sdtmct-2018-12-21.pkl": 28397,
    "sdtmct-2019-03-29.pkl": 28590,
    "sdtmct-2019-06-28.pkl": 29095,
    "sdtmct-2019-09-27.pkl": 29684,
    "sdtmct-2019-12-20.pkl": 30278,
    "sdtmct-2020-03-27.pkl": 30600,
    "sdtmct-2020-06-26.pkl": 30995,
    "sdtmct-2020-11-06.pkl": 31198,
    "sdtmct-2021-03-26.pkl": 32354,
    "sdtmct-2021-06-25.pkl": 33462,
    "sdtmct-2021-09-24.pkl": 34014,
    "sdtmct-2021-12-17.pkl": 36786,
    "sdtmct-2022-03-25.pkl": 37497,
    "sdtmct-2022-06-24.pkl": 38333,
    "sdtmct-2022-09-30.pkl": 38953,
    "sdtmct-2022-12-16.pkl": 39612,
    "sdtmct-2023-03-31.pkl": 40155,
    "sdtmct-2023-06-30.pkl": 40870,
    "sdtmct-2023-09-29.pkl": 41393,
    "sdtmct-2023-12-15.pkl": 40816,
    "sdtmct-2024-03-29.pkl": 41232,
    "sdtmct-2024-09-27.pkl": 43976,
    "sdtmct-2025-03-28.pkl": 44856,
}


def get_codelist_files(directory: str):
    extensions = ["*.xlsx", "*.xls", "*.csv", "*.pkl"]
    files = []
    for ext in extensions:
        files.extend(Path(directory).glob(ext))
    valid_files = [f for f in files if CodelistReader.FILENAME_PATTERN.match(f.name)]
    return sorted(valid_files)


@pytest.fixture
def test_files():
    dir = DefaultFilePaths.CACHE.value
    files = get_codelist_files(dir)
    if not files:
        pytest.skip(f"No valid codelist files found in {dir}")
    return files


def test_metadata_extraction(test_files):
    for file_path in test_files:
        reader = CodelistReader(file_path)
        metadata = reader.metadata

        assert metadata.standard_type.lower() == "sdtm"
        assert metadata.version_date is not None
        assert metadata.extension in ["csv", "xlsx", "xls", "pkl"]


def test_all_files_readable(test_files):
    for file_path in test_files:
        reader = CodelistReader(file_path)
        data = reader.read()

        assert isinstance(data, list), f"Failed to read {file_path.name} as a list"
        if len(data) > 0:
            assert isinstance(data[0], dict), f"Data in {file_path.name} items are not dicts"


def test_row_counts(test_files):
    for file_path in test_files:
        reader = CodelistReader(file_path)
        data = reader.read()

        expected_count = EXPECTED_ROW_COUNTS.get(file_path.name)
        if expected_count is not None:
            assert len(data) == expected_count, f"{file_path.name}: Expected {expected_count} rows, got {len(data)}"


def test_keys_structure(test_files):
    for file_path in test_files:
        reader = CodelistReader(file_path)
        data = reader.read()

        if not data:
            continue

        for i in range(min(100, len(data))):
            actual_keys = list(data[i].keys())
            assert actual_keys == CODELIST_KEYS, (
                f"{file_path.name} row {i}: Keys mismatch.\n" f"Expected: {CODELIST_KEYS}\n" f"Actual: {actual_keys}"
            )


def test_data_values_populated(test_files):
    for file_path in test_files:
        reader = CodelistReader(file_path)
        data = reader.read()

        if not data:
            continue

        first_row = data[0]
        for col in CODELIST_KEYS:
            assert first_row[col] is not None
