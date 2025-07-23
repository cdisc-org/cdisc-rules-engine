import pytest
from cdisc_rules_engine.readers.metadata_standards_reader import MetadataStandardsReader


ADAM_DATASET_KEYS = [
    "standard_type",
    "version",
    "structure_name",
    "structure_description",
    "class",
    "subclass",
    "notes",
]

SDTM_DATASET_KEYS = ["standard_type", "version", "class", "dataset_name", "dataset_label", "structure"]

ADAM_VARIABLE_KEYS = [
    "standard_type",
    "version",
    "structure_name",
    "variable_set",
    "variable_name",
    "variable_label",
    "type",
    "codelist_code",
    "submission_value",
    "value_domain",
    "value_list",
    "core",
    "notes",
]

SDTM_VARIABLE_KEYS = [
    "standard_type",
    "version",
    "variable_order",
    "class",
    "dataset_name",
    "variable_name",
    "variable_label",
    "type",
    "codelist_code",
    "submission_value",
    "value_domain",
    "value_list",
    "role",
    "notes",
    "core",
]

EXPECTED_COUNTS = {
    "ADaMIG_MD_v1.0.xlsx": {"datasets": 4, "variables": 25},
    "ADaMIG_NCA_v1.0.xlsx": {"datasets": 1, "variables": 59},
    "ADaMIG_v1.0.xlsx": {"datasets": 2, "variables": 205},
    "ADaMIG_v1.1.xlsx": {"datasets": 2, "variables": 310},
    "ADaMIG_v1.2.xlsx": {"datasets": 2, "variables": 336},
    "ADaMIG_v1.3.xlsx": {"datasets": 3, "variables": 336},
    "SDTMIG-AP_v1.0.xlsx": {"datasets": 2, "variables": 22},
    "SDTMIG-MD_v1.0.xlsx": {"datasets": 7, "variables": 111},
    "SDTMIG-MD_v1.1.xlsx": {"datasets": 7, "variables": 110},
    "SDTMIG_v3.1.2.xlsx": {"datasets": 32, "variables": 714},
    "SDTMIG_v3.1.3.xlsx": {"datasets": 35, "variables": 818},
    "SDTMIG_v3.2.xlsx": {"datasets": 46, "variables": 1142},
    "SDTMIG_v3.3.xlsx": {"datasets": 59, "variables": 1723},
    "SDTMIG_v3.4.xlsx": {"datasets": 63, "variables": 1917},
}


@pytest.fixture
def ig_directory(resources_directory):
    """Get the path to the IG files directory."""
    return resources_directory / "igs"


def get_all_ig_files(ig_directory):
    """Get all IG files from the directory."""
    return sorted(ig_directory.glob("*IG*_v*.xlsx"))


def test_filename_pattern_valid():
    """Test regex pattern matches valid filenames including dash/underscore variants."""
    pattern = MetadataStandardsReader.FILENAME_PATTERN

    valid_names = [
        ("ADaMIG_v1.0.xlsx", "ADaMIG"),
        ("ADaMIG_MD_v1.0.xlsx", "ADaMIG_MD"),
        ("ADaMIG_NCA_v1.0.xlsx", "ADaMIG_NCA"),
        ("SDTMIG_v3.2.xlsx", "SDTMIG"),
        ("SDTMIG-AP_v1.0.xlsx", "SDTMIG-AP"),
        ("SDTMIG-MD_v1.0.xlsx", "SDTMIG-MD"),
        ("SDTMIG-MD_v1.1.xlsx", "SDTMIG-MD"),
    ]

    for name, expected_standard_name in valid_names:
        match = pattern.match(name)
        assert match is not None, f"Should match: {name}"
        assert (
            match.group("standard_name") == expected_standard_name
        ), f"Wrong standard_name for {name}: got {match.group('standard_name')}, expected {expected_standard_name}"


def test_filename_pattern_invalid():
    """Test regex pattern rejects invalid filenames."""
    pattern = MetadataStandardsReader.FILENAME_PATTERN

    invalid_names = [
        "AdamIG_v1.0.xlsx",
        "ADaMIG_1.0.xlsx",
        "ADaMIG_v1.xlsx",
        "ADaMIG_v1.0.pdf",
        "IG_v1.0.xlsx",
        "SDTMIGAP_v1.0.xlsx",
        "SDTMIGMD_v1.0.xlsx",
    ]

    for name in invalid_names:
        assert pattern.match(name) is None, f"Should not match: {name}"


def test_metadata_extraction_all_files(ig_directory):
    """Test metadata extraction from all actual files."""
    files = get_all_ig_files(ig_directory)

    for file_path in files:
        reader = MetadataStandardsReader(str(file_path))

        assert reader.metadata.standard_type in ["ADaM", "SDTM"]
        assert reader.metadata.version is not None
        assert reader.metadata.extension in ["xlsx", "xls"]

        if "-" in file_path.name or "_" in file_path.name.replace("IG_", ""):
            assert len(reader.metadata.standard_name) > 6  # More than just "ADaMIG" or "SDTMIG"


def test_all_files_readable(ig_directory):
    """Test that all IG files can be read without errors."""
    files = get_all_ig_files(ig_directory)

    assert len(files) > 0, "No IG files found in test directory"

    for file_path in files:
        try:
            reader = MetadataStandardsReader(str(file_path))
            data = reader.read()
            assert isinstance(data, dict), f"Failed to read {file_path.name}"
            assert "datasets" in data
            assert "variables" in data
        except Exception as e:
            pytest.fail(f"Failed to read {file_path.name}: {str(e)}")


def test_row_counts(ig_directory):
    """Test that each file returns the expected number of rows."""
    files = get_all_ig_files(ig_directory)

    for file_path in files:
        reader = MetadataStandardsReader(str(file_path))
        data = reader.read()

        expected_count = EXPECTED_COUNTS.get(file_path.name)
        assert len(data["datasets"]) == expected_count["datasets"], (
            f"{file_path.name}: Expected {expected_count['datasets']} datasets, " f"got {len(data['datasets'])}"
        )
        assert len(data["variables"]) == expected_count["variables"], (
            f"{file_path.name}: Expected {expected_count['variables']} variables, " f"got {len(data['variables'])}"
        )


def test_keys_structure(ig_directory):
    """Test that all rows have the correct keys based on standard type."""
    files = get_all_ig_files(ig_directory)

    for file_path in files:
        reader = MetadataStandardsReader(str(file_path))
        data = reader.read()

        if len(data["datasets"]) == 0 and len(data["variables"]) == 0:
            continue

        if reader.metadata.standard_type == "ADaM":
            expected_dataset_keys = ADAM_DATASET_KEYS
            expected_variable_keys = ADAM_VARIABLE_KEYS
        elif reader.metadata.standard_type == "SDTM":
            expected_dataset_keys = SDTM_DATASET_KEYS
            expected_variable_keys = SDTM_VARIABLE_KEYS
        else:
            pytest.fail(f"Unknown standard type: {reader.metadata.standard_type} in {file_path.name}")

        # Check dataset keys (if any datasets exist)
        if data["datasets"]:
            first_dataset = data["datasets"][0]
            actual_keys = list(first_dataset.keys())
            assert actual_keys == expected_dataset_keys, (
                f"{file_path.name} dataset keys don't match.\n"
                f"Expected: {expected_dataset_keys}\n"
                f"Actual: {actual_keys}"
            )

        if data["variables"]:
            rows_to_check = min(5, len(data["variables"]))
            for i in range(rows_to_check):
                variable = data["variables"][i]
                actual_keys = list(variable.keys())
                assert actual_keys == expected_variable_keys, (
                    f"{file_path.name} variable row {i} keys don't match.\n"
                    f"Expected: {expected_variable_keys}\n"
                    f"Actual: {actual_keys}"
                )


def test_data_values_populated(ig_directory):
    """Test that data values are properly populated for all files."""
    files = get_all_ig_files(ig_directory)

    for file_path in files:
        reader = MetadataStandardsReader(str(file_path))
        data = reader.read()

        if len(data["datasets"]) == 0:
            continue

        if data["datasets"]:
            for dataset in data["datasets"]:
                assert dataset["standard_type"] == reader.metadata.standard_type
                assert dataset["version"] == reader.metadata.version

                if reader.metadata.standard_type == "ADaM":
                    assert dataset.get("structure_name") is not None
                elif reader.metadata.standard_type == "SDTM":
                    assert dataset.get("dataset_name") is not None
                else:
                    pytest.fail(f"Unknown standard type: {reader.metadata.standard_type} in {file_path.name}")


def test_variable_values_populated(ig_directory):
    """Test that variable values are properly populated for all files."""
    files = get_all_ig_files(ig_directory)

    for file_path in files:
        reader = MetadataStandardsReader(str(file_path))
        data = reader.read()

        if len(data["variables"]) == 0:
            continue

        if data["variables"]:
            non_none_counts = {"variable_name": 0, "variable_label": 0, "type": 0, "core": 0}

            for var in data["variables"]:
                assert var["standard_type"] == reader.metadata.standard_type
                assert var["version"] == reader.metadata.version
                assert var["variable_name"] is not None

                for field in non_none_counts:
                    if var.get(field) is not None:
                        non_none_counts[field] += 1

            for field, count in non_none_counts.items():
                assert count > 0, f"All {field} values are None in {file_path.name}"


def test_sheet_name_handling(ig_directory):
    """Test that correct sheet names are used based on standard type."""
    adam_file = ig_directory / "ADaMIG_v1.0.xlsx"
    if adam_file.exists():
        reader = MetadataStandardsReader(str(adam_file))
        assert reader.DATA_STRUCTURES_SHEET == "Data Structures"
        data = reader.read()
        assert len(data["datasets"]) > 0

    sdtm_file = ig_directory / "SDTMIG_v3.4.xlsx"
    if sdtm_file.exists():
        reader = MetadataStandardsReader(str(sdtm_file))
        assert reader.DATASETS_SHEET == "Datasets"
        data = reader.read()
        assert len(data["datasets"]) > 0
