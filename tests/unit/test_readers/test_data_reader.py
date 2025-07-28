import pytest
from cdisc_rules_engine.readers.data_reader import DataReader


ADAM_DOMAINS = ["ADAE", "ADEF", "ADSL", "ADTTE"]
SDTM_DOMAINS = ["AE", "DM", "EX", "LB", "SUPPDM", "TA", "TD", "TE", "TI", "TS", "TV", "XP"]


@pytest.fixture
def clinical_data_directory(resources_directory):
    """Get the clinical data directory."""
    return resources_directory / "clinical_data"


def get_all_data_files(clinical_data_directory):
    """Get all clinical data files."""

    xpt_adam_files = list((clinical_data_directory / "xpt" / "adam").glob("*.xpt"))
    xpt_sdtm_files = list((clinical_data_directory / "xpt" / "sdtm").glob("*.xpt"))

    sas_adam_files = list((clinical_data_directory / "sas7bdat" / "adam").glob("*.sas7bdat"))
    sas_sdtm_files = list((clinical_data_directory / "sas7bdat" / "sdtm").glob("*.sas7bdat"))

    return sorted(xpt_adam_files + xpt_sdtm_files + sas_adam_files + sas_sdtm_files)


def test_metadata_extraction(clinical_data_directory):
    """Test metadata extraction for all files."""
    files = get_all_data_files(clinical_data_directory)

    assert len(files) > 0, "No clinical data files found"

    for file_path in files:
        reader = DataReader(str(file_path))

        assert reader.metadata.domain.upper() in ADAM_DOMAINS + SDTM_DOMAINS
        assert reader.metadata.standard_type in ("ADaM", "SDTM")
        assert reader.metadata.file_format in ("xpt", "sas7bdat")


def test_read_metadata_only(clinical_data_directory):
    """Test reading metadata without loading data."""
    files = get_all_data_files(clinical_data_directory)

    for file_path in files[:5]:
        try:
            reader = DataReader(str(file_path))
            metadata_result = reader.read_metadata()

            assert isinstance(metadata_result, dict)
            assert "metadata" in metadata_result
            assert "variables" in metadata_result
            assert "data" not in metadata_result

            metadata = metadata_result["metadata"]
            assert metadata["name"] == file_path.name
            assert metadata["domain"].upper() in ADAM_DOMAINS + SDTM_DOMAINS
            assert metadata["standard_type"] in ("ADaM", "SDTM")
            assert metadata["record_count"] >= 0
            assert metadata["variable_count"] > 0

        except Exception as e:
            pytest.fail(f"Failed to read metadata for {file_path.name}: {str(e)}")


def test_streaming_functionality(clinical_data_directory):
    """Test streaming data in chunks."""

    files = get_all_data_files(clinical_data_directory)
    for data_file in files:
        if data_file.exists():
            reader = DataReader(str(data_file))

            chunk_count = 0
            total_records = 0

            for chunk in reader.read():
                chunk_count += 1
                assert isinstance(chunk, list)
                assert len(chunk) > 0
                total_records += len(chunk)

                if chunk_count >= 3:
                    break

            assert chunk_count > 0, "No chunks were yielded"
            assert total_records > 0, "No records were found"


def test_variable_metadata_extraction(clinical_data_directory):
    """Test variable metadata extraction."""
    files = get_all_data_files(clinical_data_directory)
    for data_file in files:
        if data_file.exists():
            reader = DataReader(str(data_file))
            metadata_result = reader.read_metadata()

            variables = metadata_result["variables"]
            assert len(variables) > 0

            for var in variables:
                assert "name" in var
                assert "label" in var
                assert "format" in var
                assert "ctype" in var or "type" in var
                assert "length" in var


def test_standard_type_classification(clinical_data_directory):
    """Test that files are correctly classified as ADaM or SDTM."""
    files = get_all_data_files(clinical_data_directory)
    for file_path in files:
        reader = DataReader(str(file_path))
        domain = reader.metadata.domain.upper()

        if domain in ADAM_DOMAINS:
            assert reader.metadata.standard_type == "ADaM", f"{domain} should be classified as ADaM"
        elif domain in SDTM_DOMAINS:
            assert reader.metadata.standard_type == "SDTM", f"{domain} should be classified as SDTM"


def test_file_format_support(clinical_data_directory):
    """Test reading both XPT and SAS7BDAT formats."""
    files = get_all_data_files(clinical_data_directory)
    for data_file in files:
        if data_file.exists():
            reader = DataReader(str(data_file))
            assert reader.metadata.file_format in (
                "xpt",
                "sas7bdat",
            ), f"Unsupported file format: {reader.metadata.file_format} for {data_file.name}"

            first_chunk = next(reader.read())
            assert isinstance(first_chunk, list)
            assert len(first_chunk) > 0


def test_memory_efficiency(clinical_data_directory):
    """Test that metadata can be read without loading all data."""
    files = get_all_data_files(clinical_data_directory)
    for data_file in files:
        if data_file.exists():
            reader = DataReader(str(data_file))

            import tracemalloc

            tracemalloc.start()

            metadata_result = reader.read_metadata()
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            memory_mb = peak / 1024 / 1024
            assert memory_mb < 50, f"Memory usage too high: {memory_mb:.2f} MB"

            assert metadata_result["metadata"]["record_count"] > 0
            assert len(metadata_result["variables"]) > 0


def test_chunk_iteration(clinical_data_directory):
    """Test iterating through all chunks of a file."""
    files = get_all_data_files(clinical_data_directory)
    for data_file in files:
        if data_file.exists():
            reader = DataReader(str(data_file))

            metadata = reader.read_metadata()
            expected_records = metadata["metadata"]["record_count"]

            actual_records = 0
            for chunk in reader.read():
                actual_records += len(chunk)

            assert abs(actual_records - expected_records) <= reader.CHUNKSIZE
