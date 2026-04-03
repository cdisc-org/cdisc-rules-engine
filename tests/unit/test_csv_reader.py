import logging
import tempfile
import textwrap
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import InvalidCSVFile
from cdisc_rules_engine.models.dataset import PandasDataset
from cdisc_rules_engine.services.csv_metadata_reader import DatasetCSVMetadataReader
from cdisc_rules_engine.services.data_readers.csv_reader import CSVReader
from core import _validate_csv_data_paths

DEFAULT_ENCODING = "utf-8"


def test_no_tables_csv_raises_error():
    paths = ["/data/variables.csv", "/data/dm.csv"]
    with pytest.raises(InvalidCSVFile):
        _validate_csv_data_paths(paths)


class TestTablesCsvMissingFilenameColumn:
    def test_non_csv_files_excluded_when_no_filename_col(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Name": ["dm"]}).to_csv(tables_csv, index=False)

        dm = tmp_path / "dm.csv"
        readme = tmp_path / "readme.txt"
        dm.touch()
        readme.touch()
        with pytest.raises(InvalidCSVFile):
            _validate_csv_data_paths([str(tables_csv), str(dm), str(readme)])


class TestTablesCsvWithFilenameColumn:
    def test_keeps_only_allowed_datasets(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame(
            {"Filename": ["dm.csv", "customers.csv"], "Label": ["test1", "test2"]}
        ).to_csv(tables_csv, index=False)

        dm = tmp_path / "dm.csv"
        customers = tmp_path / "customers.csv"
        orders = tmp_path / "orders.csv"
        for f in (dm, customers, orders):
            f.touch()

        result = _validate_csv_data_paths(
            [str(tables_csv), str(dm), str(customers), str(orders)]
        )
        assert sorted(result) == sorted([str(dm), str(customers)])
        assert str(orders) not in result

    def test_variables_csv_excluded_even_if_listed(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame(
            {"Filename": ["variables.csv", "dm.csv"], "Label": ["test1", "test2"]}
        ).to_csv(tables_csv, index=False)
        variables = tmp_path / "variables.csv"
        dm = tmp_path / "dm.csv"
        variables.touch()
        dm.touch()

        result = _validate_csv_data_paths([str(tables_csv), str(variables), str(dm)])
        assert str(variables) not in result
        assert str(dm) in result

    def test_filename_with_path_prefix_uses_stem_matching(self, tmp_path):
        """Filename 'subdir/dm.csv' -> stem 'dm' -> matches 'dm.csv' on disk."""
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["subdir/dm.csv"], "Label": ["test1"]}).to_csv(
            tables_csv, index=False
        )
        dm = tmp_path / "dm.csv"
        dm.touch()

        result = _validate_csv_data_paths([str(tables_csv), str(dm)])
        assert str(dm) in result

    def test_nan_filenames_are_ignored(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv", None], "Label": ["test1", None]}).to_csv(
            tables_csv, index=False
        )
        dm = tmp_path / "dm.csv"
        unknown = tmp_path / "unknown.csv"
        dm.touch()
        unknown.touch()

        result = _validate_csv_data_paths([str(tables_csv), str(dm), str(unknown)])
        assert str(dm) in result
        assert str(unknown) not in result

    def test_no_matching_files_returns_empty(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["nonexistent.csv"], "Label": ["test1"]}).to_csv(
            tables_csv, index=False
        )
        dm = tmp_path / "dm.csv"
        dm.touch()

        assert _validate_csv_data_paths([str(tables_csv), str(dm)]) == []

    def test_non_csv_files_excluded_even_if_stem_matches(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"], "Label": ["test1"]}).to_csv(
            tables_csv, index=False
        )

        dm_xlsx = tmp_path / "dm.xlsx"
        dm_xlsx.touch()

        assert _validate_csv_data_paths([str(tables_csv), str(dm_xlsx)]) == []

    def test_encoding_is_forwarded_to_read_csv(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"], "Label": ["test1"]}).to_csv(
            tables_csv, index=False
        )
        (tmp_path / "dm.csv").touch()

        with patch("pandas.read_csv", wraps=pd.read_csv) as mock_read:
            _validate_csv_data_paths(
                [str(tables_csv), str(tmp_path / "dm.csv")], encoding="latin-1"
            )
            mock_read.assert_called_once_with(tables_csv, encoding="latin-1")


class TestEdgeCases:
    def test_only_tables_csv_in_input(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"], "Label": ["test1"]}).to_csv(
            tables_csv, index=False
        )
        assert _validate_csv_data_paths([str(tables_csv)]) == []

    def test_only_variables_csv_in_input(self):
        with pytest.raises(InvalidCSVFile):
            _validate_csv_data_paths(["/data/variables.csv"])

    def test_empty_filename_column_returns_empty(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame(
            {"Filename": pd.Series([], dtype=str), "Label": pd.Series([], dtype=str)}
        ).to_csv(tables_csv, index=False)
        dm = tmp_path / "dm.csv"
        dm.touch()

        assert _validate_csv_data_paths([str(tables_csv), str(dm)]) == []

    def test_all_filename_values_nan_returns_empty(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": [None, None], "Label": [None, None]}).to_csv(
            tables_csv, index=False
        )
        dm = tmp_path / "dm.csv"
        dm.touch()

        assert _validate_csv_data_paths([str(tables_csv), str(dm)]) == []

    def test_duplicate_paths_removed(self, tmp_path):
        """The function does not deduplicate; duplicates in -> duplicates out."""
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"], "Label": ["test1"]}).to_csv(
            tables_csv, index=False
        )
        dm = tmp_path / "dm.csv"
        dm.touch()

        paths = [str(tables_csv), str(dm), str(dm)]
        result = _validate_csv_data_paths(paths)
        assert result.count(str(dm)) == 1


VARIABLES_CSV = textwrap.dedent(
    """\
    dataset,variable,label,type,length
    patients.csv,id,Patient ID,integer,10
    patients.csv,name,Patient Name,string,50
    patients.csv,age,Patient Age,integer,3
"""
)

DATA_CSV = textwrap.dedent(
    """\
    id,name,age
    1,Alice,30
    2,Bob,25
    3,Carol,40
"""
)

TABLES_CSV = textwrap.dedent(
    """\
    Filename,Label
    patients.csv,Patient Dataset
"""
)


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


class TestDatasetCSVMetadataReaderRead:
    """Tests for DatasetCSVMetadataReader.read()"""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.data_path = Path(self.tmpdir) / "patients.csv"
        _write(self.data_path, DATA_CSV)

    def _variables_path(self):
        return Path(self.tmpdir) / "variables.csv"

    def test_returns_dict_with_expected_keys(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        result = reader.read()

        expected_keys = {
            "dataset_name",
            "variable_names",
            "variable_labels",
            "variable_formats",
            "variable_name_to_label_map",
            "variable_name_to_data_type_map",
            "variable_name_to_size_map",
            "number_of_variables",
            "dataset_modification_date",
            "adam_info",
            "dataset_length",
            "first_record",
        }
        assert expected_keys.issubset(result.keys())

    def test_variable_names_and_labels(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        result = reader.read()
        assert result["variable_names"] == ["id", "name", "age"]
        assert result["variable_labels"] == [
            "Patient ID",
            "Patient Name",
            "Patient Age",
        ]

    def test_number_of_variables(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        assert reader.read()["number_of_variables"] == 3

    def test_variable_formats_are_empty_strings(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        result = reader.read()
        assert all(fmt == "" for fmt in result["variable_formats"])

    def test_dataset_length_equals_data_rows(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        # DATA_CSV has 3 data rows (+ 1 header)
        assert reader.read()["dataset_length"] == 3

    def test_first_record_matches_first_data_row(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        first = reader.read()["first_record"]
        assert first == {"id": "1", "name": "Alice", "age": "30"}

    def test_modification_date_is_iso_string(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        mod_date = reader.read()["dataset_modification_date"]
        # Should parse without raising
        datetime.fromisoformat(mod_date)

    def test_adam_info_structure(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        adam = reader.read()["adam_info"]
        assert adam == {
            "categorization_scheme": {},
            "w_indexes": {},
            "period": {},
            "selection_algorithm": {},
        }

    def test_variable_name_to_label_map(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        m = reader.read()["variable_name_to_label_map"]
        assert m == {"id": "Patient ID", "name": "Patient Name", "age": "Patient Age"}

    def test_variable_name_to_size_map_with_values(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        sizes = reader.read()["variable_name_to_size_map"]
        assert sizes == {"id": 10, "name": 50, "age": 3}

    def test_variable_name_to_size_map_with_nan_length(self):
        variables_with_nan = textwrap.dedent(
            """\
            dataset,variable,label,type,length
            patients.csv,id,Patient ID,integer,
        """
        )
        _write(self._variables_path(), variables_with_nan)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        sizes = reader.read()["variable_name_to_size_map"]
        assert sizes["id"] is None

    def test_dataset_name_lookup_is_case_insensitive(self):
        """File name with mixed case should still match variables.csv entry."""
        variables_upper = VARIABLES_CSV.replace("patients.csv", "PATIENTS.CSV")
        _write(self._variables_path(), variables_upper)
        reader = DatasetCSVMetadataReader(str(self.data_path), "PATIENTS.CSV")
        result = reader.read()
        assert result["dataset_name"] == "PATIENTS"

    def test_returns_partial_meta_when_no_variables_file(self, caplog):
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        with caplog.at_level(logging.INFO, logger="validator"):
            result = reader.read()
        assert {
            "dataset_name",
            "dataset_modification_date",
            "adam_info",
            "dataset_length",
            "first_record",
        }.issubset(set(result.keys()))
        assert result["first_record"] == {"age": "30", "id": "1", "name": "Alice"}
        assert "No variables file found" in caplog.text

    def test_dataset_label_added_when_tables_csv_present(self):
        _write(self._variables_path(), VARIABLES_CSV)
        _write(Path(self.tmpdir) / "tables.csv", TABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        result = reader.read()
        assert result.get("dataset_label") == "Patient Dataset"

    def test_no_dataset_label_when_tables_csv_absent(self):
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(self.data_path), "patients.csv")
        result = reader.read()
        assert "dataset_label" not in result

    def test_empty_data_file_returns_empty_first_record(self):
        empty_data = Path(self.tmpdir) / "patients.csv"
        _write(empty_data, "id,name,age\n")  # header only
        _write(self._variables_path(), VARIABLES_CSV)
        reader = DatasetCSVMetadataReader(str(empty_data), "patients.csv")
        result = reader.read()
        assert result["dataset_length"] == 0
        assert result["first_record"] == {}


class TestCSVReaderFromFile:
    """Tests for CSVReader.from_file()"""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.csv_path = Path(self.tmpdir) / "data.csv"
        _write(self.csv_path, DATA_CSV)

    def test_returns_dataframe(self):
        reader = CSVReader()
        df = reader.from_file(str(self.csv_path))
        assert isinstance(df, PandasDataset)

    def test_correct_number_of_rows(self):
        reader = CSVReader()
        df = reader.from_file(str(self.csv_path))
        assert len(df) == 3

    def test_correct_column_names(self):
        reader = CSVReader()
        df = reader.from_file(str(self.csv_path))
        assert list(df.columns) == ["id", "name", "age"]

    def test_correct_values(self):
        reader = CSVReader()
        df = reader.from_file(str(self.csv_path))
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[1]["age"] == 25

    def test_no_index_column(self):
        """index_col=False means the DataFrame index should be the default RangeIndex."""
        reader = CSVReader()
        df = reader.from_file(str(self.csv_path))
        assert list(df.index) == [0, 1, 2]

    def test_empty_csv_returns_empty_dataframe(self):
        empty_path = Path(self.tmpdir) / "empty.csv"
        _write(empty_path, "id,name,age\n")
        reader = CSVReader()
        df = reader.from_file(str(empty_path))
        assert df.empty
        assert list(df.columns) == ["id", "name", "age"]


class TestCSVReaderToParquet:

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()

    def _write_csv(self, name: str, content: str) -> str:
        p = Path(self.tmpdir) / name
        _write(p, content)
        return str(p)

    def test_returns_tuple_of_int_and_string(self):
        path = self._write_csv("data.csv", DATA_CSV)
        reader = CSVReader()
        result = reader.to_parquet(path)
        assert isinstance(result, tuple)
        num_rows, parquet_path = result
        assert isinstance(num_rows, int)
        assert isinstance(parquet_path, str)

    def test_row_count_matches_csv(self):
        path = self._write_csv("data.csv", DATA_CSV)
        reader = CSVReader()
        num_rows, _ = reader.to_parquet(path)
        assert num_rows == 3

    def test_parquet_file_is_created(self):
        path = self._write_csv("data.csv", DATA_CSV)
        reader = CSVReader()
        _, parquet_path = reader.to_parquet(path)
        assert Path(parquet_path).exists()

    def test_parquet_content_matches_source(self):
        path = self._write_csv("data.csv", DATA_CSV)
        reader = CSVReader()
        _, parquet_path = reader.to_parquet(path)
        df = pd.read_parquet(parquet_path)
        assert list(df.columns) == ["id", "name", "age"]
        assert len(df) == 3

    def test_large_file_chunked_row_count(self):
        """Simulate a file larger than the 20 000-row chunk boundary."""
        rows = "\n".join(f"{i},Name{i},{20 + i % 50}" for i in range(1, 25_001))
        content = "id,name,age\n" + rows
        path = self._write_csv("large.csv", content)
        reader = CSVReader()
        num_rows, parquet_path = reader.to_parquet(path)
        assert num_rows == 25_000
        df = pd.read_parquet(parquet_path)
        assert len(df) == 25_000

    def test_parquet_suffix(self):
        path = self._write_csv("data.csv", DATA_CSV)
        reader = CSVReader()
        _, parquet_path = reader.to_parquet(path)
        assert parquet_path.endswith(".parquet")

    def test_empty_csv_returns_zero_rows(self):
        path = self._write_csv("empty.csv", "id,name,age\n")
        reader = CSVReader()
        num_rows, parquet_path = reader.to_parquet(path)
        assert num_rows == 0
