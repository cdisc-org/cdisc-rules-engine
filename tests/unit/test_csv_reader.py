from unittest.mock import patch

import pandas as pd

from core import _filter_dataset_paths

DEFAULT_ENCODING = "utf-8"


class TestNoTablesCsv:
    def test_returns_all_csv_files(self):
        paths = ["/data/dm.csv", "/data/customers.csv"]
        result = _filter_dataset_paths(paths)
        assert sorted(result) == sorted(paths)

    def test_excludes_non_csv_files(self):
        paths = ["/data/dm.csv", "/data/readme.txt", "/data/report.xlsx"]
        assert _filter_dataset_paths(paths) == ["/data/dm.csv"]

    def test_only_non_csv_returns_empty(self):
        assert _filter_dataset_paths(["/data/readme.txt", "/data/image.png"]) == []

    def test_variables_csv_excluded_without_tables_csv(self):
        paths = ["/data/dm.csv", "/data/variables.csv"]
        result = _filter_dataset_paths(paths)
        assert result == ["/data/dm.csv"]
        assert "/data/variables.csv" not in result


class TestTablesCsvMissingFilenameColumn:
    def test_returns_all_csv_dataset_files(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Name": ["dm"]}).to_csv(tables_csv, index=False)

        dm = tmp_path / "dm.csv"
        customers = tmp_path / "customers.csv"
        dm.touch()
        customers.touch()

        paths = [str(tables_csv), str(dm), str(customers)]
        result = _filter_dataset_paths(paths)
        assert sorted(result) == sorted([str(dm), str(customers)])

    def test_non_csv_files_excluded_when_no_filename_col(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Name": ["dm"]}).to_csv(tables_csv, index=False)

        dm = tmp_path / "dm.csv"
        readme = tmp_path / "readme.txt"
        dm.touch()
        readme.touch()

        result = _filter_dataset_paths([str(tables_csv), str(dm), str(readme)])
        assert str(dm) in result
        assert str(readme) not in result


class TestTablesCsvWithFilenameColumn:
    def test_keeps_only_allowed_datasets(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv", "customers.csv"]}).to_csv(
            tables_csv, index=False
        )

        dm = tmp_path / "dm.csv"
        customers = tmp_path / "customers.csv"
        orders = tmp_path / "orders.csv"
        for f in (dm, customers, orders):
            f.touch()

        result = _filter_dataset_paths(
            [str(tables_csv), str(dm), str(customers), str(orders)]
        )
        assert sorted(result) == sorted([str(dm), str(customers)])
        assert str(orders) not in result

    def test_variables_csv_excluded_even_if_listed(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["variables.csv", "dm.csv"]}).to_csv(
            tables_csv, index=False
        )
        variables = tmp_path / "variables.csv"
        dm = tmp_path / "dm.csv"
        variables.touch()
        dm.touch()

        result = _filter_dataset_paths([str(tables_csv), str(variables), str(dm)])
        assert str(variables) not in result
        assert str(dm) in result

    def test_filename_with_path_prefix_uses_stem_matching(self, tmp_path):
        """Filename 'subdir/dm.csv' -> stem 'dm' -> matches 'dm.csv' on disk."""
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["subdir/dm.csv"]}).to_csv(tables_csv, index=False)
        dm = tmp_path / "dm.csv"
        dm.touch()

        result = _filter_dataset_paths([str(tables_csv), str(dm)])
        assert str(dm) in result

    def test_nan_filenames_are_ignored(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv", None]}).to_csv(tables_csv, index=False)
        dm = tmp_path / "dm.csv"
        unknown = tmp_path / "unknown.csv"
        dm.touch()
        unknown.touch()

        result = _filter_dataset_paths([str(tables_csv), str(dm), str(unknown)])
        assert str(dm) in result
        assert str(unknown) not in result

    def test_no_matching_files_returns_empty(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["nonexistent.csv"]}).to_csv(tables_csv, index=False)
        dm = tmp_path / "dm.csv"
        dm.touch()

        assert _filter_dataset_paths([str(tables_csv), str(dm)]) == []

    def test_non_csv_files_excluded_even_if_stem_matches(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"]}).to_csv(tables_csv, index=False)

        dm_xlsx = tmp_path / "dm.xlsx"
        dm_xlsx.touch()

        assert _filter_dataset_paths([str(tables_csv), str(dm_xlsx)]) == []

    def test_encoding_is_forwarded_to_read_csv(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"]}).to_csv(tables_csv, index=False)
        (tmp_path / "dm.csv").touch()

        with patch("pandas.read_csv", wraps=pd.read_csv) as mock_read:
            _filter_dataset_paths(
                [str(tables_csv), str(tmp_path / "dm.csv")], encoding="latin-1"
            )
            mock_read.assert_called_once_with(tables_csv, encoding="latin-1")


class TestEdgeCases:
    def test_only_tables_csv_in_input(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"]}).to_csv(tables_csv, index=False)
        assert _filter_dataset_paths([str(tables_csv)]) == []

    def test_only_variables_csv_in_input(self):
        assert _filter_dataset_paths(["/data/variables.csv"]) == []

    def test_empty_filename_column_returns_empty(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": pd.Series([], dtype=str)}).to_csv(
            tables_csv, index=False
        )
        dm = tmp_path / "dm.csv"
        dm.touch()

        assert _filter_dataset_paths([str(tables_csv), str(dm)]) == []

    def test_all_filename_values_nan_returns_empty(self, tmp_path):
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": [None, None]}).to_csv(tables_csv, index=False)
        dm = tmp_path / "dm.csv"
        dm.touch()

        assert _filter_dataset_paths([str(tables_csv), str(dm)]) == []

    def test_duplicate_paths_removed(self, tmp_path):
        """The function does not deduplicate; duplicates in -> duplicates out."""
        tables_csv = tmp_path / "tables.csv"
        pd.DataFrame({"Filename": ["dm.csv"]}).to_csv(tables_csv, index=False)
        dm = tmp_path / "dm.csv"
        dm.touch()

        paths = [str(tables_csv), str(dm), str(dm)]
        result = _filter_dataset_paths(paths)
        assert result.count(str(dm)) == 1
