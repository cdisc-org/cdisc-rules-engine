from unittest.mock import MagicMock, patch

import pytest

from scripts import run_sql_validation as rsv


@pytest.fixture
def interactive():
    with patch.object(rsv.sys.stdin, "isatty", return_value=True):
        yield


@pytest.mark.parametrize("key, expected", [("Y", True), ("y", True), ("n", False), ("\r", False), (" ", False)])
def test_confirm_run_without_rules_continues_only_on_y(interactive, capsys, key, expected):
    with patch.object(rsv.click, "getchar", return_value=key):
        assert rsv.confirm_run_without_rules() is expected
    assert rsv.NO_RULES_PROMPT in capsys.readouterr().out


def test_confirm_run_without_rules_does_not_prompt_without_a_terminal(capsys):
    with patch.object(rsv.sys.stdin, "isatty", return_value=False), patch.object(rsv.click, "getchar") as getchar:
        assert rsv.confirm_run_without_rules() is False
    getchar.assert_not_called()
    assert rsv.NO_RULES_MESSAGE in capsys.readouterr().err


def test_run_sql_validation_exits_before_loading_data_when_no_rules_and_user_declines():
    args = MagicMock(log_level="disabled", progress="disabled")
    with (
        patch.object(rsv, "get_rules", return_value=[]),
        patch.object(rsv, "confirm_run_without_rules", return_value=False),
        patch.object(rsv, "get_library_metadata_from_cache") as get_library_metadata,
        patch.object(rsv.PostgresQLDataService, "from_dataset_paths") as load_data,
    ):
        with pytest.raises(SystemExit) as exit_info:
            rsv.run_sql_validation(args)
    assert exit_info.value.code == 1
    get_library_metadata.assert_not_called()
    load_data.assert_not_called()


def test_run_sql_validation_does_not_prompt_when_rules_are_found():
    args = MagicMock(log_level="disabled", progress="disabled")
    with (
        patch.object(rsv, "get_rules", return_value=[{"core_id": "CORE-000001"}]),
        patch.object(rsv, "confirm_run_without_rules") as confirm,
        patch.object(rsv, "get_library_metadata_from_cache", side_effect=RuntimeError("stop after rule check")),
    ):
        with pytest.raises(RuntimeError, match="stop after rule check"):
            rsv.run_sql_validation(args)
    confirm.assert_not_called()
