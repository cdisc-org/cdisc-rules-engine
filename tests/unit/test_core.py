from unittest.mock import MagicMock

import pytest

from core import validate_standard


def test_validate_standard_accepts_supported_value():
    logger = MagicMock()
    ctx = MagicMock()

    result = validate_standard("SDTMIG", False, logger, ctx)

    assert result == "sdtmig"
    logger.error.assert_not_called()
    ctx.exit.assert_not_called()


def test_validate_standard_rejects_typo():
    logger = MagicMock()
    ctx = MagicMock()
    ctx.exit.side_effect = SystemExit(2)

    with pytest.raises(SystemExit) as exc_info:
        validate_standard("stdtmig", False, logger, ctx)

    assert exc_info.value.code == 2
    logger.error.assert_called_once()
    message = logger.error.call_args.args[0]
    assert "stdtmig" in message
    assert "sdtmig" in message


def test_validate_standard_allows_custom_standard():
    logger = MagicMock()
    ctx = MagicMock()

    result = validate_standard("cust_standard", True, logger, ctx)

    assert result == "cust_standard"
    logger.error.assert_not_called()
    ctx.exit.assert_not_called()
