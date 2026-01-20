import json
import locale
import os
import tempfile
from unittest import mock

import pytest

from cdisc_rules_engine.utilities.utils import load_json_with_optional_encoding


def test_user_encoding_used_successfully():
    data = {"日本語": [1, 2, 3]}

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "data.json")

        with open(path, "w", encoding="cp932") as f:
            json.dump(data, f, ensure_ascii=False)

        result = load_json_with_optional_encoding(path, encoding="cp932")

        assert result == data


def test_user_encoding_fails_then_fallback_to_utf8():
    data = {"key": "value"}

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "data.json")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        result = load_json_with_optional_encoding(path, encoding="cp932")

        assert result == data


def test_no_encoding_utf8_with_bom():
    data = {"STD_BOM": [1]}

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "data.json")

        with open(path, "w", encoding="utf-8-sig") as f:
            json.dump(data, f)

        result = load_json_with_optional_encoding(path)

        assert result == data


def test_system_encoding_fallback_mocked():
    data = {"日本語": [1, 2]}

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "data.json")

        with open(path, "w", encoding="cp932") as f:
            json.dump(data, f, ensure_ascii=False)

        with mock.patch.object(
            locale,
            "getpreferredencoding",
            return_value="cp932",
        ):
            result = load_json_with_optional_encoding(path)

        assert result == data


def test_all_encodings_fail():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "broken.json")

        with open(path, "wb") as f:
            f.write(b"\xff\xfe\xfa\xfb")

        with pytest.raises(ValueError) as exc:
            load_json_with_optional_encoding(path, encoding="utf-8")

        assert "Unable to load JSON file" in str(exc.value)
