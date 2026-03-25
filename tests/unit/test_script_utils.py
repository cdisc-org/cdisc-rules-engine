from types import SimpleNamespace
from os import path
from unittest.mock import MagicMock, patch

import pytest

import scripts.script_utils as script_utils
from scripts.script_utils import (
    fill_cache_with_dictionaries,
    load_rules_from_local,
    load_specified_rules,
)
from cdisc_rules_engine.utilities.utils import get_rules_cache_key
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes


@pytest.fixture
def standard_context():
    standard = "sdtmig"
    version = "3-2"
    substandard = None
    key = get_rules_cache_key(standard, version, substandard)
    return standard, version, substandard, key


def test_load_specified_rules_include(standard_context):
    standard, version, substandard, key = standard_context
    rules_data = {
        "CORE-0001": {"core_id": "CORE-0001"},
        "CORE-0002": {"core_id": "CORE-0002"},
        "CORE-0003": {"core_id": "CORE-0003"},
    }
    standard_dict = {key: {rule_id: {} for rule_id in rules_data.keys()}}

    result = load_specified_rules(
        rules_data,
        ("CORE-0001", "CORE-0003"),
        (),
        standard,
        version,
        standard_dict,
        substandard,
    )

    returned_ids = {rule["core_id"] for rule in result[0] if isinstance(result, tuple)}
    assert returned_ids == {"CORE-0001", "CORE-0003"}


def test_load_specified_rules_exclude(standard_context):
    standard, version, substandard, key = standard_context
    rules_data = {
        "CORE-0001": {"core_id": "CORE-0001"},
        "CORE-0002": {"core_id": "CORE-0002"},
        "CORE-0003": {"core_id": "CORE-0003"},
    }
    standard_dict = {key: {rule_id: {} for rule_id in rules_data.keys()}}

    result = load_specified_rules(
        rules_data,
        (),
        ("CORE-0002",),
        standard,
        version,
        standard_dict,
        substandard,
    )

    returned_ids = {rule["core_id"] for rule in result[0] if isinstance(result, tuple)}
    assert returned_ids == {"CORE-0001", "CORE-0003"}


def test_load_rules_from_local_include(monkeypatch, standard_context):
    standard, version, substandard, _ = standard_context
    rule_paths = [
        path.join("/rules", file)
        for file in [
            "CORE.Local.0001.yml",
            "CORE.Local.0002.yml",
            "CORE.Local.0003.yml",
        ]
    ]

    args = SimpleNamespace(
        local_rules=("/rules",),
        rules=("CORE.Local.0001", "CORE.Local.0003"),
        exclude_rules=(),
        standard=standard,
        version=version,
        substandard=substandard,
    )

    fake_rules = {
        rule_paths[0]: {
            "core_id": "CORE.Local.0001",
            "status": "published",
            "standards": [
                {"Name": standard.upper(), "Version": version.replace("-", ".")}
            ],
        },
        rule_paths[1]: {
            "core_id": "CORE.Local.0002",
            "status": "published",
            "standards": [
                {"Name": standard.upper(), "Version": version.replace("-", ".")}
            ],
        },
        rule_paths[2]: {
            "core_id": "CORE.Local.0003",
            "status": "published",
            "standards": [
                {"Name": standard.upper(), "Version": version.replace("-", ".")}
            ],
        },
    }

    monkeypatch.setattr(script_utils.os.path, "isdir", lambda path: True)
    monkeypatch.setattr(
        script_utils.os,
        "listdir",
        lambda path: [
            "CORE.Local.0001.yml",
            "CORE.Local.0002.yml",
            "CORE.Local.0003.yml",
        ],
    )
    monkeypatch.setattr(
        script_utils,
        "load_and_parse_rule",
        lambda path: fake_rules.get(path, None),
    )

    rules = load_rules_from_local(args)

    returned_ids = {rule["core_id"] for rule in rules}
    assert returned_ids == {"CORE.Local.0001", "CORE.Local.0003"}


def test_load_rules_from_local_exclude(monkeypatch, standard_context):
    standard, version, substandard, _ = standard_context
    rule_paths = [
        path.join("/rules", file)
        for file in [
            "CORE.Local.0001.yml",
            "CORE.Local.0002.yml",
            "CORE.Local.0003.yml",
        ]
    ]

    args = SimpleNamespace(
        local_rules=("/rules",),
        rules=(),
        exclude_rules=("CORE.Local.0002",),
        standard=standard,
        version=version,
        substandard=substandard,
    )

    fake_rules = {
        rule_paths[0]: {
            "core_id": "CORE.Local.0001",
            "status": "published",
            "standards": [
                {"Name": standard.upper(), "Version": version.replace("-", ".")}
            ],
        },
        rule_paths[1]: {
            "core_id": "CORE.Local.0002",
            "status": "published",
            "standards": [
                {"Name": standard.upper(), "Version": version.replace("-", ".")}
            ],
        },
        rule_paths[2]: {
            "core_id": "CORE.Local.0003",
            "status": "published",
            "standards": [
                {"Name": standard.upper(), "Version": version.replace("-", ".")}
            ],
        },
    }

    monkeypatch.setattr(script_utils.os.path, "isdir", lambda path: True)
    monkeypatch.setattr(
        script_utils.os,
        "listdir",
        lambda path: [
            "CORE.Local.0001.yml",
            "CORE.Local.0002.yml",
            "CORE.Local.0003.yml",
        ],
    )
    monkeypatch.setattr(
        script_utils,
        "load_and_parse_rule",
        lambda path: fake_rules.get(path, None),
    )

    rules = load_rules_from_local(args)

    returned_ids = {rule["core_id"] for rule in rules}
    assert returned_ids == {"CORE.Local.0001", "CORE.Local.0003"}


def _make_args(mapping):
    return SimpleNamespace(
        external_dictionaries=SimpleNamespace(dictionary_path_mapping=mapping)
    )


_NO_EXTRACT = object()


def _extract_meddra_fails_whodrug_ok(whodrug_terms):
    def fn(_ds, dtype, _dpath):
        if dtype == "meddra":
            raise MissingDataError("Missing files")
        return whodrug_terms

    return fn


@pytest.mark.parametrize(
    "mapping,expected,extract_ret,raise_exc,warn_has,cache_with",
    [
        ({"meddra": ""}, {}, _NO_EXTRACT, None, None, None),
        ({"meddra": None}, {}, _NO_EXTRACT, None, None, None),
        ({}, {}, _NO_EXTRACT, None, None, None),
        (
            {DictionaryTypes.SNOMED.value: {"edition": "MAIN", "version": "2024-01"}},
            {DictionaryTypes.SNOMED.value: "MAIN/MAIN/2024-01"},
            _NO_EXTRACT,
            None,
            None,
            None,
        ),
        (
            {DictionaryTypes.SNOMED.value: {"edition": "MAIN"}},
            {},
            _NO_EXTRACT,
            None,
            None,
            None,
        ),
        (
            {"meddra": r".\bad_meddra"},
            {},
            MissingDataError("Required files not found"),
            None,
            [
                "meddra",
                r".\bad_meddra",
                "could not be loaded",
                "Required files not found",
            ],
            None,
        ),
        (
            {"meddra": r".\valid_meddra"},
            {"meddra": "26.0"},
            ExternalDictionary(terms={"PT1": {}}, version="26.0"),
            None,
            None,
            (r".\valid_meddra", "26.0"),
        ),
        (
            {"meddra": r".\bad_meddra", "whodrug": r".\valid_whodrug"},
            {"whodrug": "2024-01"},
            "side_effect",
            None,
            None,
            (r".\valid_whodrug", "2024-01"),
        ),
        (
            {"meddra": r".\some_path"},
            None,
            OSError("Permission denied"),
            OSError,
            None,
            None,
        ),
    ],
    ids=[
        "empty_path",
        "none_path",
        "empty_mapping",
        "snomed_full",
        "snomed_incomplete",
        "missing_data_error",
        "success",
        "one_fails_others_ok",
        "other_exception_propagates",
    ],
)
def test_fill_cache_with_dictionaries(
    mapping, expected, extract_ret, raise_exc, warn_has, cache_with
):
    """Parametrized: falsy/SNOMED skip, MissingDataError skip+warn, success,
    one-fails-others-succeed, other exception propagates."""
    cache, args, ds = MagicMock(), _make_args(mapping), MagicMock()
    with patch.object(script_utils, "extract_dictionary_terms") as ext:
        if extract_ret is _NO_EXTRACT:
            pass
        elif extract_ret == "side_effect":
            terms = ExternalDictionary(terms={}, version="2024-01")
            ext.side_effect = _extract_meddra_fails_whodrug_ok(terms)
        elif isinstance(extract_ret, BaseException):
            ext.side_effect = extract_ret
        else:
            ext.return_value = extract_ret
        with patch.object(script_utils, "engine_logger") as log:
            if raise_exc:
                with pytest.raises(raise_exc, match="Permission denied"):
                    fill_cache_with_dictionaries(cache, args, ds)
            else:
                result = fill_cache_with_dictionaries(cache, args, ds)
                assert result == expected
            if warn_has:
                log.warning.assert_called_once()
                msg = log.warning.call_args[0][0]
                for s in warn_has:
                    assert s in msg or s in msg.lower()
    if extract_ret is _NO_EXTRACT:
        ext.assert_not_called()
    if cache_with:
        cache.add.assert_called_once()
        assert cache.add.call_args[0][0] == cache_with[0]
        assert cache.add.call_args[0][1].version == cache_with[1]
    else:
        cache.add.assert_not_called()
