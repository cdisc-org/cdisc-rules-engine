from types import SimpleNamespace

import pytest

import scripts.script_utils as script_utils
from scripts.script_utils import load_rules_from_local, load_specified_rules
from cdisc_rules_engine.utilities.utils import get_rules_cache_key


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
        None,
        standard,
        version,
        standard_dict,
        substandard,
    )

    returned_ids = {rule["core_id"] for rule in result}
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
        None,
        ("CORE-0002",),
        standard,
        version,
        standard_dict,
        substandard,
    )

    returned_ids = {rule["core_id"] for rule in result}
    assert returned_ids == {"CORE-0001", "CORE-0003"}


def test_load_rules_from_local_include(monkeypatch, standard_context):
    standard, version, substandard, _ = standard_context
    rule_paths = [
        "/rules/CORE.Local.0001.yml",
        "/rules/CORE.Local.0002.yml",
        "/rules/CORE.Local.0003.yml",
    ]

    args = SimpleNamespace(
        local_rules=("/rules",),
        rules=("CORE.Local.0001", "CORE.Local.0003"),
        exclude_rules=None,
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
        "/rules/CORE.Local.0001.yml",
        "/rules/CORE.Local.0002.yml",
        "/rules/CORE.Local.0003.yml",
    ]

    args = SimpleNamespace(
        local_rules=("/rules",),
        rules=None,
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
