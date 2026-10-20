import asyncio
import os
import pickle
import warnings
from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.services.cache import cache_populator_service
from cdisc_rules_engine.services.cache.cache_populator_service import (
    BRANCH_CT_PACKAGES,
    BRANCH_RULES,
    BRANCH_STANDARDS,
    CachePopulator,
    is_library_access_error,
)
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService

ACCESS_ERROR = Exception(
    "Request to https://api.library.cdisc.org/api/mdr/ct/packages returned unsuccessful 401 response"
)


def rule(core_id: str, version: str = "new") -> dict:
    return {
        "core_id": core_id,
        "version": version,
        "standards": [{"Name": "SDTMIG", "Version": "3.4"}],
    }


class FakeLibraryService:
    """Returns one CT package, one rules catalog and one standard, unless told to fail."""

    def __init__(
        self, failures: dict = None, skipped_rules: list = None, extra_rules: list = None, extra_standards: list = None
    ):
        self.failures = failures or {}
        self.skipped_rules = skipped_rules or []
        self.extra_rules = extra_rules or []
        self.extra_standards = extra_standards or []

    def _fail(self, call: str, item: str = None):
        error = self.failures.get((call, item)) or self.failures.get(call)
        if error:
            raise error

    def get_all_ct_packages(self):
        self._fail("ct packages")
        return [
            {"href": "/mdr/ct/packages/sdtmct-2099-01-01"},
            {"href": "/mdr/ct/packages/sdtmct-2099-02-01"},
        ]

    def get_codelist_terms_map(self, package):
        self._fail("ct package", package)
        return {"package": package, "submission_lookup": {}}

    def get_all_rule_catalogs(self):
        self._fail("rule catalogs")
        return [{"href": "/mdr/rules/sdtmig/3-4"}]

    def get_rules_by_catalog(self, standard, version):
        self._fail("rules catalog", f"{standard}/{version}")
        return {
            "key_prefix": f"{standard}/{version}",
            "rules": [rule("CORE-000001"), rule("CORE-000002"), *self.extra_rules],
            "skipped_rules": self.skipped_rules,
        }

    def get_all_tabulation_ig_standards(self):
        self._fail("standards")
        return [{"href": "/mdr/sdtmig/3-4"}, *self.extra_standards]

    def get_all_collection_ig_standards(self):
        return []

    def get_all_analysis_ig_standards(self):
        return []

    def get_tig_standards(self):
        return []

    def get_standard_details(self, standard_type, version, substandard=None):
        self._fail("standard details", f"{standard_type}/{version}")
        return {"name": f"{standard_type} {version}", "version": version}

    def get_variable_codelists_map(self, standard_type, version, substandard=None):
        return {"name": f"{standard_type}/{version}", "AESEV": ["C66769"]}

    def get_variables_details(self, standard_type, version, substandard=None):
        self._fail("variables details", f"{standard_type}/{version}")
        return {"AESEV": {"name": "AESEV"}}

    def get_model_details(self, standard_details):
        return {"standard_type": "sdtm", "version": "2-0"}


def update_cache(cache_path, library_service):
    populator = CachePopulator(MagicMock(), library_service, cache_path=str(cache_path))
    return asyncio.run(populator.update_cache())


def load(cache_path, file_name: str):
    with open(os.path.join(cache_path, file_name), "rb") as f:
        return pickle.load(f)


def write(cache_path, file_name: str, data):
    with open(os.path.join(cache_path, file_name), "wb") as f:
        pickle.dump(data, f)


def test_update_cache_saves_all_branches(tmp_path):
    report = update_cache(tmp_path, FakeLibraryService())

    assert report.succeeded
    assert report.summary() == "Cache updated successfully"
    assert load(tmp_path, "sdtmct-2099-02-01.pkl")["package"] == "sdtmct-2099-02-01"
    assert set(load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)) == {
        "CORE-000001",
        "CORE-000002",
    }
    assert "standards/sdtmig/3-4" in load(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value)
    assert not list(tmp_path.glob("*.partial"))


def test_failed_ct_package_is_skipped_and_keeps_its_cached_file(tmp_path):
    write(tmp_path, "sdtmct-2099-01-01.pkl", {"package": "sdtmct-2099-01-01", "cached": True})
    service = FakeLibraryService({("ct package", "sdtmct-2099-01-01"): Exception("timeout")})

    report = update_cache(tmp_path, service)

    assert report.succeeded
    assert load(tmp_path, "sdtmct-2099-01-01.pkl")["cached"]
    assert load(tmp_path, "sdtmct-2099-02-01.pkl")["package"] == "sdtmct-2099-02-01"
    assert report.skipped_items[BRANCH_CT_PACKAGES] == ["CT package sdtmct-2099-01-01 (Exception: timeout)"]
    assert "1 skipped (previous version kept)" in report.summary()


def test_unparseable_rule_is_skipped_and_keeps_its_cached_version(tmp_path):
    write(
        tmp_path,
        DefaultFilePaths.RULES_CACHE_FILE.value,
        {"CORE-000003": rule("CORE-000003", "cached"), "CORE-000001": rule("CORE-000001", "cached")},
    )
    write(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value, {"sdtmig/3-4": ["CORE-000003"]})
    service = FakeLibraryService(skipped_rules=[("CORE-000003", ValueError("No check data provided"))])

    report = update_cache(tmp_path, service)

    rules = load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)
    assert report.succeeded
    assert rules["CORE-000001"]["version"] == "new"
    assert rules["CORE-000003"]["version"] == "cached"
    assert sorted(load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value)["sdtmig/3-4"]) == [
        "CORE-000001",
        "CORE-000002",
        "CORE-000003",
    ]
    assert report.skipped_items[BRANCH_RULES] == ["rule CORE-000003 (sdtmig/3-4) (ValueError: No check data provided)"]


def test_failed_rules_catalog_is_skipped_and_keeps_its_cached_rules(tmp_path):
    write(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value, {"CORE-000009": rule("CORE-000009", "cached")})
    write(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value, {"sdtmig/3-4": ["CORE-000009"]})
    service = FakeLibraryService({("rules catalog", "sdtmig/3-4"): Exception("timeout")})

    report = update_cache(tmp_path, service)

    assert report.succeeded
    assert set(load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)) == {"CORE-000009"}
    assert load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value) == {"sdtmig/3-4": ["CORE-000009"]}
    assert report.skipped_items[BRANCH_RULES][0].startswith("rules catalog /mdr/rules/sdtmig/3-4")


def test_skipped_standard_keeps_its_cached_details(tmp_path):
    cached_details = {"sdtmig/3-3": {"name": "cached"}}
    write(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value, cached_details)
    service = FakeLibraryService({("standard details", "sdtmig/3-4"): Exception("timeout")})

    report = update_cache(tmp_path, service)

    assert report.succeeded
    assert load(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value) == cached_details
    assert report.skipped_items[BRANCH_STANDARDS][0].startswith("details of /mdr/sdtmig/3-4")


def test_rule_with_an_incomplete_standard_is_skipped(tmp_path):
    incomplete = {**rule("CORE-000003"), "standards": [{"Name": "SDTMIG", "Version": None}]}

    report = update_cache(tmp_path, FakeLibraryService(extra_rules=[incomplete]))

    assert report.succeeded
    assert set(load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)) == {"CORE-000001", "CORE-000002"}
    assert load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value) == {"sdtmig/3-4": ["CORE-000001", "CORE-000002"]}
    assert report.skipped_items[BRANCH_RULES][0].startswith("rule CORE-000003 (sdtmig/3-4) (AttributeError")


def test_standard_with_a_malformed_link_is_skipped(tmp_path):
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)  # e.g. coroutines never awaited
        report = update_cache(tmp_path, FakeLibraryService(extra_standards=[{"href": ""}]))

    assert report.succeeded
    assert BRANCH_STANDARDS not in report.failed_branches
    assert "standards/sdtmig/3-4" in load(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value)
    assert len(report.skipped_items[BRANCH_STANDARDS]) == 3  # details, variable codelist map, variables metadata


def test_skipped_standard_keeps_its_entries_in_every_standards_file(tmp_path):
    cached_metadata = {"standards/sdtmig/3-3": {"cached": True}}
    write(tmp_path, DefaultFilePaths.VARIABLE_METADATA_CACHE_FILE.value, cached_metadata)
    service = FakeLibraryService({("variables details", "sdtmig/3-4"): Exception("timeout")})

    report = update_cache(tmp_path, service)

    assert report.succeeded
    assert load(tmp_path, DefaultFilePaths.VARIABLE_METADATA_CACHE_FILE.value) == cached_metadata
    assert "standards/sdtmig/3-4" in load(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value)


def test_clean_update_replaces_the_cached_files(tmp_path):
    write(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value, {"CORE-WITHDRAWN": rule("CORE-WITHDRAWN")})
    write(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value, {"sdtmig/3-4": ["CORE-WITHDRAWN"]})

    report = update_cache(tmp_path, FakeLibraryService())

    assert report.summary() == "Cache updated successfully"
    assert set(load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)) == {"CORE-000001", "CORE-000002"}
    assert load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value) == {"sdtmig/3-4": ["CORE-000001", "CORE-000002"]}


def test_failed_write_keeps_the_branch_cache_as_it_was(tmp_path, monkeypatch):
    cached_rules = {"CORE-000009": rule("CORE-000009", "cached")}
    write(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value, cached_rules)
    real_dump = pickle.dump

    def dump(data, f):
        if isinstance(data.get("sdtmig/3-4"), list):  # the rules dictionary, written after rules.pkl
            raise OSError("No space left on device")
        real_dump(data, f)

    monkeypatch.setattr(cache_populator_service.pickle, "dump", dump)

    report = update_cache(tmp_path, FakeLibraryService())

    assert report.failed_branches == {BRANCH_RULES: "OSError: No space left on device"}
    assert load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value) == cached_rules
    assert not (tmp_path / DefaultFilePaths.RULES_DICTIONARY.value).exists()
    assert not list(tmp_path.glob("*.partial"))


def jsonata_rule(core_id: str) -> dict:
    return {**rule(core_id), "conditions": "$.study.versions[$not(name)]"}


def test_jsonata_rule_is_cached_but_not_listed(tmp_path):
    report = update_cache(tmp_path, FakeLibraryService(extra_rules=[jsonata_rule("CORE-000003")]))

    assert report.succeeded
    assert not report.skipped_items
    assert load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)["CORE-000003"]["conditions"].startswith("$.study")
    assert load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value) == {"sdtmig/3-4": ["CORE-000001", "CORE-000002"]}
    assert report.summary() == (
        "Cache updated successfully\n1 rules with a JSONata check were cached but not listed for validation, "
        "as this engine does not support JSONata."
    )


def test_jsonata_rule_is_not_listed_again_by_a_partial_update(tmp_path):
    write(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value, {"sdtmig/3-4": ["CORE-000003"]})
    service = FakeLibraryService(
        skipped_rules=[("CORE-000004", ValueError("No check data provided"))],
        extra_rules=[jsonata_rule("CORE-000003")],
    )

    update_cache(tmp_path, service)

    assert "CORE-000003" not in load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value)["sdtmig/3-4"]


def test_rule_whose_cached_version_is_jsonata_is_not_listed(tmp_path):
    # the same CORE ID in a later catalog with a JSONata check: that is the version cached
    report = update_cache(tmp_path, FakeLibraryService(extra_rules=[jsonata_rule("CORE-000001")]))

    assert load(tmp_path, DefaultFilePaths.RULES_DICTIONARY.value) == {"sdtmig/3-4": ["CORE-000002"]}
    assert report.unlisted_jsonata_rules == {"CORE-000001"}


def test_global_failure_only_fails_its_own_branch(tmp_path):
    write(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value, {"cached": {}})
    service = FakeLibraryService({"standards": Exception("server error")})

    report = update_cache(tmp_path, service)

    assert report.succeeded
    assert report.failed_branches == {BRANCH_STANDARDS: "Exception: server error"}
    assert load(tmp_path, DefaultFilePaths.STANDARD_DETAILS_CACHE_FILE.value) == {"cached": {}}
    assert set(load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)) == {"CORE-000001", "CORE-000002"}
    assert (tmp_path / "sdtmct-2099-01-01.pkl").exists()
    assert "standards metadata: NOT updated, previous cache kept" in report.summary()


def test_access_error_fails_the_run_but_other_branches_are_saved(tmp_path):
    service = FakeLibraryService({"ct packages": ACCESS_ERROR, "standards": ACCESS_ERROR})

    report = update_cache(tmp_path, service)

    assert not report.succeeded
    assert report.access_denied
    assert set(load(tmp_path, DefaultFilePaths.RULES_CACHE_FILE.value)) == {"CORE-000001", "CORE-000002"}
    summary = report.summary()
    assert summary.startswith("Cache update failed:")
    assert "CDISC Library refused access (401)" in summary


def test_access_error_on_a_single_item_also_fails_the_run(tmp_path):
    service = FakeLibraryService({("ct package", "sdtmct-2099-01-01"): ACCESS_ERROR})

    report = update_cache(tmp_path, service)

    assert not report.succeeded
    assert (tmp_path / "sdtmct-2099-02-01.pkl").exists()


def test_all_branches_failing_fails_the_run(tmp_path):
    error = Exception("server error")
    service = FakeLibraryService({"ct packages": error, "rule catalogs": error, "standards": error})

    report = update_cache(tmp_path, service)

    assert not report.succeeded
    assert not report.access_denied
    assert set(report.failed_branches) == {BRANCH_CT_PACKAGES, BRANCH_RULES, BRANCH_STANDARDS}
    assert not list(tmp_path.iterdir())


def test_helpers_still_raise_outside_of_update_cache():
    service = FakeLibraryService({("standard details", "sdtmig/3-4"): Exception("timeout")})
    populator = CachePopulator(MagicMock(), service)

    with pytest.raises(Exception, match="timeout"):
        asyncio.run(populator._async_get_details_of_all_standards([{"href": "/mdr/sdtmig/3-4"}]))


def test_is_library_access_error():
    access_error_0_1_7 = Exception("Members-only content.")
    access_error_0_1_7.status_code = 401
    assert is_library_access_error(ACCESS_ERROR)
    assert is_library_access_error(access_error_0_1_7)
    assert not is_library_access_error(Exception("returned unsuccessful 500 response"))


def test_get_rules_by_catalog_skips_unparseable_rules():
    valid_rule = {
        "Core": {"Id": "CORE-000001", "Status": "Published"},
        "Check": {"all": [{"name": "AESTDY", "operator": "non_empty"}]},
        "Outcome": {"Message": "AESTDY is populated"},
        "Authorities": [{"Standards": [{"Name": "SDTMIG", "Version": "3.4"}]}],
    }
    service = CDISCLibraryService("api key", MagicMock())
    service._client = MagicMock()
    service._client.get_rules_catalog.return_value = {
        "valid": valid_rule,
        "jsonata": {**valid_rule, "Core": {"Id": "CORE-000002"}, "Check": "$.study"},
        "invalid": {**valid_rule, "Core": {"Id": "CORE-000003"}, "Check": None},
    }

    rules = service.get_rules_by_catalog("sdtmig", "3-4")

    assert [r["core_id"] for r in rules["rules"]] == ["CORE-000001", "CORE-000002"]
    assert rules["rules"][1]["conditions"] == "$.study"
    assert [(core_id, str(e)) for core_id, e in rules["skipped_rules"]] == [("CORE-000003", "No check data provided")]
