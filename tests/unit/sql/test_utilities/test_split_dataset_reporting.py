import pytest
from types import SimpleNamespace

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models import sql_result_handler
from cdisc_rules_engine.models.sql_result_handler import SqlResultHandler
from cdisc_rules_engine.models.validation_error_container import ValidationErrorContainer
from cdisc_rules_engine.models.validation_error_entity import ValidationErrorEntity
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine


def make_handler(split_part_filenames):
    handler = SqlResultHandler.__new__(SqlResultHandler)
    handler.rule = {}
    handler.dataset_metadata = SimpleNamespace(
        name="MH",
        domain="MH",
        variable_prefix="MH",
        filename="mh.xpt",
        split_part_filenames=split_part_filenames,
    )
    handler.data_service = SimpleNamespace(pgi=SimpleNamespace(schema=SimpleNamespace(get_table=lambda _name: None)))
    return handler


def error_in(filename, row):
    return ValidationErrorEntity(value={"MHSEQ": 1}, dataset=filename, row=row)


@pytest.fixture(autouse=True)
def _stub_target_columns(monkeypatch):
    monkeypatch.setattr(sql_result_handler, "get_target_columns", lambda *_args: ["MHSEQ"])


def test_clean_split_part_is_still_reported():
    handler = make_handler(["mh1.xpt", "mh2.xpt"])

    containers = handler._bundle_error_objects_per_source(
        "Duplicate MHSEQ",
        [error_in("mh1.xpt", 1), error_in("mh1.xpt", 2)],
    )

    by_dataset = {container.dataset: container for container in containers}
    assert sorted(by_dataset) == ["mh1.xpt", "mh2.xpt"]
    assert len(by_dataset["mh1.xpt"].errors) == 2
    assert by_dataset["mh1.xpt"].status == ExecutionStatus.SUCCESS.value
    assert by_dataset["mh2.xpt"].errors == []


def test_errors_are_split_across_their_source_parts():
    handler = make_handler(["mh1.xpt", "mh2.xpt"])

    containers = handler._bundle_error_objects_per_source(
        "Duplicate MHSEQ",
        [error_in("mh1.xpt", 1), error_in("mh2.xpt", 7)],
    )

    assert {c.dataset: len(c.errors) for c in containers} == {"mh1.xpt": 1, "mh2.xpt": 1}


def test_every_part_reported_when_no_part_has_errors():
    handler = make_handler(["mh1.xpt", "mh2.xpt"])

    containers = handler._bundle_error_objects_per_source("Duplicate MHSEQ", [])

    assert sorted(c.dataset for c in containers) == ["mh1.xpt", "mh2.xpt"]
    assert all(c.errors == [] for c in containers)


def test_unsplit_dataset_reports_a_single_container():
    handler = make_handler(None)

    containers = handler._bundle_error_objects_per_source(
        "Duplicate MHSEQ",
        [error_in("mh.xpt", 1)],
    )

    assert len(containers) == 1
    assert containers[0].dataset == "mh.xpt"


@pytest.mark.parametrize(
    "split_part_filenames, expected",
    [
        (["mh1.xpt", "mh2.xpt"], ["mh1.xpt", "mh2.xpt"]),
        (None, ["mh.xpt"]),
        ([], ["mh.xpt"]),
    ],
)
def test_containers_per_reported_file(split_part_filenames, expected):
    dataset_metadata = SimpleNamespace(
        name="MH",
        domain="MH",
        filename="mh.xpt",
        split_part_filenames=split_part_filenames,
    )

    representations = SQLRulesEngine._containers_per_reported_file(
        dataset_metadata,
        lambda filename: ValidationErrorContainer(
            dataset=filename,
            domain=dataset_metadata.domain,
            errors=[],
        ),
    )

    assert [rep["dataset"] for rep in representations] == expected


def test_skipped_split_dataset_is_reported_per_part():
    """A skip reason applies to every part, so each part carries it."""
    dataset_metadata = SimpleNamespace(
        name="MH",
        domain="MH",
        filename="mh.xpt",
        split_part_filenames=["mh1.xpt", "mh2.xpt"],
    )

    representations = SQLRulesEngine._containers_per_reported_file(
        dataset_metadata,
        lambda filename: ValidationErrorContainer(
            status=ExecutionStatus.SKIPPED.value,
            message="Not in scope",
            dataset=filename,
            domain=dataset_metadata.domain,
        ),
    )

    assert [rep["dataset"] for rep in representations] == ["mh1.xpt", "mh2.xpt"]
    assert all(rep["executionStatus"] == ExecutionStatus.SKIPPED.value for rep in representations)
    assert all(rep["message"] == "Not in scope" for rep in representations)


def test_clean_part_carries_no_message():
    """A part with no errors of its own passed, so it carries no error message."""
    handler = make_handler(["mh1.xpt", "mh2.xpt"])

    containers = handler._bundle_error_objects_per_source(
        "Duplicate MHSEQ",
        [error_in("mh1.xpt", 1)],
    )

    by_dataset = {container.dataset: container for container in containers}
    assert by_dataset["mh1.xpt"].message == "Duplicate MHSEQ"
    assert by_dataset["mh2.xpt"].message is None


def test_no_part_carries_a_message_when_none_have_errors():
    handler = make_handler(["mh1.xpt", "mh2.xpt"])

    containers = handler._bundle_error_objects_per_source("Duplicate MHSEQ", [])

    assert all(container.message is None for container in containers)


def test_domain_substitution_still_applies_to_parts_with_errors():
    """The "--" placeholder is still replaced with the domain where a message is set."""
    handler = make_handler(["mh1.xpt", "mh2.xpt"])

    containers = handler._bundle_error_objects_per_source(
        "-- has a duplicate sequence",
        [error_in("mh1.xpt", 1)],
    )

    by_dataset = {container.dataset: container for container in containers}
    assert by_dataset["mh1.xpt"].message == "MH has a duplicate sequence"
    assert by_dataset["mh2.xpt"].message is None


def make_dataset_sensitivity_handler(monkeypatch, split_part_filenames):
    handler = make_handler(split_part_filenames)
    handler.rule = {"sensitivity": "Dataset"}
    monkeypatch.setattr(
        SqlResultHandler,
        "_create_error_for_row",
        lambda _self, row, _compiled: ValidationErrorEntity(value={"MHCAT": row["mhcat"]}),
    )
    return handler


source_ds_schema = SimpleNamespace(get_column_hash=lambda column: column.lower())
compiled_query = SimpleNamespace(schema=source_ds_schema, target_columns={"MHCAT": True}, inline_outputs=[])


def test_dataset_error_is_reported_on_each_split_part_with_error_rows(monkeypatch):
    """A domain-wide error on a concatenated split dataset is kept for every part it came from."""
    handler = make_dataset_sensitivity_handler(monkeypatch, ["mh1.xpt", "mh2.xpt", "mh3.xpt"])
    rows = [
        {"source_ds": "MH1", "mhcat": "GENERAL"},
        {"source_ds": "MH1", "mhcat": "OTHER"},
        {"source_ds": "MH2", "mhcat": "GENERAL"},
    ]

    errors = handler._generate_errors_list(rows, compiled_query)
    containers = handler._bundle_error_objects_per_source("MHCAT has the same value", errors)

    assert {c.dataset: [e.value for e in c.errors] for c in containers} == {
        "mh1.xpt": [{"MHCAT": "GENERAL"}],
        "mh2.xpt": [{"MHCAT": "GENERAL"}],
        "mh3.xpt": [],
    }


def test_dataset_error_on_unsplit_dataset_is_reported_once(monkeypatch):
    handler = make_dataset_sensitivity_handler(monkeypatch, None)
    rows = [
        {"source_ds": "MH", "mhcat": "GENERAL"},
        {"source_ds": "MH", "mhcat": "GENERAL"},
    ]

    errors = handler._generate_errors_list(rows, compiled_query)

    assert [(e._dataset, e.value) for e in errors] == [("mh.xpt", {"MHCAT": "GENERAL"})]
