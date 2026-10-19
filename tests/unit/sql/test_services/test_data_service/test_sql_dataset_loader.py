import threading

import pandas as pd
import pyarrow as pa
import pytest

from cdisc_rules_engine.constants.metadata_columns import SOURCE_DS, SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.utilities.ingestion_progress import DisabledIngestionProgressReporter


class RecordingInterface:
    """Stands in for PostgresQLInterface while keeping what would have been copied."""

    def __init__(self, fail_on_call=None):
        self.copied = []
        self.fail_on_call = fail_on_call

    def copy_dataframe(self, table_name, frame):
        if self.fail_on_call is not None and len(self.copied) == self.fail_on_call:
            raise RuntimeError("COPY failed")
        self.copied.append(frame)
        return len(frame)


def copy_chunks(pgi, chunks):
    return SqlDatasetLoader._copy_chunks(pgi, "ae", "ae.xpt", iter(chunks), DisabledIngestionProgressReporter())


def run_with_timeout(target, seconds=10):
    outcome = {}

    def run():
        try:
            outcome["result"] = target()
        except BaseException as error:
            outcome["error"] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), "loader did not return - it is probably deadlocked"
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


def test_rows_are_numbered_across_chunks_of_either_kind():
    pgi = RecordingInterface()
    chunks = [
        pa.table({"usubjid": ["1", "2"]}),
        pd.DataFrame({"usubjid": ["3", "4", "5"]}),
    ]

    assert copy_chunks(pgi, chunks) == 5

    first, second = pgi.copied
    assert first.column(SOURCE_ROW_NUMBER).to_pylist() == [1, 2]
    assert first.column(SOURCE_DS).to_pylist() == ["AE", "AE"]
    assert second[SOURCE_ROW_NUMBER].tolist() == [3, 4, 5]
    assert second[SOURCE_DS].tolist() == ["AE", "AE", "AE"]


def test_a_failed_copy_stops_the_reader_instead_of_hanging():
    pgi = RecordingInterface(fail_on_call=0)
    chunks = (pa.table({"usubjid": [str(index)]}) for index in range(100))

    with pytest.raises(RuntimeError, match="COPY failed"):
        run_with_timeout(lambda: copy_chunks(pgi, chunks))


@pytest.mark.parametrize("reserved", [SOURCE_ROW_NUMBER, SOURCE_DS])
def test_source_data_cannot_use_a_reserved_column(reserved):
    pgi = RecordingInterface()

    with pytest.raises(ValueError, match=f"'ae.xpt' contains reserved column '{reserved}'"):
        run_with_timeout(lambda: copy_chunks(pgi, [pa.table({reserved: ["x"]})]))
    assert pgi.copied == []
