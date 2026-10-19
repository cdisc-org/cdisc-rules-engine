import os
import queue
import threading
from pathlib import Path
from typing import List, Optional

import numpy as np
import pyarrow as pa

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER, SOURCE_DS
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.models.sql.table_schema import SqlColumnSchema, SqlTableSchema
from cdisc_rules_engine.readers.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.ingestion_progress import (
    DisabledIngestionProgressReporter,
    IngestionProgressReporter,
)


class SqlDatasetLoader:

    @staticmethod
    def load_datasets(
        pgi: PostgresQLInterface,
        dataset_paths: List[str],
        progress_reporter: Optional[IngestionProgressReporter] = None,
    ) -> List[DatasetMetadata2]:
        """
        Iterate through dataset files in `self.dataset_paths`
        and create corresponding SQL tables.
        """
        reporter = progress_reporter or DisabledIngestionProgressReporter()
        total_bytes = sum(os.path.getsize(path) for path in dataset_paths if os.path.exists(path))
        reporter.start(total_files=len(dataset_paths), total_bytes=total_bytes)

        try:
            return [
                SqlDatasetLoader._load_dataset_file(pgi, file_index, len(dataset_paths), file_path, reporter)
                for file_index, file_path in enumerate(dataset_paths)
            ]
        finally:
            reporter.finish()

    @staticmethod
    def _load_dataset_file(
        pgi: PostgresQLInterface,
        file_index: int,
        total_files: int,
        file_path_str: str,
        reporter: IngestionProgressReporter,
    ) -> DatasetMetadata2:
        """Load a single dataset file."""
        file_path = Path(file_path_str)
        try:
            reader = DataReaderFactory.get_data_reader(file_path_str)
            metadata, chunk_stream = reader.read()

            # force table_name to be lowercase
            table_name = file_path.stem.lower()

            logger.info(f"Loading dataset {file_path.name} into table {table_name}")

            schema = SqlTableSchema.from_metadata(metadata, pgi)
            source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
            schema.add_column(source_row_column)

            source_ds_column = SqlColumnSchema(name=SOURCE_DS, hash=SOURCE_DS, type="Char")
            schema.add_column(source_ds_column)

            pgi.create_table(schema)

            reporter.start_file(
                file_index=file_index,
                total_files=total_files,
                file_name=file_path.name,
                file_bytes=metadata.file_size or 0,
                total_rows=reader._get_total_rows(),
            )

            metadata.record_count = SqlDatasetLoader._copy_chunks(
                pgi, table_name, file_path.name, chunk_stream, reporter
            )

            pgi.create_index(table_name, ["USUBJID"])
            pgi.create_index(table_name, ["USUBJID", "VISIT"])

            reporter.end_file(metadata.record_count)

            logger.info(f"Successfully loaded {file_path.name}")

            return metadata
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")
            raise

    @staticmethod
    def _copy_chunks(
        pgi: PostgresQLInterface,
        table_name: str,
        file_name: str,
        chunk_stream,
        reporter: IngestionProgressReporter,
    ) -> int:
        prepared: "queue.Queue" = queue.Queue(maxsize=2)
        stop = threading.Event()
        failure: List[BaseException] = []
        counted = {"rows": 0}

        def prepare():
            row_number = 0
            try:
                for chunk in chunk_stream:
                    if stop.is_set():
                        break
                    SqlDatasetLoader._reject_reserved_columns(file_name, chunk)
                    rows = len(chunk)
                    chunk = SqlDatasetLoader._add_source_columns(table_name, chunk, row_number, rows)
                    row_number += rows
                    prepared.put(chunk)
                counted["rows"] = row_number
            except BaseException as error:  # surfaced on the user side
                failure.append(error)
            finally:
                if hasattr(chunk_stream, "close"):
                    chunk_stream.close()
                prepared.put(None)

        worker = threading.Thread(target=prepare, name=f"prepare-{table_name}", daemon=True)
        worker.start()
        try:
            while True:
                chunk = prepared.get()
                if chunk is None:
                    break
                pgi.copy_dataframe(table_name, chunk)
                reporter.report_chunk(len(chunk))
        finally:
            # if the write fails the worker may be blocked on a full queue
            # tell it to stop and keep draining until it exits
            stop.set()
            while worker.is_alive():
                try:
                    prepared.get(timeout=0.1)
                except queue.Empty:
                    pass
            worker.join()

        if failure:
            raise failure[0]
        return counted["rows"]

    @staticmethod
    def _add_source_columns(table_name: str, chunk, row_number: int, rows: int):
        """Attach the generated provenance columns to a chunk."""
        numbers = np.arange(row_number + 1, row_number + rows + 1, dtype="int64")
        source = table_name.upper()
        if isinstance(chunk, pa.Table):
            return chunk.append_column(SOURCE_ROW_NUMBER, pa.array(numbers)).append_column(
                SOURCE_DS, pa.repeat(pa.scalar(source, pa.string()), rows)
            )
        chunk[SOURCE_ROW_NUMBER] = numbers
        chunk[SOURCE_DS] = source
        return chunk

    @staticmethod
    def _column_names(chunk) -> List[str]:
        return list(chunk.column_names) if isinstance(chunk, pa.Table) else list(chunk.columns)

    @staticmethod
    def _reject_reserved_columns(file_name: str, chunk) -> None:
        for reserved in (SOURCE_ROW_NUMBER, SOURCE_DS):
            if reserved in SqlDatasetLoader._column_names(chunk):
                raise ValueError(
                    f"Dataset file '{file_name}' contains reserved column '{reserved}'. "
                    "This column is automatically generated and should not be in source data."
                )
