import sys
from abc import ABC, abstractmethod
from typing import Optional

import click

from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions


class IngestionProgressReporter(ABC):
    """
    Abstract callback interface for reporting SQL data ingestion progress.
    """

    @abstractmethod
    def start(self, total_files: int, total_bytes: int):
        pass

    @abstractmethod
    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        pass

    @abstractmethod
    def report_chunk(self, rows: int):
        pass

    @abstractmethod
    def end_file(self, rows: int):
        pass

    @abstractmethod
    def finish(self):
        pass


class DisabledIngestionProgressReporter(IngestionProgressReporter):
    def start(self, total_files: int, total_bytes: int):
        self._bar = None
        self._total_rows_estimate = 0
        self._rows_loaded = 0

    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        pass

    def report_chunk(self, rows: int):
        pass

    def end_file(self, rows: int):
        pass

    def finish(self):
        pass


class PercentIngestionProgressReporter(IngestionProgressReporter):
    def __init__(self, output_stream=None):
        self.output_stream = output_stream or sys.stdout
        self._total_files = 0
        self._current_file = 0

    def start(self, total_files: int, total_bytes: int):
        self._total_files = total_files
        self._current_file = 0

    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        pass

    def report_chunk(self, rows: int):
        pass

    def end_file(self, rows: int):
        self._current_file += 1
        if self._total_files <= 0:
            return
        percent = int(self._current_file / self._total_files * 100)
        self.output_stream.write(f"{percent}\n")
        self.output_stream.flush()

    def finish(self):
        pass


class ClickIngestionProgressBar(IngestionProgressReporter):
    def __init__(self, label: str = "Loading datasets", output=None):
        self._label = label
        self._output = output or sys.stdout
        self._bar = None
        self._total_rows_estimate = 0
        self._rows_loaded = 0

    def start(self, total_files: int, total_bytes: int):
        pass

    def start_file(
        self,
        file_index: int,
        total_files: int,
        file_name: str,
        file_bytes: int,
        total_rows: Optional[int],
    ):
        if self._bar is not None:
            self._bar.finish()

        if total_rows is not None:
            self._total_rows_estimate += total_rows
        else:
            self._total_rows_estimate += max(file_bytes // 200, 1)

        label = f"Loading {file_name} ({file_index + 1}/{total_files})"

        self._bar = click.progressbar(
            length=self._total_rows_estimate,
            label=label,
            fill_char=click.style("#", fg="green"),
            empty_char=click.style("-", fg="white", dim=True),
            show_eta=False,
            file=self._output,
        )
        self._bar.__enter__()
        self._bar.update(self._rows_loaded)

    def report_chunk(self, rows: int):
        self._rows_loaded += rows
        if self._bar is not None:
            if self._rows_loaded > self._bar.length:
                self._bar.length = self._rows_loaded
            self._bar.update(rows)

    def end_file(self, rows: int):
        if self._bar is not None:
            self._bar.__exit__(None, None, None)
            self._bar = None

    def finish(self):
        if self._bar is not None:
            self._bar.__exit__(None, None, None)
            self._bar = None


def get_ingestion_progress_reporter(progress_option: str) -> IngestionProgressReporter:
    if progress_option == ProgressParameterOptions.DISABLED.value:
        return DisabledIngestionProgressReporter()
    if progress_option == ProgressParameterOptions.PERCENTS.value:
        return PercentIngestionProgressReporter()
    if progress_option == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        return DisabledIngestionProgressReporter()
    return ClickIngestionProgressBar()
