import sys
from abc import ABC, abstractmethod

import click

from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions


def progress_bar_is_hidden(bar: click.progressbar) -> bool:
    if hasattr(bar, "hidden"):
        return bar.hidden
    return bar.is_hidden


class ReportProgressReporter(ABC):
    @abstractmethod
    def start(self, total_steps: int):
        pass

    @abstractmethod
    def start_step(self, label: str):
        pass

    @abstractmethod
    def end_step(self):
        pass

    @abstractmethod
    def finish(self):
        pass


class DisabledReportProgressReporter(ReportProgressReporter):
    def start(self, total_steps: int):
        pass

    def start_step(self, label: str):
        pass

    def end_step(self):
        pass

    def finish(self):
        pass


class PercentReportProgressReporter(ReportProgressReporter):
    def __init__(self, output_stream=None):
        self.output_stream = output_stream or sys.stdout
        self._total_steps = 0
        self._finished_steps = 0

    def start(self, total_steps: int):
        self._total_steps = total_steps
        self._finished_steps = 0

    def start_step(self, label: str):
        pass

    def end_step(self):
        self._finished_steps += 1
        if self._total_steps <= 0:
            return
        percent = int(self._finished_steps / self._total_steps * 100)
        self.output_stream.write(f"{percent}\n")
        self.output_stream.flush()

    def finish(self):
        pass


class ClickReportProgressBar(ReportProgressReporter):
    """
    Renders a status line naming the report step currently being written,
    with a plain click progress bar beneath it:
    Writing: Issue Details (sheet 2/5)
    [████████████████████████████--------]   40%
    """

    def __init__(self, output=None):
        self._output = output or sys.stdout
        self._bar = None

    def start(self, total_steps: int):
        self._output.write("Writing report...\n")
        self._bar = click.progressbar(
            length=max(total_steps, 1),
            fill_char=click.style("█", fg="green"),
            empty_char=click.style("-", fg="white", dim=True),
            show_eta=False,
            file=self._output,
        )
        self._bar.__enter__()

    def start_step(self, label: str):
        if self._bar is None:
            return
        if progress_bar_is_hidden(self._bar):
            self._output.write(f"{label}\n")
            self._output.flush()
        else:
            self._bar.label = label
            self._bar.render_progress()

    def end_step(self):
        if self._bar is not None:
            self._bar.update(1)

    def finish(self):
        if self._bar is not None:
            self._bar.__exit__(None, None, None)
            self._bar = None


def get_report_progress_reporter(progress_option: str) -> ReportProgressReporter:
    if progress_option == ProgressParameterOptions.DISABLED.value:
        return DisabledReportProgressReporter()
    if progress_option == ProgressParameterOptions.PERCENTS.value:
        return PercentReportProgressReporter()
    if progress_option == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        return DisabledReportProgressReporter()
    return ClickReportProgressBar()
