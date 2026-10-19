import shutil
import sys
from abc import ABC, abstractmethod

import click

from cdisc_rules_engine.enums.base_enum import BaseEnum
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.utilities.report_progress import progress_bar_is_hidden


class RuleProgressPhase(BaseEnum):
    PARSING = "Parsing"
    EXECUTING = "Executing"
    EVALUATING = "Evaluating Results"


class RuleProgressReporter(ABC):
    @abstractmethod
    def start(self, total_rules: int):
        pass

    @abstractmethod
    def start_rule(self, rule_id: str, rule_index: int, total_rules: int):
        pass

    @abstractmethod
    def set_phase(self, phase: RuleProgressPhase):
        pass

    @abstractmethod
    def end_rule(self):
        pass

    @abstractmethod
    def finish(self):
        pass


class DisabledRuleProgressReporter(RuleProgressReporter):
    def start(self, total_rules: int):
        pass

    def start_rule(self, rule_id: str, rule_index: int, total_rules: int):
        pass

    def set_phase(self, phase: RuleProgressPhase):
        pass

    def end_rule(self):
        pass

    def finish(self):
        pass


class PercentRuleProgressReporter(RuleProgressReporter):
    def __init__(self, output_stream=None):
        self.output_stream = output_stream or sys.stdout
        self._total_rules = 0
        self._finished_rules = 0

    def start(self, total_rules: int):
        self._total_rules = total_rules
        self._finished_rules = 0

    def start_rule(self, rule_id: str, rule_index: int, total_rules: int):
        pass

    def set_phase(self, phase: RuleProgressPhase):
        pass

    def end_rule(self):
        self._finished_rules += 1
        if self._total_rules <= 0:
            return
        percent = int(self._finished_rules / self._total_rules * 100)
        self.output_stream.write(f"{percent}\n")
        self.output_stream.flush()

    def finish(self):
        pass


_CURSOR_UP_AND_CLEAR_LINE = "\x1b[1A\r\x1b[2K"


class ClickRuleProgressBar(RuleProgressReporter):
    """
    Renders two lines above the shell prompt: a status line naming the rule
    currently running and its phase, and beneath it the plain click progress
    bar over the rules being validated:
    Current rule: CORE-000123 (12/369) - Executing
    [████████████████████████████--------]   40%
    """

    def __init__(self, output=None):
        self._output = output or sys.stdout
        self._bar = None
        self._show_status_line = False
        self._rule_label = ""

    def start(self, total_rules: int):
        self._bar = click.progressbar(
            length=max(total_rules, 1),
            fill_char=click.style("█", fg="green"),
            empty_char=click.style("-", fg="white", dim=True),
            show_eta=False,
            file=self._output,
        )
        self._show_status_line = not progress_bar_is_hidden(self._bar)
        if self._show_status_line:
            self._output.write("\n")
        self._bar.__enter__()

    def start_rule(self, rule_id: str, rule_index: int, total_rules: int):
        self._rule_label = self._format_rule_label(rule_id, rule_index, total_rules)
        if not self._show_status_line:
            self._output.write(f"Current rule: {self._rule_label}\n")
            self._output.flush()
            return
        self.set_phase(RuleProgressPhase.PARSING)

    def set_phase(self, phase: RuleProgressPhase):
        if not self._show_status_line:
            return
        self._redraw_status_line(f"Current rule: {self._rule_label} - {phase.value}")

    def _redraw_status_line(self, text: str) -> None:
        width = shutil.get_terminal_size(fallback=(120, 24)).columns
        if len(text) > width:
            text = f"{text[: width - 1]}…"
        self._output.write(_CURSOR_UP_AND_CLEAR_LINE)
        self._output.write(text)
        self._output.write("\n\r")
        self._output.flush()

    def end_rule(self):
        if self._bar is not None:
            self._bar.update(1)

    def finish(self):
        if self._bar is not None:
            self._bar.__exit__(None, None, None)
            self._bar = None

    @staticmethod
    def _format_rule_label(rule_id: str, rule_index: int, total_rules: int) -> str:
        return f"{rule_id or 'unknown'} ({rule_index + 1}/{total_rules})"


def get_rule_progress_reporter(progress_option: str) -> RuleProgressReporter:
    if progress_option == ProgressParameterOptions.DISABLED.value:
        return DisabledRuleProgressReporter()
    if progress_option == ProgressParameterOptions.PERCENTS.value:
        return PercentRuleProgressReporter()
    if progress_option == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        return DisabledRuleProgressReporter()
    return ClickRuleProgressBar()
