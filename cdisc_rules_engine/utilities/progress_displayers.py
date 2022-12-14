import sys
from typing import List, Iterable, Callable, Dict

import click

from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args


def _disabled_progress_displayer(
    rules: List[dict],
    validation_results: Iterable[RuleValidationResult],
    results: List[RuleValidationResult],
) -> List[RuleValidationResult]:
    """
    Doesn't display any progress.
    """
    for rule_result in validation_results:
        results.append(rule_result)
    return results


def _percents_progress_displayer(
    rules: List[dict],
    validation_results: Iterable[RuleValidationResult],
    results: List[RuleValidationResult],
) -> List[RuleValidationResult]:
    """
    Prints validation progress like:
    5
    8
    10
    ...
    """
    counter = 0
    rules_len = len(rules)
    for rule_result in validation_results:
        counter += 1
        current_progress: int = int(counter / rules_len * 100)
        sys.stdout.write(f"{current_progress}\n")
        sys.stdout.flush()
        results.append(rule_result)
    return results


def _bar_progress_displayer(
    rules: List[dict],
    validation_results: Iterable[RuleValidationResult],
    results: List[RuleValidationResult],
) -> List[RuleValidationResult]:
    """
    Prints a progress bar like:
    [████████████████████████████--------]   78%
    """
    with click.progressbar(
        length=len(rules),
        fill_char=click.style("\u2588", fg="green"),
        empty_char=click.style("-", fg="white", dim=True),
        show_eta=False,
    ) as bar:
        for rule_result in validation_results:
            results.append(rule_result)
            bar.update(1)
    return results


def get_progress_displayer(args: Validation_args) -> Callable:
    """
    Returns corresponding progress handler (bar, percents or disabled)
    based on the input parameters.
    By default, a progress bar is returned.
    """
    handlers_map: Dict[str, Callable] = {
        ProgressParameterOptions.DISABLED.value: _disabled_progress_displayer,
        ProgressParameterOptions.PERCENTS.value: _percents_progress_displayer,
        # Disable progress on verbose output
        ProgressParameterOptions.VERBOSE_OUTPUT.value: _disabled_progress_displayer,
    }
    return handlers_map.get(args.progress, _bar_progress_displayer)
