import click
import logging
from datetime import datetime
from collections import namedtuple
from run_validation import run_validation
from engine.utilities.utils import generate_report_filename
from multiprocessing import freeze_support


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "-ca",
    "--cache",
    default="resources/cache",
    help="Relative path to cache files containing pre loaded metadata and rules",
)
@click.option(
    "-p",
    "--pool_size",
    default=10,
    type=int,
    help="Number of parallel processes for validation",
)
@click.option(
    "-d",
    "--data",
    required=True,
    help="Relative path to directory containing data files",
)
@click.option(
    "-l",
    "--log_level",
    default="disabled",
    help="Sets log level for engine logs, logs are disabled by default",
)
@click.option(
    "-rt",
    "--report_template",
    default="resources/templates/report-template.xlsx",
    help="File path of report template to use for excel output",
)
@click.option(
    "-s", "--standard", required=True, help="CDISC standard to validate against"
)
@click.option(
    "-v", "--version", required=True, help="Standard version to validate against"
)
@click.option(
    "-ct",
    "--controlled_terminology_package",
    multiple=True,
    help="Controlled terminology package to validate against, can provide more than one",
)
@click.option(
    "-o",
    "--output",
    default=generate_report_filename(datetime.now().isoformat()),
    help="Report output file destination",
)
@click.option(
    "-dv",
    "--define_version",
    default="2.1",
    help="Define-XML version used for validation",
)
@click.option("--whodrug", help="Path to directory with WHODrug dictionary files")
@click.option("--meddra", help="Path to directory with MedDRA dictionary files")
@click.option("--dictionaries_path", help="Path to directory with dictionaries")
@click.pass_context
def validate(
    ctx,
    cache,
    pool_size,
    data,
    log_level,
    report_template,
    standard,
    version,
    controlled_terminology_package,
    output,
    define_version,
    whodrug,
    meddra,
    dictionaries_path,
):
    """
    Validate data using CDISC Rules Engine

    Example:

    python core.py -s SDTM -v 3.4 -d /path/to/datasets
    """
    Validation_args = namedtuple(
        "Validation_args",
        [
            "cache",
            "pool_size",
            "data",
            "log_level",
            "report_template",
            "standard",
            "version",
            "controlled_terminology_package",
            "output",
            "define_version",
            "whodrug",
            "meddra",
            "dictionaries_path",
        ],
    )

    run_validation(
        Validation_args(
            cache,
            pool_size,
            data,
            log_level,
            report_template,
            standard,
            version,
            controlled_terminology_package,
            output,
            define_version,
            whodrug,
            meddra,
            dictionaries_path,
        )
    )


cli.add_command(validate)

if __name__ == "__main__":
    freeze_support()
    cli()
