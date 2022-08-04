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
@click.option("-ca", "--cache", default="resources/cache", help="Cache location")
@click.option(
    "-p",
    "--pool_size",
    default=10,
    type=int,
    help="Number of parallel processes for validation",
)
@click.option("-d", "--data", required=True, help="Data location")
@click.option("-l", "--log_level", default="disabled", help="Log level")
@click.option(
    "-rt",
    "--report_template",
    default="resources/templates/report-template.xlsx",
    help="File Path of report template",
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
    help="Controlled terminology package to validate against",
)
@click.option(
    "-o",
    "--output",
    default=generate_report_filename(datetime.now().isoformat()),
    help="Report output file",
)
@click.option(
    "-dv", "--define_version", default="2.1", help="Define version used for validation"
)
@click.option(
    "-dp", "--dictionaries_path", help="Path to directory with dictionaries files"
)
@click.option(
    "-dt",
    "--dictionary_type",
    type=click.Choice(["WHODrug", "MedDRA"], case_sensitive=False),
    help="Dictionary type (MedDRA, WHODrug). Required if dictionaries_path is provided.",
)
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
    dictionaries_path,
    dictionary_type,
):
    """
    Validate data using CDISC Rules Engine

    Example:

    python core.py -s SDTM -v 3.4 -d /path/to/datasets
    """

    logger = logging.getLogger("validator")
    # Check dependent options
    if dictionaries_path != None and dictionary_type == None:
        logger.error(
            "Option dictionary_type must be provided when dictionary_path is provided."
        )
        ctx.abort()

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
            "dictionaries_path",
            "dictionary_type",
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
            dictionaries_path,
            dictionary_type,
        )
    )


cli.add_command(validate)

if __name__ == "__main__":
    freeze_support()
    cli()
