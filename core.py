import asyncio
from collections import namedtuple
from datetime import datetime
from multiprocessing import freeze_support

import click

from scripts.run_validation import run_validation
from cdisc_rules_engine.utilities.utils import generate_report_filename
from cdisc_rules_engine.services.cache.cache_populator_service import CachePopulator
from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService

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
    ],
)


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "-ca",
    "--cache",
    default="cdisc_rules_engine/resources/cache",
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
    default="cdisc_rules_engine/resources/templates/report-template.xlsx",
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
):
    """
    Validate data using CDISC Rules Engine

    Example:

    python core.py -s SDTM -v 3.4 -d /path/to/datasets
    """
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
        )
    )


@click.command()
@click.option(
    "-c",
    "--cache_path",
    default="resources/cache",
    help="Relative path to cache files containing pre loaded metadata and rules",
)
@click.option(
    "--apikey",
    envvar="CDISC_LIBRARY_API_KEY",
    help="CDISC Library api key. Can be provided in the environment variable CDISC_LIBRARY_API_KEY",
    required=True,
)
@click.pass_context
def update_cache(ctx: click.Context, cache_path: str, apikey: str):
    cache = CacheServiceFactory(config).get_cache_service()
    library_service = CDISCLibraryService(apikey, cache)
    cache_populator = CachePopulator(cache, library_service)
    cache = asyncio.run(cache_populator.load_cache_data())
    cache_populator.save_rules_locally(cache_path)
    cache_populator.save_ct_packages_locally(cache_path)
    cache_populator.save_standards_metadata_locally(cache_path)
    cache_populator.save_variable_codelist_maps_locally(cache_path)
    cache_populator.save_variables_metadata_locally(cache_path)


cli.add_command(validate)
cli.add_command(update_cache)

if __name__ == "__main__":
    freeze_support()
    cli()
