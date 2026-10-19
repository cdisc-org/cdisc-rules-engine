import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.codelist_reader import CodelistReader
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.ingestion_progress import (
    DisabledIngestionProgressReporter,
    IngestionProgressReporter,
)

ROOT_PATH = Path(__file__).parents[3]

_VERSION_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def _schema():
    table = SqlTableSchema.static(StaticTables.IG_CODELIST_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version_date", hash="version_date", type="Char"))
    table.add_column(SqlColumnSchema(name="item_code", hash="item_code", type="Char"))
    table.add_column(SqlColumnSchema(name="codelist_code", hash="codelist_code", type="Char"))
    table.add_column(SqlColumnSchema(name="extensible", hash="extensible", type="Char"))
    table.add_column(SqlColumnSchema(name="name", hash="name", type="Char"))
    table.add_column(SqlColumnSchema(name="value", hash="value", type="Char"))
    table.add_column(SqlColumnSchema(name="synonym", hash="synonym", type="Char"))
    table.add_column(SqlColumnSchema(name="definition", hash="definition", type="Char"))
    table.add_column(SqlColumnSchema(name="term", hash="term", type="Char"))
    table.add_column(SqlColumnSchema(name="standard_and_date", hash="standard_and_date", type="Char"))
    return table


def populate_codelists(
    pgi: PostgresQLInterface,
    cache_path: str,
    codelists: Optional[List[str]],
    progress_reporter: Optional[IngestionProgressReporter] = None,
):
    """Populate the codelists table in the database."""
    valid_ct_paths = []
    invalid_ct_paths = []

    if not codelists:
        return

    codelists = [item for item in codelists if isinstance(item, str)]

    for file_path in codelists:
        path = ROOT_PATH / Path(cache_path) / Path(file_path)
        if path.exists() and path.is_file():
            valid_ct_paths.append(path)
        else:
            invalid_ct_paths.append(path)

    if invalid_ct_paths:
        logger.warning(f"The following requested codelists were not found: {invalid_ct_paths}")

    schema = _schema()
    pgi.create_table(schema)

    reporter = progress_reporter or DisabledIngestionProgressReporter()
    reporter.start(total_files=len(valid_ct_paths), total_bytes=sum(path.stat().st_size for path in valid_ct_paths))

    try:
        for file_index, file_path in enumerate(valid_ct_paths):
            try:
                reader = CodelistReader(str(file_path))
                codelist_data = reader.read() or []

                reporter.start_file(
                    file_index=file_index,
                    total_files=len(valid_ct_paths),
                    file_name=file_path.name,
                    file_bytes=file_path.stat().st_size,
                    total_rows=len(codelist_data),
                )

                if codelist_data:
                    pgi.insert_data(schema.hash, codelist_data)
                    reporter.report_chunk(len(codelist_data))
                    logger.info(f"Loaded codelist from {file_path.name}")
                else:
                    logger.warning(f"No data found in codelist file: {file_path.name}")

                reporter.end_file(len(codelist_data))

            except Exception as e:
                logger.error(f"Failed to load codelist {file_path.name}: {e}")
                continue
    finally:
        reporter.finish()


def populate_referenced_codelists(
    pgi: PostgresQLInterface,
    cache_path: Optional[str],
    ct_type: str,
    version_dates: Iterable[str],
    referenced_by: str,
):
    """
    Loads the cached CT packages of the given type (e.g. SDTM) for the given version dates, if not loaded yet.
    The version dates come from dataset values (e.g. TSVCDVER), so only plain YYYY-MM-DD dates become file names.
    """
    if not cache_path:
        return
    versions = {v for v in version_dates if isinstance(v, str) and _VERSION_DATE_PATTERN.fullmatch(v)}
    to_load = []
    for version in sorted(versions - _loaded_versions(pgi, ct_type)):
        file_name = f"{ct_type}ct-{version}.pkl"
        if (ROOT_PATH / Path(cache_path) / file_name).is_file():
            to_load.append(file_name)
        else:
            logger.warning(
                f"CT package {ct_type}ct-{version} referenced by {referenced_by} is not available "
                f"in the cache ({cache_path}): records referencing it fall back to the provided CT packages if any, "
                "otherwise they are matched to no CT package"
            )
    populate_codelists(pgi, cache_path, to_load)


def populate_latest_codelist(pgi: PostgresQLInterface, cache_path: Optional[str], ct_type: str):
    """Loads the most recent cached CT package of the given type (e.g. SDTM), if none of that type is loaded yet."""
    if not cache_path or _loaded_versions(pgi, ct_type):
        return
    prefix = f"{ct_type}ct-"
    cached = sorted(
        path.stem[len(prefix) :]
        for path in (ROOT_PATH / Path(cache_path)).glob(f"{prefix}*.pkl")
        if _VERSION_DATE_PATTERN.fullmatch(path.stem[len(prefix) :])
    )
    if cached:
        populate_codelists(pgi, cache_path, [f"{prefix}{cached[-1]}.pkl"])


def _loaded_versions(pgi: PostgresQLInterface, ct_type: str) -> set:
    table_name = StaticTables.IG_CODELIST_TABLE_NAME.value
    if not pgi.schema.get_table(table_name):
        return set()
    pgi.execute_sql(f"SELECT DISTINCT version_date FROM {table_name} WHERE standard_type = %s", (ct_type,))
    return {row["version_date"] for row in pgi.fetch_all()}


def add_extensible_terms(
    pgi: PostgresQLInterface,
    extensible_terms: Optional[Dict[str, Dict[str, Any]]] = None,
):
    """Add extensible terms to the codelists table in the database."""
    if not extensible_terms:
        return

    pgi.execute_sql(
        f"DELETE FROM {StaticTables.IG_CODELIST_TABLE_NAME.value} WHERE standard_type IS NULL AND extensible = 'Yes'"
    )

    data = []
    for name, details in extensible_terms.items():
        for val in details.get("extended_values", []):
            data.append({"codelist_code": details.get("codelist"), "name": name, "extensible": "Yes", "value": val})
    pgi.insert_data(StaticTables.IG_CODELIST_TABLE_NAME.value, data)
    logger.info(f"Added extensible terms: {list(extensible_terms.keys())}")
