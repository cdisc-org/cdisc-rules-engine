from pathlib import Path
from typing import Any, Dict, List, Optional

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.codelist_reader import CodelistReader
from cdisc_rules_engine.services import logger

ROOT_PATH = Path(__file__).parents[3]


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

    for file_path in valid_ct_paths:
        try:
            reader = CodelistReader(str(file_path))
            codelist_data = reader.read()

            if codelist_data:
                pgi.insert_data(schema.hash, codelist_data)
                logger.info(f"Loaded codelist from {file_path.name}")
            else:
                logger.warning(f"No data found in codelist file: {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to load codelist {file_path.name}: {e}")
            continue


def add_extensible_terms(
    pgi: PostgresQLInterface,
    extensible_terms: Optional[Dict[str, Dict[str, Any]]] = None,
):
    """Add extensible terms to the codelists table in the database."""
    if not extensible_terms:
        return

    data = []
    for name, details in extensible_terms.items():
        for val in details.get("extended_values", []):
            data.append({"codelist_code": details.get("codelist"), "name": name, "extensible": "Yes", "value": val})
    pgi.insert_data(StaticTables.IG_CODELIST_TABLE_NAME.value, data)
    logger.info(f"Added extensible terms: {list(extensible_terms.keys())}")
