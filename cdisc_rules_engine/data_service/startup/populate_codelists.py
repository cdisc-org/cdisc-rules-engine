import logging
from pathlib import Path

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.codelist_reader import CodelistReader

# TODO: Put this somewhere sensible
CODELIST_TABLE_NAME = "ig_codelists"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schemas"


def _schema():
    table = SqlTableSchema.static(CODELIST_TABLE_NAME)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version_date", hash="version_date", type="Date"))
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


def populate_codelists(pgi: PostgresQLInterface, path: Path = None):
    """
    Create tables to store CDISC codelists
    """
    if not path:
        logger.info("No codelists path provided, will use cached CDISC codelists")
        # TODO: Use a default path or configuration for codelists
        return

    if not path.exists():
        logger.warning(f"Codelists path {path} does not exist")
        return

    schema = _schema()
    pgi.create_table(schema)
    # TODO: INDEX

    for file_path in path.iterdir():
        try:
            reader = CodelistReader(str(file_path))
            codelist_data = reader.read()

            if codelist_data:
                pgi.insert_data(schema.hash, codelist_data)
                logger.info(f"Loaded codelist from {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to load codelist {file_path.name}: {e}")
            continue
