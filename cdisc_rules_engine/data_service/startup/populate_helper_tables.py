import json
from pathlib import Path

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services import logger


def _cg_taugs_schema():
    table = SqlTableSchema.static(StaticTables.CG_TAUGS_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="fda_guides", hash="fda_guides", type="Char"))
    return table


def _fda_submission_standards_schema():
    table = SqlTableSchema.static(StaticTables.FDA_STANDARDS_TABLE_NAME.value)
    for col in ["use", "standard", "fda_centers", "exchange_format", "sdo", "property", "property_version"]:
        table.add_column(SqlColumnSchema(name=col, hash=col, type="Char"))
    return table


SCHEMA_MAP = {
    Path("taugs.json"): _cg_taugs_schema,
    Path("fda-submission-standards.json"): _fda_submission_standards_schema,
}

ROOT_PATH = Path(__file__).parents[3]
HELPER_DATA_PATH = ROOT_PATH / "resources/helper_data"


def populate_helper_tables(pgi: PostgresQLInterface):
    """Populate the helper tables in the database."""
    valid_helper_paths = []

    for file_path in SCHEMA_MAP:
        path = HELPER_DATA_PATH / file_path
        if path.exists() and path.is_file():
            valid_helper_paths.append(path)

    if not valid_helper_paths:
        logger.warning("No valid helper data files found to populate helper tables.")
        return

    for file_path, schema_func in SCHEMA_MAP.items():
        schema = schema_func()
        pgi.create_table(schema)

        with open(HELPER_DATA_PATH / file_path, "r") as f:
            data = json.load(f)

        if data:
            pgi.insert_data(schema.hash, data)
            logger.info(f"Loaded helper table from {file_path}")
        else:
            logger.warning(f"No data found in helper table file: {file_path.name}")
