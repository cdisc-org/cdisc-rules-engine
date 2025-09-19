import logging
from pathlib import Path

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.metadata_standards_reader import MetadataStandardsReader

# TODO: Put this somewhere sensible
IG_DATASETS_TABLE_NAME = "ig_datasets"
IG_VARIABLES_TABLE_NAME = "ig_variables"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schemas"


def _dataset_schema():
    table = SqlTableSchema.static(IG_DATASETS_TABLE_NAME)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version", hash="version", type="Char"))
    table.add_column(SqlColumnSchema(name="class", hash="class", type="Char"))
    table.add_column(SqlColumnSchema(name="dataset_name", hash="dataset_name", type="Char"))
    table.add_column(SqlColumnSchema(name="dataset_label", hash="dataset_label", type="Char"))
    table.add_column(SqlColumnSchema(name="structure", hash="structure", type="Char"))
    table.add_column(SqlColumnSchema(name="structure_name", hash="structure_name", type="Char"))
    table.add_column(SqlColumnSchema(name="structure_description", hash="structure_description", type="Char"))
    table.add_column(SqlColumnSchema(name="subclass", hash="subclass", type="Char"))
    table.add_column(SqlColumnSchema(name="notes", hash="notes", type="Char"))
    return table


def _variable_schema():
    table = SqlTableSchema.static(IG_VARIABLES_TABLE_NAME)
    table.add_column(SqlColumnSchema(name="standard_type", hash="standard_type", type="Char"))
    table.add_column(SqlColumnSchema(name="version", hash="version", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_order", hash="variable_order", type="Num"))
    table.add_column(SqlColumnSchema(name="class", hash="class", type="Char"))
    table.add_column(SqlColumnSchema(name="dataset_name", hash="dataset_name", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_name", hash="variable_name", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_label", hash="dataset_label", type="Char"))
    table.add_column(SqlColumnSchema(name="structure_name", hash="structure_name", type="Char"))
    table.add_column(SqlColumnSchema(name="variable_set", hash="variable_set", type="Char"))
    table.add_column(SqlColumnSchema(name="type", hash="type", type="Char"))
    table.add_column(SqlColumnSchema(name="codelist_code", hash="codelist_code", type="Char"))
    table.add_column(SqlColumnSchema(name="submission_value", hash="submission_value", type="Char"))
    table.add_column(SqlColumnSchema(name="value_domain", hash="value_domain", type="Char"))
    table.add_column(SqlColumnSchema(name="value_list", hash="value_list", type="Char"))
    table.add_column(SqlColumnSchema(name="role", hash="role", type="Char"))
    table.add_column(SqlColumnSchema(name="notes", hash="notes", type="Char"))
    table.add_column(SqlColumnSchema(name="core", hash="core", type="Char"))
    return table


def populate_standards(pgi: PostgresQLInterface, path: Path = None):
    """
    Create all necessary SQL tables for IG standards.
    """
    if not path:
        logger.info("No metadata standards path provided, will use cached IG metadata")
        # TODO: Use a default path or configuration for metadata
        return

    if not path.exists():
        logger.warning(f"Metadata standards path {path} does not exist")
        return

    ds_schema = _dataset_schema()
    var_schema = _variable_schema()
    pgi.create_table(ds_schema)
    # TODO: INDEX

    for file_path in path.iterdir():
        try:
            reader = MetadataStandardsReader(str(file_path))
            ig_data = reader.read()

            if ig_data.get("datasets"):
                pgi.insert_data(ds_schema.hash, ig_data["datasets"])

            if ig_data.get("variables"):
                pgi.insert_data(var_schema.hash, ig_data["variables"])

            logger.info(f"Loaded IG metadata from {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to load IG metadata {file_path.name}: {e}")
            continue
