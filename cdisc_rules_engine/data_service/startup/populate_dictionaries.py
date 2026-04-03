from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.enums.whodrug_files import WhoDrugFormats
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer


def _whodrug_schema(metadata=None) -> SqlTableSchema:
    table = SqlTableSchema.static(StaticTables.WHODRUG_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="atc_code", hash="atc_code", type="Char"))
    table.add_column(SqlColumnSchema(name="level_1", hash="level_1", type="Char"))
    table.add_column(SqlColumnSchema(name="level_2", hash="level_2", type="Char"))
    table.add_column(SqlColumnSchema(name="level_3", hash="level_3", type="Char"))
    table.add_column(SqlColumnSchema(name="level_4", hash="level_4", type="Char"))
    if metadata:
        if metadata.format == WhoDrugFormats.C3.value:
            table.add_column(SqlColumnSchema(name="med_prod_id", hash="med_prod_id", type="Num"))
        elif metadata.format == WhoDrugFormats.B3.value:
            table.add_column(SqlColumnSchema(name="drug_rec_num", hash="drug_rec_num", type="Num"))
    table.add_column(SqlColumnSchema(name="drug_name", hash="drug_name", type="Char"))
    return table


def _meddra_schema(metadata=None) -> SqlTableSchema:
    # metadata object is not currently used but will be relevant when I plan to implement more version logic
    table = SqlTableSchema.static(StaticTables.MEDDRA_TABLE_NAME.value)
    table.add_column(SqlColumnSchema(name="term_code", hash="term_code", type="Char"))
    table.add_column(SqlColumnSchema(name="term_name", hash="term_name", type="Char"))
    table.add_column(SqlColumnSchema(name="term_type", hash="term_type", type="Char"))
    return table


_SCHEMA_MAP = {
    DictionaryTypes.WHODRUG.value: _whodrug_schema,
    DictionaryTypes.MEDDRA.value: _meddra_schema,
}


def populate_dictionaries(pgi: PostgresQLInterface, external_dictionaries: SqlExternalDictionariesContainer):
    """Populates the dictionary tables with the provided external dictionaries."""
    if not external_dictionaries:
        return

    for dictionary_type, reader in external_dictionaries.get_all_implemented_reader_classes().items():
        path = external_dictionaries.get_dictionary_path(dictionary_type)
        reader_instance = reader(pgi, path)

        metadata = reader_instance._extract_version_metadata()
        schema: SqlTableSchema = _SCHEMA_MAP[dictionary_type](metadata)
        pgi.create_table(schema)

        df = reader_instance.process_data(metadata)
        columns_to_insert = [col_name for col_name, _ in schema.get_columns() if col_name != "id"]
        records = df[columns_to_insert].to_dict(orient="records")

        pgi.insert_data(schema.name, records)
