from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.enums.whodrug_files import WhoDrugFormats
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer


def _add_char_columns(table: SqlTableSchema, *column_names: str):
    for col in column_names:
        table.add_column(SqlColumnSchema(name=col, hash=col, type="Char"))


def _create_standard_term_schema(table_name: str) -> SqlTableSchema:
    table = SqlTableSchema.static(table_name)
    _add_char_columns(table, "term_code", "term_name")
    return table


def _whodrug_schema(metadata=None) -> SqlTableSchema:
    table = _create_standard_term_schema(StaticTables.WHODRUG_TABLE_NAME.value)
    _add_char_columns(table, "level_1", "level_2", "level_3", "level_4")
    if metadata:
        if metadata.format == WhoDrugFormats.C3.value:
            table.add_column(SqlColumnSchema(name="med_prod_id", hash="med_prod_id", type="Num"))
        elif metadata.format == WhoDrugFormats.B3.value:
            table.add_column(SqlColumnSchema(name="drug_rec_num", hash="drug_rec_num", type="Num"))

    return table


def _meddra_schema(metadata=None) -> SqlTableSchema:
    table = _create_standard_term_schema(StaticTables.MEDDRA_TABLE_NAME.value)
    _add_char_columns(table, "term_type")
    return table


def _unii_schema(metadata=None) -> SqlTableSchema:
    return _create_standard_term_schema(StaticTables.UNII_TABLE_NAME.value)


def _medrt_schema(metadata=None) -> SqlTableSchema:
    table = _create_standard_term_schema(StaticTables.MEDRT_TABLE_NAME.value)
    _add_char_columns(table, "term_tag")
    return table


def _loinc_schema(metadata=None) -> SqlTableSchema:
    table = _create_standard_term_schema(StaticTables.LOINC_TABLE_NAME.value)
    _add_char_columns(table, "version")
    return table


_SCHEMA_MAP = {
    DictionaryTypes.WHODRUG.value: _whodrug_schema,
    DictionaryTypes.MEDDRA.value: _meddra_schema,
    DictionaryTypes.UNII.value: _unii_schema,
    DictionaryTypes.MEDRT.value: _medrt_schema,
    DictionaryTypes.LOINC.value: _loinc_schema,
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
