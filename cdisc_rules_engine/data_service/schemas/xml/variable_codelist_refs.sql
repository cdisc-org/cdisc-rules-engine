CREATE TABLE IF NOT EXISTS variable_codelist_refs (
    ref_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    variable_id INTEGER NOT NULL REFERENCES variables(variable_id),
    codelist_id INTEGER NOT NULL REFERENCES codelists(codelist_id),
    UNIQUE (variable_id, codelist_id)
)
