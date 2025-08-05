CREATE TABLE IF NOT EXISTS variable_codelist_refs (
    ref_id SERIAL PRIMARY KEY,
    variable_id INTEGER NOT NULL REFERENCES variables(variable_id),
    codelist_id INTEGER NOT NULL REFERENCES codelists(codelist_id),
    UNIQUE (variable_id, codelist_id)
)
