CREATE TABLE IF NOT EXISTS dataset_variables (
    dataset_var_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    dataset_id INTEGER NOT NULL REFERENCES datasets(dataset_id),
    variable_id INTEGER NOT NULL REFERENCES variables(variable_id),
    order_number INTEGER,
    mandatory BOOLEAN,
    key_sequence INTEGER,
    role VARCHAR(100),
    role_codelist_oid VARCHAR(200),
    method_oid VARCHAR(200),
    UNIQUE (dataset_id, variable_id)
)

CREATE INDEX IF NOT EXISTS idx_order ON dataset_variables(dataset_id, order_number)