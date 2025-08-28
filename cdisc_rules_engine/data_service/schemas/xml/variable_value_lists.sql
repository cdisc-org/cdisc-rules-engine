CREATE TABLE IF NOT EXISTS variable_value_lists (
    ref_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    variable_id INTEGER NOT NULL REFERENCES variables(variable_id),
    value_list_id INTEGER NOT NULL REFERENCES value_lists(value_list_id),
    UNIQUE (variable_id, value_list_id)
)