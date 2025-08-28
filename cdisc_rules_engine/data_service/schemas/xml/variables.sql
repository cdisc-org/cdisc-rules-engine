CREATE TABLE IF NOT EXISTS variables (
    variable_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    variable_oid VARCHAR(200) NOT NULL,
    variable_name VARCHAR(40),
    sas_field_name VARCHAR(32),
    data_type VARCHAR(50),
    length INTEGER,
    significant_digits INTEGER,
    display_format VARCHAR(50),
    description TEXT,
    origin_type VARCHAR(50),
    origin_description TEXT,
    comment_oid VARCHAR(200),
    UNIQUE (version_id, variable_oid)
)

CREATE INDEX IF NOT EXISTS idx_var_name ON variables(variable_name)