CREATE TABLE IF NOT EXISTS value_lists (
    value_list_id SERIAL PRIMARY KEY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    value_list_oid VARCHAR(200) NOT NULL,
    UNIQUE (version_id, value_list_oid)
)