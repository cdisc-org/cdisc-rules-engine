CREATE TABLE IF NOT EXISTS value_lists (
    value_list_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    value_list_oid VARCHAR(200) NOT NULL,
    UNIQUE (version_id, value_list_oid)
)