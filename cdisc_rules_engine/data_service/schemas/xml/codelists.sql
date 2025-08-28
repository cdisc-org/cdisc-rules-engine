CREATE TABLE IF NOT EXISTS codelists (
    codelist_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    codelist_oid VARCHAR(200) NOT NULL,
    codelist_name VARCHAR(200),
    data_type VARCHAR(50),
    is_external BOOLEAN DEFAULT FALSE,
    external_dictionary VARCHAR(100),
    external_version VARCHAR(50),
    UNIQUE (version_id, codelist_oid)
)
