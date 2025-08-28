CREATE TABLE IF NOT EXISTS documents (
    document_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    leaf_id VARCHAR(200) NOT NULL,
    href VARCHAR(500),
    title VARCHAR(500),
    UNIQUE (version_id, leaf_id)
)
