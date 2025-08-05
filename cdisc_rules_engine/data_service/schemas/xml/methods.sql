CREATE TABLE IF NOT EXISTS methods (
    method_id SERIAL PRIMARY KEY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    method_oid VARCHAR(200) NOT NULL,
    method_name VARCHAR(500),
    method_type VARCHAR(50),
    description TEXT,
    UNIQUE (version_id, method_oid)
)