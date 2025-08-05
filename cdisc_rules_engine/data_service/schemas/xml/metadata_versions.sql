CREATE TABLE IF NOT EXISTS metadata_versions (
    version_id SERIAL PRIMARY KEY,
    study_id INTEGER NOT NULL REFERENCES studies(study_id),
    version_oid VARCHAR(200) NOT NULL,
    version_name VARCHAR(500),
    description TEXT,
    define_version VARCHAR(50),
    standard_name VARCHAR(50),
    standard_version VARCHAR(50),
    creation_datetime TIMESTAMP,
    UNIQUE (study_id, version_oid)
)