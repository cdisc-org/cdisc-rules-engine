CREATE TABLE IF NOT EXISTS datasets (
    dataset_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    dataset_oid VARCHAR(200) NOT NULL,
    domain VARCHAR(10),
    dataset_name VARCHAR(40),
    description TEXT,
    repeating BOOLEAN,
    purpose VARCHAR(50),
    is_reference_data BOOLEAN,
    sas_dataset_name VARCHAR(32),
    structure TEXT,
    class VARCHAR(100),
    comment_oid VARCHAR(200),
    archive_location_id VARCHAR(200),
    UNIQUE (version_id, dataset_oid)
)