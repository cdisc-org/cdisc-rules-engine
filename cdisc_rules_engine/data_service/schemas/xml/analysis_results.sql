CREATE TABLE IF NOT EXISTS analysis_results (
    result_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    result_oid VARCHAR(200) NOT NULL,
    display_oid VARCHAR(200),
    result_identifier VARCHAR(200),
    parameter_oid VARCHAR(200),
    analysis_reason VARCHAR(200),
    analysis_purpose TEXT,
    description TEXT,
    documentation TEXT,
    programming_code TEXT,
    programming_context VARCHAR(100),
    UNIQUE (version_id, result_oid)
)
