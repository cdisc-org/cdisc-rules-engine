CREATE TABLE IF NOT EXISTS comments (
    comment_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    comment_oid VARCHAR(200) NOT NULL,
    comment_text TEXT,
    UNIQUE (version_id, comment_oid)
)
