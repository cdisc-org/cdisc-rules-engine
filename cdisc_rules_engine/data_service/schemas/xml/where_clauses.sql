CREATE TABLE IF NOT EXISTS where_clauses (
    where_clause_id SERIAL PRIMARY KEY,
    version_id INTEGER NOT NULL REFERENCES metadata_versions(version_id),
    where_clause_oid VARCHAR(200) NOT NULL,
    UNIQUE (version_id, where_clause_oid)
)