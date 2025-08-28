CREATE TABLE IF NOT EXISTS where_clause_conditions (
    condition_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    where_clause_id INTEGER NOT NULL REFERENCES where_clauses(where_clause_id),
    item_oid VARCHAR(200),
    comparator VARCHAR(10),
    check_value TEXT,
    soft_hard VARCHAR(10)
)