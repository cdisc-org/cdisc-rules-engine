CREATE TABLE IF NOT EXISTS value_list_items (
    value_item_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    value_list_id INTEGER NOT NULL REFERENCES value_lists(value_list_id),
    item_oid VARCHAR(200),
    order_number INTEGER,
    mandatory BOOLEAN,
    method_oid VARCHAR(200),
    where_clause_oid VARCHAR(200)
)