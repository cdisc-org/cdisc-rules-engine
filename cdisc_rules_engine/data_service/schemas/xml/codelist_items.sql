CREATE TABLE IF NOT EXISTS codelist_items (
    item_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    codelist_id INTEGER NOT NULL REFERENCES codelists(codelist_id),
    coded_value VARCHAR(500),
    decode_text TEXT,
    rank INTEGER,
    order_number INTEGER
)

CREATE INDEX IF NOT EXISTS idx_coded_value ON codelist_items(codelist_id, coded_value)