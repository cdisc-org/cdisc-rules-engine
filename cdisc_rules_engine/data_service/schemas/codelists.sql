CREATE TABLE IF NOT EXISTS codelists (
    standard_type VARCHAR(10),
    version_date DATE,
    item_code VARCHAR(255),
    codelist_code VARCHAR(255),
    extensible VARCHAR(10),
    name VARCHAR(500),
    value VARCHAR(500),
    synonym TEXT,
    definition TEXT,
    term VARCHAR(500),
    standard_and_date VARCHAR(255),
    PRIMARY KEY (standard_type, version_date, codelist_code, item_code)
)