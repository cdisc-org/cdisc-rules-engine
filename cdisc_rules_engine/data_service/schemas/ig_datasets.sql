CREATE TABLE IF NOT EXISTS ig_datasets (
    standard_type VARCHAR(10),
    version VARCHAR(20),
    class VARCHAR(50),
    dataset_name VARCHAR(50),
    dataset_label VARCHAR(255),
    structure VARCHAR(50),
    structure_name VARCHAR(50),
    structure_description VARCHAR(500),
    subclass VARCHAR(50),
    notes TEXT,
    PRIMARY KEY (standard_type, version, dataset_name)
)