CREATE TABLE IF NOT EXISTS studies (
    study_id SERIAL PRIMARY KEY,
    study_oid VARCHAR(100) UNIQUE NOT NULL,
    study_name VARCHAR(200),
    study_description TEXT,
    protocol_name VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)