CREATE TABLE IF NOT EXISTS project_mapping (
    id SERIAL PRIMARY KEY,
    NrCommessa VARCHAR(20) NOT NULL,
    openproject_id INTEGER NOT NULL,
    project_hash CHAR(32) NOT NULL,
    admin_hash CHAR(32) NOT NULL,
    last_sync TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(commessa_id)
);

-- Indici per performance
CREATE INDEX idx_commessa_id ON project_mapping(commessa_id);
CREATE INDEX idx_openproject_id ON project_mapping(openproject_id);

-- CREATE TABLE IF NOT EXISTS user_mapping (
--     id SERIAL PRIMARY KEY,
--     openproject_id INTEGER NOT NULL,
--     user_name VARCHAR(30) NOT NULL,
--     last_sync TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );