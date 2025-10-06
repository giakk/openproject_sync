CREATE TABLE IF NOT EXISTS cached_projects (
    gestionale_id VARCHAR(20) PRIMARY KEY,
    openproject_id INTEGER,
    current_hash VARCHAR(64),
    last_sync_hash VARCHAR(64),
    sync_status CHAR(32) NOT NULL,
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(openproject_id)
);

CREATE INDEX idx_gestionale_id ON cached_projects(gestionale_id);
CREATE INDEX idx_openproject_id ON cached_projects(openproject_id);
