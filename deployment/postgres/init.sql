CREATE TABLE campaign_versions (
    campaign_id TEXT NOT NULL,
    effective_from BIGINT NOT NULL,
    effective_to BIGINT,
    source_version BIGINT NOT NULL,
    attributes JSONB NOT NULL,
    PRIMARY KEY (campaign_id, effective_from, source_version),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);
ALTER TABLE campaign_versions REPLICA IDENTITY FULL;
INSERT INTO campaign_versions VALUES
('campaign-0', 0, NULL, 1, '{"channel":"short-video"}'),
('campaign-1', 0, NULL, 1, '{"channel":"feed"}'),
('campaign-2', 0, NULL, 1, '{"channel":"search"}'),
('campaign-3', 0, NULL, 1, '{"channel":"short-video"}');

CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'adpulse-cdc-local';
GRANT CONNECT ON DATABASE adpulse TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON campaign_versions TO debezium;
CREATE PUBLICATION adpulse_publication FOR TABLE campaign_versions;

CREATE TABLE releases (
    release_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('live','replay')),
    status TEXT NOT NULL CHECK (status IN ('building','validated','active','retired','failed')),
    topic TEXT UNIQUE NOT NULL,
    partition_count INTEGER NOT NULL,
    rules_sha256 TEXT NOT NULL,
    rules JSONB NOT NULL,
    manifest_sha256 TEXT,
    report JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    validated_at TIMESTAMPTZ
);
CREATE TABLE active_release (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), release_id TEXT REFERENCES releases);
CREATE TABLE release_audit (
    id BIGSERIAL PRIMARY KEY, previous_release TEXT, next_release TEXT NOT NULL,
    reason TEXT NOT NULL, changed_at TIMESTAMPTZ DEFAULT now()
);
