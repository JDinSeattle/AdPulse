CREATE DATABASE IF NOT EXISTS adpulse;
CREATE TABLE IF NOT EXISTS adpulse.results (
    release_id String, output_key String, record_type LowCardinality(String),
    output_topic String, output_partition UInt32, output_offset UInt64,
    payload String, hash FixedString(64), received_at UInt64,
    inserted_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(output_offset) ORDER BY (release_id, output_key);

CREATE TABLE IF NOT EXISTS adpulse.deliveries (
    topic String, partition_id UInt32, offset_id UInt64, hash FixedString(64)
) ENGINE = ReplacingMergeTree ORDER BY (topic, partition_id, offset_id);

CREATE TABLE IF NOT EXISTS adpulse.quality (
    output_topic String, output_partition UInt32, output_offset UInt64,
    receipt_id String, disposition LowCardinality(String), error_code LowCardinality(String),
    rule_version String, payload String, inserted_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree ORDER BY (output_topic, output_partition, output_offset);

CREATE TABLE IF NOT EXISTS adpulse.receipts (
    batch_id String, received_at UInt64, receipt_ids Array(String), payload String
) ENGINE = ReplacingMergeTree ORDER BY batch_id;

CREATE VIEW IF NOT EXISTS adpulse.latest_results AS
SELECT release_id, output_key, argMax(payload, output_offset) AS payload,
       max(inserted_at) AS last_inserted_at
FROM adpulse.results GROUP BY release_id, output_key;

CREATE TABLE IF NOT EXISTS adpulse.visibility (
    release_id String, receipt_id String, received_at UInt64, event_time UInt64,
    output_topic String, output_partition UInt32, output_offset UInt64,
    visible_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree ORDER BY (release_id, receipt_id);
