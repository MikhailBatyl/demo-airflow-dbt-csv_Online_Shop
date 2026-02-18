CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.events (
    event_id   UInt64,
    ts         DateTime,
    equipment  String,
    metric     Float64
)
ENGINE = MergeTree
ORDER BY (equipment, ts);