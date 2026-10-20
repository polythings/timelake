-- TimescaleDB's tuned Industrial IoT tables (D23): an hourly continuous aggregate per tag, with real-time aggregation
-- so a query also sees what isn't materialized yet, and a refresh policy as its docs set one up. Maintenance refreshes
-- it to the end; the tuned queries (`[postgres tuned]`) read it.
CREATE MATERIALIZED VIEW readings_1h WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT tag, time_bucket(INTERVAL '1 hour', time) AS bucket,
       min(value) AS lo, max(value) AS hi, sum(value) AS total, count(value) AS valued, count(*) AS samples
FROM readings
GROUP BY tag, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('readings_1h', start_offset => NULL, end_offset => INTERVAL '1 hour',
                                       schedule_interval => INTERVAL '1 minute');
