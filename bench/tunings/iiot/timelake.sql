-- Timelake's tuned Industrial IoT tables (D23): an hourly rollup per tag, as the rollups docs set one up for trends,
-- reports, signal aggregates and gaps. Inline maintenance keeps it fresh, and the reader routes queries to it, with
-- the buckets changed since its last refresh read from the readings.
CREATE MATERIALIZED ROLLUP readings_1h AS
SELECT tag, time_bucket(INTERVAL '1 hour', time) AS bucket,
       min(value) AS lo, max(value) AS hi, sum(value) AS total, count(value) AS valued, count(*) AS samples,
       time_weighted_mean(time, value) AS twa, gap_count(time, INTERVAL '5 minutes') AS gaps
FROM readings
GROUP BY tag, bucket;
