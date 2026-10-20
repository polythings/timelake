-- Timelake's tuned TSBS IoT tables (D23): 10-minute rollups per truck, the window most of TSBS's fleet queries
-- group by. Inline maintenance keeps them fresh, and the reader routes queries to them, with the buckets changed
-- since the last refresh read from the raw tables.
CREATE MATERIALIZED ROLLUP readings_10m AS
SELECT tags_id, time_bucket(INTERVAL '10 minutes', time) AS bucket,
       sum(velocity) AS velocity_sum, count(velocity) AS velocity_n,
       sum(fuel_consumption) AS fuel_sum, count(fuel_consumption) AS fuel_n, count(*) AS n
FROM readings
GROUP BY tags_id, bucket;

CREATE MATERIALIZED ROLLUP diagnostics_10m AS
SELECT tags_id, time_bucket(INTERVAL '10 minutes', time) AS bucket,
       sum(status) AS status_sum, count(status) AS status_n,
       sum(current_load) AS load_sum, count(current_load) AS load_n, count(*) AS n
FROM diagnostics
GROUP BY tags_id, bucket;
