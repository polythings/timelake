-- QuestDB's tuned Industrial IoT tables (D23): an hourly materialized view per tag, as its docs build one
-- (SAMPLE BY), refreshed as the readings arrive; a run waits for it to catch up before querying. The tuned queries
-- (`[questdb tuned]`) read it.
CREATE MATERIALIZED VIEW readings_1h AS (
    SELECT time, tag, min(value) AS lo, max(value) AS hi, sum(value) AS total, count(value) AS valued, count() AS samples
    FROM readings
    SAMPLE BY 1h
) PARTITION BY DAY;
