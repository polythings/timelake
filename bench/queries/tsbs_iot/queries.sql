-- TSBS IoT queries, ported from tsbs/cmd/tsbs_generate_queries/databases/timescaledb/iot.go
-- (TimescaleDB flavour, tagset in a normalized `tags` table).
--
-- Porting rules, so every engine runs the same logic:
--   * Random parameters are fixed: fleet = 'South'; windows end near the data end (2026-01-04).
--   * `INNER JOIN LATERAL (... ORDER BY time DESC LIMIT 1)` (an index lookup in TimescaleDB) becomes
--     ROW_NUMBER() over (tags_id ORDER BY time DESC) = 1, which lake engines can execute.
--   * Engine-specific functions are placeholders filled in by run.py:
--       @B10M(x) 10-minute time bucket, @B1D(x) 1-day bucket, @EPOCH(x) seconds since epoch,
--       @TS('...') timestamptz literal.
--   * Integer divisions that Postgres truncates use float division (results are reported, not checked).
--   * Some engines (ClickHouse) return 0, false or 1970 instead of NULL from lag/lead past a series' edge, so
--     edges are told apart by comparing epoch times (prev_t > 0, stop_t > start_t), not by NULL.
--   * breakdown-frequency: TSBS's `count(STATUS = 0) / count(*)` counts non-NULL booleans, so it is
--     always 1. We use the intended fraction of rows with status = 0.
--   * `-- name: <query> [timelake tuned]` is the version a tuned Timelake run reads (D23), as its docs write it
--     for its rollups. Answers are checked like every other.

-- name: last-loc
-- Fleet map: the latest position of every truck in one fleet.
SELECT t.name, t.driver, r.longitude, r.latitude
FROM tags t
JOIN (SELECT tags_id, longitude, latitude,
             ROW_NUMBER() OVER (PARTITION BY tags_id ORDER BY time DESC) AS rn
      FROM readings) r ON r.tags_id = t.id AND r.rn = 1
WHERE t.name IS NOT NULL AND t.fleet = 'South';

-- name: low-fuel
-- Alert: trucks in one fleet whose latest fuel reading is below 10%.
SELECT t.name, t.driver, d.fuel_state
FROM tags t
JOIN (SELECT tags_id, fuel_state,
             ROW_NUMBER() OVER (PARTITION BY tags_id ORDER BY time DESC) AS rn
      FROM diagnostics) d ON d.tags_id = t.id AND d.rn = 1
WHERE t.name IS NOT NULL AND d.fuel_state < 0.1 AND t.fleet = 'South';

-- name: high-load
-- Alert: trucks in one fleet whose latest load is over 90% of their capacity.
SELECT t.name, t.driver, d.current_load
FROM tags t
JOIN (SELECT tags_id, current_load,
             ROW_NUMBER() OVER (PARTITION BY tags_id ORDER BY time DESC) AS rn
      FROM diagnostics) d ON d.tags_id = t.id AND d.rn = 1
WHERE t.name IS NOT NULL AND d.current_load / t.load_capacity > 0.9 AND t.fleet = 'South';

-- name: stationary-trucks
-- Trucks in one fleet that barely moved: an average speed under 1 over a 10-minute window.
SELECT t.name, t.driver
FROM tags t
JOIN readings r ON r.tags_id = t.id
WHERE r.time >= @TS('2026-01-03 12:00:00') AND r.time < @TS('2026-01-03 12:10:00')
  AND t.name IS NOT NULL AND t.fleet = 'South'
GROUP BY t.name, t.driver
HAVING avg(r.velocity) < 1;

-- name: long-driving-sessions
-- Trucks in one fleet that drove in more than 22 of the 24 ten-minute windows of a 4-hour span.
SELECT t.name, t.driver
FROM tags t
JOIN (SELECT @B10M(time) AS ten_minutes, tags_id
      FROM readings
      WHERE time >= @TS('2026-01-03 08:00:00') AND time < @TS('2026-01-03 12:00:00')
      GROUP BY 1, 2
      HAVING avg(velocity) > 1) r ON t.id = r.tags_id
WHERE t.name IS NOT NULL AND t.fleet = 'South'
GROUP BY t.name, t.driver
HAVING count(r.ten_minutes) > 22;

-- name: long-daily-sessions
-- Trucks in one fleet that drove in more than 60 ten-minute windows of a 24-hour span.
SELECT t.name, t.driver
FROM tags t
JOIN (SELECT @B10M(time) AS ten_minutes, tags_id
      FROM readings
      WHERE time >= @TS('2026-01-02 12:00:00') AND time < @TS('2026-01-03 12:00:00')
      GROUP BY 1, 2
      HAVING avg(velocity) > 1) r ON t.id = r.tags_id
WHERE t.name IS NOT NULL AND t.fleet = 'South'
GROUP BY t.name, t.driver
HAVING count(r.ten_minutes) > 60;

-- name: avg-vs-projected-fuel-consumption
-- Per fleet, the average fuel consumption while moving against the trucks' nominal consumption.
SELECT t.fleet, avg(r.fuel_consumption) AS avg_fuel_consumption,
       avg(t.nominal_fuel_consumption) AS projected_fuel_consumption
FROM tags t
JOIN readings r ON r.tags_id = t.id
WHERE r.velocity > 1
  AND t.fleet IS NOT NULL AND t.nominal_fuel_consumption IS NOT NULL AND t.name IS NOT NULL
GROUP BY t.fleet;

-- name: avg-daily-driving-duration
-- Per truck, the average hours driven a day, counted in ten-minute windows.
WITH ten_minute_driving_sessions AS (
    SELECT @B10M(time) AS ten_minutes, tags_id
    FROM readings
    GROUP BY 1, 2
    HAVING avg(velocity) > 1
), daily_total_session AS (
    SELECT @B1D(ten_minutes) AS day, tags_id, count(*) / 6.0 AS hours
    FROM ten_minute_driving_sessions
    GROUP BY 1, 2
)
SELECT t.fleet, t.name, t.driver, avg(d.hours) AS avg_daily_hours
FROM daily_total_session d
JOIN tags t ON t.id = d.tags_id
GROUP BY t.fleet, t.name, t.driver;

-- name: avg-daily-driving-session
-- Per truck and day, the average length of a driving session, from when it starts driving to when it stops.
WITH driver_status AS (
    SELECT tags_id, @B10M(time) AS ten_minutes, avg(velocity) > 5 AS driving
    FROM readings
    GROUP BY 1, 2
), driver_status_change AS (
    SELECT tags_id, ten_minutes AS start_ts, @EPOCH(ten_minutes) AS start_t,
           lead(@EPOCH(ten_minutes)) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS stop_t, driving
    FROM (SELECT tags_id, ten_minutes, driving,
                 lag(driving) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS prev_driving,
                 lag(@EPOCH(ten_minutes)) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS prev_t
          FROM driver_status) x
    WHERE x.prev_t > 0 AND x.driving <> x.prev_driving
)
SELECT t.name, @B1D(d.start_ts) AS day,
       avg(CASE WHEN d.stop_t > d.start_t THEN d.stop_t - d.start_t END) AS duration_s
FROM tags t
JOIN driver_status_change d ON t.id = d.tags_id
WHERE t.name IS NOT NULL AND d.driving = true
GROUP BY 1, 2
ORDER BY 1, 2;

-- name: avg-load
-- Per fleet, model and capacity, the average load as a share of capacity.
SELECT t.fleet, t.model, t.load_capacity, avg(d.avg_load / t.load_capacity) AS avg_load_percentage
FROM tags t
JOIN (SELECT tags_id, avg(current_load) AS avg_load FROM diagnostics GROUP BY tags_id) d ON t.id = d.tags_id
WHERE t.name IS NOT NULL
GROUP BY t.fleet, t.model, t.load_capacity;

-- name: daily-activity
-- Per fleet, model and day, the share of the day trucks were active.
SELECT t.fleet, t.model, y.day, sum(y.ten_mins_per_day) / 144.0 AS daily_activity
FROM tags t
JOIN (SELECT @B1D(time) AS day, @B10M(time) AS ten_minutes, tags_id, count(*) AS ten_mins_per_day
      FROM diagnostics
      GROUP BY 1, 2, 3
      HAVING avg(status) < 1) y ON y.tags_id = t.id
WHERE t.name IS NOT NULL
GROUP BY t.fleet, t.model, y.day
ORDER BY y.day;

-- name: breakdown-frequency
-- Per model, how many times trucks broke down: ten-minute windows where most diagnostics report a fault after a window where they didn't.
WITH breakdown_per_truck_per_ten_minutes AS (
    SELECT @B10M(time) AS ten_minutes, tags_id,
           sum(CASE WHEN status = 0 THEN 1 ELSE 0 END) * 1.0 / count(*) >= 0.5 AS broken_down
    FROM diagnostics
    GROUP BY 1, 2
), breakdowns_per_truck AS (
    SELECT ten_minutes, tags_id, broken_down,
           lead(broken_down) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS next_broken_down
    FROM breakdown_per_truck_per_ten_minutes
)
SELECT t.model, count(*)
FROM tags t
JOIN breakdowns_per_truck b ON t.id = b.tags_id
WHERE t.name IS NOT NULL AND b.broken_down = false AND b.next_broken_down = true
GROUP BY t.model;

-- name: daily-activity [timelake tuned]
-- Grouped by the 10-minute bucket alone, which the rollup answers; each bucket lies in one day.
SELECT t.fleet, t.model, @B1D(y.ten_minutes) AS day, sum(y.ten_mins_per_day) / 144.0 AS daily_activity
FROM tags t
JOIN (SELECT @B10M(time) AS ten_minutes, tags_id, count(*) AS ten_mins_per_day
      FROM diagnostics
      GROUP BY 1, 2
      HAVING avg(status) < 1) y ON y.tags_id = t.id
WHERE t.name IS NOT NULL
GROUP BY t.fleet, t.model, @B1D(y.ten_minutes)
ORDER BY day;

-- name: long-driving-sessions [clickhouse tuned]
-- The 10-minute materialized view's states, merged.
SELECT t.name, t.driver
FROM tags t
JOIN (SELECT bucket AS ten_minutes, tags_id
      FROM readings_10m
      WHERE bucket >= @TS('2026-01-03 08:00:00') AND bucket < @TS('2026-01-03 12:00:00')
      GROUP BY bucket, tags_id
      HAVING sumMerge(velocity_sum) / countMerge(velocity_n) > 1) r ON t.id = r.tags_id
WHERE t.name IS NOT NULL AND t.fleet = 'South'
GROUP BY t.name, t.driver
HAVING count(r.ten_minutes) > 22;

-- name: long-daily-sessions [clickhouse tuned]
-- The 10-minute materialized view's states, merged.
SELECT t.name, t.driver
FROM tags t
JOIN (SELECT bucket AS ten_minutes, tags_id
      FROM readings_10m
      WHERE bucket >= @TS('2026-01-02 12:00:00') AND bucket < @TS('2026-01-03 12:00:00')
      GROUP BY bucket, tags_id
      HAVING sumMerge(velocity_sum) / countMerge(velocity_n) > 1) r ON t.id = r.tags_id
WHERE t.name IS NOT NULL AND t.fleet = 'South'
GROUP BY t.name, t.driver
HAVING count(r.ten_minutes) > 60;

-- name: avg-daily-driving-duration [clickhouse tuned]
-- The 10-minute materialized view's states, merged.
WITH ten_minute_driving_sessions AS (
    SELECT bucket AS ten_minutes, tags_id
    FROM readings_10m
    GROUP BY bucket, tags_id
    HAVING sumMerge(velocity_sum) / countMerge(velocity_n) > 1
), daily_total_session AS (
    SELECT @B1D(ten_minutes) AS day, tags_id, count(*) / 6.0 AS hours
    FROM ten_minute_driving_sessions
    GROUP BY 1, 2
)
SELECT t.fleet, t.name, t.driver, avg(d.hours) AS avg_daily_hours
FROM daily_total_session d
JOIN tags t ON t.id = d.tags_id
GROUP BY t.fleet, t.name, t.driver;

-- name: avg-daily-driving-session [clickhouse tuned]
-- The 10-minute materialized view's states, merged.
WITH driver_status AS (
    SELECT tags_id, bucket AS ten_minutes, sumMerge(velocity_sum) / countMerge(velocity_n) > 5 AS driving
    FROM readings_10m
    GROUP BY 1, 2
), driver_status_change AS (
    SELECT tags_id, ten_minutes AS start_ts, @EPOCH(ten_minutes) AS start_t,
           lead(@EPOCH(ten_minutes)) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS stop_t, driving
    FROM (SELECT tags_id, ten_minutes, driving,
                 lag(driving) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS prev_driving,
                 lag(@EPOCH(ten_minutes)) OVER (PARTITION BY tags_id ORDER BY ten_minutes) AS prev_t
          FROM driver_status) x
    WHERE x.prev_t > 0 AND x.driving <> x.prev_driving
)
SELECT t.name, @B1D(d.start_ts) AS day,
       avg(CASE WHEN d.stop_t > d.start_t THEN d.stop_t - d.start_t END) AS duration_s
FROM tags t
JOIN driver_status_change d ON t.id = d.tags_id
WHERE t.name IS NOT NULL AND d.driving = true
GROUP BY 1, 2
ORDER BY 1, 2;

-- name: avg-load [clickhouse tuned]
-- The 10-minute materialized view's states, merged over all time.
SELECT t.fleet, t.model, t.load_capacity, avg(d.avg_load / t.load_capacity) AS avg_load_percentage
FROM tags t
JOIN (SELECT tags_id, sumMerge(load_sum) / countMerge(load_n) AS avg_load FROM diagnostics_10m GROUP BY tags_id) d
  ON t.id = d.tags_id
WHERE t.name IS NOT NULL
GROUP BY t.fleet, t.model, t.load_capacity;

-- name: daily-activity [clickhouse tuned]
-- The 10-minute materialized view's states, merged; each bucket lies in one day.
SELECT t.fleet, t.model, y.day, sum(y.ten_mins_per_day) / 144.0 AS daily_activity
FROM tags t
JOIN (SELECT @B1D(bucket) AS day, bucket AS ten_minutes, tags_id, countMerge(n) AS ten_mins_per_day
      FROM diagnostics_10m
      GROUP BY 1, 2, 3
      HAVING sumMerge(status_sum) / countMerge(status_n) < 1) y ON y.tags_id = t.id
WHERE t.name IS NOT NULL
GROUP BY t.fleet, t.model, y.day
ORDER BY y.day;
