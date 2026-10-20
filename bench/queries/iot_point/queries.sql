-- Single-device access patterns that TSBS IoT doesn't cover but dashboards and APIs hit constantly.
-- Same tables and placeholders as tsbs_iot. tags_id 1 = truck_0 (first tag set seen).

-- name: one-truck-last-hour
-- One truck's raw readings of the last hour.
SELECT time, latitude, longitude, velocity, fuel_consumption
FROM readings
WHERE tags_id = 1 AND time >= @TS('2026-01-03 23:00:00') AND time < @TS('2026-01-04 00:00:00')
ORDER BY time;

-- name: one-truck-all-history
-- One truck's every reading, in time order.
SELECT time, latitude, longitude, velocity, fuel_consumption
FROM readings
WHERE tags_id = 1
ORDER BY time;

-- name: one-truck-latest
-- One truck's latest position.
SELECT time, latitude, longitude
FROM readings
WHERE tags_id = 1
ORDER BY time DESC
LIMIT 1;

-- name: one-truck-hourly-avg
-- One truck's average speed and highest fuel consumption per hour over all its history.
SELECT @B1H(time) AS hour, avg(velocity) AS avg_velocity, max(fuel_consumption) AS max_fuel
FROM readings
WHERE tags_id = 1
GROUP BY 1
ORDER BY 1;

-- name: fleet-last-hour-avg
-- Every truck's average speed over the last hour.
SELECT r.tags_id, avg(r.velocity) AS avg_velocity
FROM readings r
WHERE r.time >= @TS('2026-01-03 23:00:00') AND r.time < @TS('2026-01-04 00:00:00')
GROUP BY r.tags_id;
