-- Industrial IoT queries (spec 45): what an operator, an engineer or a report asks of a plant historian.
--
-- Tables: `readings` (time, tag, value, quality), one row per sample, and `tags`, the asset model (tag, machine,
-- line, area, site, signal, kind, unit, rate_s, lo_limit, hi_limit, gateway). Names exist at every scale: machines
-- M00000-M00099, lines L0000-L0009, area A000.
--
-- Rules, so every system runs the same logic:
--   * Times are relative to the data's present: @AGO(15 minutes) is 15 minutes before it. Placeholders are filled
--     per engine: @B1H(x) and @B1D(x) buckets, @EPOCH(x) epoch seconds, @TS('...') a timestamp literal.
--   * Durations between samples use lead/lag over epoch seconds. Some engines return 0 instead of NULL past a
--     partition's edge, so the edge rows are dropped by comparing times (next_t >= t, prev_t > 0), not with IS NULL.
--   * Rows that arrive twice are not removed in the SQL: deduplicating them is the system's job. Answers are checked
--     against the reference system (systems/reference.py).
--   * A reader-specific version of a query is `-- name: <query> [<reader>]`, used only where its SQL lacks a construct.

-- name: latest-area
-- Dashboard: the latest value and quality of every tag in an area (1,000 tags).
SELECT r.tag, r.time, r.value, r.quality
FROM readings r
JOIN (
    SELECT r2.tag AS tag, max(r2.time) AS last
    FROM readings r2 JOIN tags t ON r2.tag = t.tag
    WHERE t.area = 'A000' AND r2.time >= @AGO(2 hours)
    GROUP BY r2.tag
) m ON r.tag = m.tag AND r.time = m.last
WHERE r.time >= @AGO(2 hours);

-- name: recent-machines
-- Dashboard: the last 15 minutes of two machines' tags, raw.
SELECT tag, time, value
FROM readings
WHERE (tag LIKE 'M00001.%' OR tag LIKE 'M00002.%') AND time >= @AGO(15 minutes)
ORDER BY tag, time;

-- name: trend-30d
-- Trend chart: one 1-second tag over 30 days, as hourly min, max and mean (720 points).
SELECT @B1H(time) AS hour, min(value) AS lo, max(value) AS hi, avg(value) AS mean, count(*) AS samples
FROM readings
WHERE tag = 'M00007.pressure' AND time >= @AGO(30 days)
GROUP BY @B1H(time)
ORDER BY hour;

-- name: alarm-window
-- Investigation: every tag of five machines, raw, 10 minutes either side of an alarm two hours ago.
SELECT r.tag, r.time, r.value, r.quality
FROM readings r JOIN tags t ON r.tag = t.tag
WHERE t.machine >= 'M00010' AND t.machine <= 'M00014'
  AND r.time >= @AGO(130 minutes) AND r.time < @AGO(110 minutes);

-- name: shift-report
-- Report: per machine of a line and analog signal, the hourly mean and peak over the last 8 hours (one shift).
SELECT t.machine, t.signal, @B1H(r.time) AS hour, avg(r.value) AS mean, max(r.value) AS peak
FROM readings r JOIN tags t ON r.tag = t.tag
WHERE t.line = 'L0003' AND t.kind = 'analog' AND r.time >= @AGO(8 hours)
GROUP BY t.machine, t.signal, @B1H(r.time);

-- name: daily-stats
-- Report: per tag of a line and day, the sample count and mean over 30 days.
SELECT r.tag, @B1D(r.time) AS day, count(*) AS samples, avg(r.value) AS mean
FROM readings r JOIN tags t ON r.tag = t.tag
WHERE t.line = 'L0002' AND r.time >= @AGO(30 days)
GROUP BY r.tag, @B1D(r.time);

-- name: time-above-limit
-- Process: per pressure tag in the area, seconds above its alarm limit in the last 24 hours.
SELECT tag, sum(CASE WHEN value > hi THEN next_t - t ELSE 0 END) AS seconds_above
FROM (
    SELECT r.tag AS tag, r.value AS value, t.hi_limit AS hi, @EPOCH(r.time) AS t,
           lead(@EPOCH(r.time)) OVER (PARTITION BY r.tag ORDER BY r.time) AS next_t
    FROM readings r JOIN tags t ON r.tag = t.tag
    WHERE t.area = 'A000' AND t.signal = 'pressure' AND r.time >= @AGO(24 hours)
) AS s
WHERE next_t >= t
GROUP BY tag;

-- name: time-weighted-avg
-- Process: per temperature tag of a line, the time-weighted mean over the last 24 hours (samples weighted by how
-- long they held).
SELECT tag, sum(value * (next_t - t)) / sum(next_t - t) AS twa
FROM (
    SELECT r.tag AS tag, r.value AS value, @EPOCH(r.time) AS t,
           lead(@EPOCH(r.time)) OVER (PARTITION BY r.tag ORDER BY r.time) AS next_t
    FROM readings r JOIN tags t ON r.tag = t.tag
    WHERE t.line = 'L0004' AND t.signal = 'temperature' AND r.time >= @AGO(24 hours)
) AS s
WHERE next_t >= t
GROUP BY tag;

-- name: uptime
-- OEE: per machine of a line, the share of the last 7 days it was running (an on-change state tag).
SELECT machine, sum(CASE WHEN value = 1 THEN next_t - t ELSE 0 END) / sum(next_t - t) AS running_share
FROM (
    SELECT t.machine AS machine, r.value AS value, @EPOCH(r.time) AS t,
           lead(@EPOCH(r.time)) OVER (PARTITION BY r.tag ORDER BY r.time) AS next_t
    FROM readings r JOIN tags t ON r.tag = t.tag
    WHERE t.line = 'L0000' AND t.signal = 'state' AND r.time >= @AGO(7 days)
) AS s
WHERE next_t >= t
GROUP BY machine;

-- name: energy
-- Energy: per machine in the area, kWh used in the last 24 hours, from cumulative counters that may reset.
SELECT machine, sum(CASE WHEN value >= prev THEN value - prev ELSE value END) AS kwh
FROM (
    SELECT t.machine AS machine, r.value AS value, @EPOCH(r.time) AS t,
           lag(r.value) OVER (PARTITION BY r.tag ORDER BY r.time) AS prev,
           lag(@EPOCH(r.time)) OVER (PARTITION BY r.tag ORDER BY r.time) AS prev_t
    FROM readings r JOIN tags t ON r.tag = t.tag
    WHERE t.area = 'A000' AND t.signal = 'energy' AND r.time >= @AGO(24 hours)
) AS s
WHERE prev_t > 0
GROUP BY machine;

-- name: gaps
-- Data quality: per 1-second pressure tag, gaps over 5 minutes in the last 24 hours (sensor or gateway down).
SELECT tag, count(*) AS gaps, sum(next_t - t) AS gap_seconds
FROM (
    SELECT tag, @EPOCH(time) AS t, lead(@EPOCH(time)) OVER (PARTITION BY tag ORDER BY time) AS next_t
    FROM readings
    WHERE tag LIKE '%.pressure' AND time >= @AGO(24 hours)
) AS s
WHERE next_t - t > 300
GROUP BY tag;
