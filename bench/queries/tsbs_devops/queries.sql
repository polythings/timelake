-- TSBS DevOps (cpu-only) queries, ported from tsbs/cmd/tsbs_generate_queries/databases/timescaledb/devops.go
-- (TimescaleDB flavour, with `hostname` in the `cpu` table as TSBS's loader puts it: in-table partition tag).
--
-- Porting rules, so every engine runs the same logic:
--   * Random parameters are fixed: hosts below 100 (they exist at every scale), windows ending at
--     2026-01-03 23:00, the lengths TSBS uses (1 h or 12 h single-groupby, 8 h cpu-max-all, 12 h double-groupby
--     and high-cpu).
--   * `SELECT DISTINCT ON (hostname) ... ORDER BY hostname, time DESC` (lastpoint) becomes
--     ROW_NUMBER() over (hostname ORDER BY time DESC) = 1, which lake engines can execute.
--   * GROUP BY and ORDER BY use column positions; time buckets are placeholders filled per engine:
--       @B1M(x) 1-minute bucket, @B1H(x) 1-hour bucket, @TS('...') timestamptz literal.
--   * Names follow TSBS: single-groupby-<metrics>-<hosts>-<hours>, cpu-max-all-<hosts>, double-groupby-<metrics>.
--   * `-- name: <query> [<reader> tuned]` is the version a tuned run of that system reads (D23), as its docs write it
--     for its precomputation. Answers are checked like every other.

-- name: single-groupby-1-1-1
-- One host's highest user CPU per minute over an hour.
SELECT @B1M(time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ('host_9') AND time >= @TS('2026-01-03 22:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: single-groupby-1-1-12
-- One host's highest user CPU per minute over 12 hours.
SELECT @B1M(time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ('host_9') AND time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: single-groupby-1-8-1
-- The highest user CPU per minute across 8 hosts over an hour.
SELECT @B1M(time) AS minute, max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ('host_9', 'host_42', 'host_17', 'host_88', 'host_3', 'host_61', 'host_25', 'host_70') AND time >= @TS('2026-01-03 22:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: single-groupby-5-1-1
-- One host's highest value of 5 CPU metrics per minute over an hour.
SELECT @B1M(time) AS minute, max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ('host_9') AND time >= @TS('2026-01-03 22:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: single-groupby-5-1-12
-- One host's highest value of 5 CPU metrics per minute over 12 hours.
SELECT @B1M(time) AS minute, max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ('host_9') AND time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: single-groupby-5-8-1
-- The highest value of 5 CPU metrics per minute across 8 hosts over an hour.
SELECT @B1M(time) AS minute, max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait
FROM cpu
WHERE hostname IN ('host_9', 'host_42', 'host_17', 'host_88', 'host_3', 'host_61', 'host_25', 'host_70') AND time >= @TS('2026-01-03 22:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: cpu-max-all-1
-- One host's highest value of all 10 CPU metrics per hour over 8 hours.
SELECT @B1H(time) AS hour,
       max(usage_user) AS max_usage_user,
       max(usage_system) AS max_usage_system,
       max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice,
       max(usage_iowait) AS max_usage_iowait,
       max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq,
       max(usage_steal) AS max_usage_steal,
       max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ('host_9') AND time >= @TS('2026-01-03 15:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: cpu-max-all-8
-- The highest value of all 10 CPU metrics per hour across 8 hosts over 8 hours.
SELECT @B1H(time) AS hour,
       max(usage_user) AS max_usage_user,
       max(usage_system) AS max_usage_system,
       max(usage_idle) AS max_usage_idle,
       max(usage_nice) AS max_usage_nice,
       max(usage_iowait) AS max_usage_iowait,
       max(usage_irq) AS max_usage_irq,
       max(usage_softirq) AS max_usage_softirq,
       max(usage_steal) AS max_usage_steal,
       max(usage_guest) AS max_usage_guest,
       max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ('host_9', 'host_42', 'host_17', 'host_88', 'host_3', 'host_61', 'host_25', 'host_70') AND time >= @TS('2026-01-03 15:00:00') AND time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1;

-- name: double-groupby-1
-- Every host's average user CPU per hour over 12 hours.
WITH cpu_avg AS (
    SELECT @B1H(time) AS hour, hostname, avg(usage_user) AS mean_usage_user
    FROM cpu
    WHERE time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00')
    GROUP BY 1, 2
)
SELECT hour, hostname, mean_usage_user
FROM cpu_avg
ORDER BY hour, hostname;

-- name: double-groupby-5
-- Every host's average of 5 CPU metrics per hour over 12 hours.
WITH cpu_avg AS (
    SELECT @B1H(time) AS hour, hostname, avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle, avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait
    FROM cpu
    WHERE time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00')
    GROUP BY 1, 2
)
SELECT hour, hostname, mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait
FROM cpu_avg
ORDER BY hour, hostname;

-- name: double-groupby-all
-- Every host's average of all 10 CPU metrics per hour over 12 hours.
WITH cpu_avg AS (
    SELECT @B1H(time) AS hour, hostname, avg(usage_user) AS mean_usage_user, avg(usage_system) AS mean_usage_system, avg(usage_idle) AS mean_usage_idle, avg(usage_nice) AS mean_usage_nice, avg(usage_iowait) AS mean_usage_iowait, avg(usage_irq) AS mean_usage_irq, avg(usage_softirq) AS mean_usage_softirq, avg(usage_steal) AS mean_usage_steal, avg(usage_guest) AS mean_usage_guest, avg(usage_guest_nice) AS mean_usage_guest_nice
    FROM cpu
    WHERE time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00')
    GROUP BY 1, 2
)
SELECT hour, hostname, mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait, mean_usage_irq, mean_usage_softirq, mean_usage_steal, mean_usage_guest, mean_usage_guest_nice
FROM cpu_avg
ORDER BY hour, hostname;

-- name: high-cpu-all
-- Every reading over 12 hours where a host's user CPU was above 90%.
SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00');

-- name: high-cpu-1
-- One host's readings over 12 hours where its user CPU was above 90%.
SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= @TS('2026-01-03 11:00:00') AND time < @TS('2026-01-03 23:00:00') AND hostname IN ('host_9');

-- name: lastpoint
-- The latest reading of every host.
SELECT *
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY hostname ORDER BY time DESC) AS rn FROM cpu) AS c
WHERE rn = 1
ORDER BY hostname;

-- name: groupby-orderby-limit
-- The highest user CPU of the 5 most recent minutes before a time, across all hosts.
SELECT @B1M(time) AS minute, max(usage_user)
FROM cpu
WHERE time < @TS('2026-01-03 23:00:00')
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;

-- name: cpu-max-all-1 [clickhouse tuned]
-- The hourly materialized view's states, merged.
SELECT bucket AS hour,
       maxMerge(max_user) AS max_usage_user,
       maxMerge(max_system) AS max_usage_system,
       maxMerge(max_idle) AS max_usage_idle,
       maxMerge(max_nice) AS max_usage_nice,
       maxMerge(max_iowait) AS max_usage_iowait,
       maxMerge(max_irq) AS max_usage_irq,
       maxMerge(max_softirq) AS max_usage_softirq,
       maxMerge(max_steal) AS max_usage_steal,
       maxMerge(max_guest) AS max_usage_guest,
       maxMerge(max_guest_nice) AS max_usage_guest_nice
FROM cpu_1h
WHERE hostname IN ('host_9') AND bucket >= @TS('2026-01-03 15:00:00') AND bucket < @TS('2026-01-03 23:00:00')
GROUP BY bucket
ORDER BY hour;

-- name: cpu-max-all-8 [clickhouse tuned]
-- The hourly materialized view's states, merged.
SELECT bucket AS hour,
       maxMerge(max_user) AS max_usage_user,
       maxMerge(max_system) AS max_usage_system,
       maxMerge(max_idle) AS max_usage_idle,
       maxMerge(max_nice) AS max_usage_nice,
       maxMerge(max_iowait) AS max_usage_iowait,
       maxMerge(max_irq) AS max_usage_irq,
       maxMerge(max_softirq) AS max_usage_softirq,
       maxMerge(max_steal) AS max_usage_steal,
       maxMerge(max_guest) AS max_usage_guest,
       maxMerge(max_guest_nice) AS max_usage_guest_nice
FROM cpu_1h
WHERE hostname IN ('host_9', 'host_42', 'host_17', 'host_88', 'host_3', 'host_61', 'host_25', 'host_70') AND bucket >= @TS('2026-01-03 15:00:00') AND bucket < @TS('2026-01-03 23:00:00')
GROUP BY bucket
ORDER BY hour;

-- name: double-groupby-1 [clickhouse tuned]
-- The hourly materialized view's states, merged.
WITH cpu_avg AS (
    SELECT bucket AS hour, hostname, avgMerge(avg_user) AS mean_usage_user
    FROM cpu_1h
    WHERE bucket >= @TS('2026-01-03 11:00:00') AND bucket < @TS('2026-01-03 23:00:00')
    GROUP BY bucket, hostname
)
SELECT hour, hostname, mean_usage_user
FROM cpu_avg
ORDER BY hour, hostname;

-- name: double-groupby-5 [clickhouse tuned]
-- The hourly materialized view's states, merged.
WITH cpu_avg AS (
    SELECT bucket AS hour, hostname, avgMerge(avg_user) AS mean_usage_user, avgMerge(avg_system) AS mean_usage_system, avgMerge(avg_idle) AS mean_usage_idle, avgMerge(avg_nice) AS mean_usage_nice, avgMerge(avg_iowait) AS mean_usage_iowait
    FROM cpu_1h
    WHERE bucket >= @TS('2026-01-03 11:00:00') AND bucket < @TS('2026-01-03 23:00:00')
    GROUP BY bucket, hostname
)
SELECT hour, hostname, mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait
FROM cpu_avg
ORDER BY hour, hostname;

-- name: double-groupby-all [clickhouse tuned]
-- The hourly materialized view's states, merged.
WITH cpu_avg AS (
    SELECT bucket AS hour, hostname, avgMerge(avg_user) AS mean_usage_user, avgMerge(avg_system) AS mean_usage_system, avgMerge(avg_idle) AS mean_usage_idle, avgMerge(avg_nice) AS mean_usage_nice, avgMerge(avg_iowait) AS mean_usage_iowait, avgMerge(avg_irq) AS mean_usage_irq, avgMerge(avg_softirq) AS mean_usage_softirq, avgMerge(avg_steal) AS mean_usage_steal, avgMerge(avg_guest) AS mean_usage_guest, avgMerge(avg_guest_nice) AS mean_usage_guest_nice
    FROM cpu_1h
    WHERE bucket >= @TS('2026-01-03 11:00:00') AND bucket < @TS('2026-01-03 23:00:00')
    GROUP BY bucket, hostname
)
SELECT hour, hostname, mean_usage_user, mean_usage_system, mean_usage_idle, mean_usage_nice, mean_usage_iowait, mean_usage_irq, mean_usage_softirq, mean_usage_steal, mean_usage_guest, mean_usage_guest_nice
FROM cpu_avg
ORDER BY hour, hostname;
