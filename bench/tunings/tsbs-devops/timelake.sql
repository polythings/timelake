-- Timelake's tuned TSBS DevOps table (D23): an hourly rollup per host of each CPU metric's maximum, sum and count,
-- for the hourly reports (cpu-max-all, double-groupby). A 1-minute one measured no faster for the minute queries,
-- which read one host's hour already (2026-10-20), at 17 M rows: not kept.
CREATE MATERIALIZED ROLLUP cpu_1h AS
SELECT hostname, time_bucket(INTERVAL '1 hour', time) AS bucket,
       max(usage_user) AS max_user, sum(usage_user) AS sum_user, count(usage_user) AS count_user,
       max(usage_system) AS max_system, sum(usage_system) AS sum_system, count(usage_system) AS count_system,
       max(usage_idle) AS max_idle, sum(usage_idle) AS sum_idle, count(usage_idle) AS count_idle,
       max(usage_nice) AS max_nice, sum(usage_nice) AS sum_nice, count(usage_nice) AS count_nice,
       max(usage_iowait) AS max_iowait, sum(usage_iowait) AS sum_iowait, count(usage_iowait) AS count_iowait,
       max(usage_irq) AS max_irq, sum(usage_irq) AS sum_irq, count(usage_irq) AS count_irq,
       max(usage_softirq) AS max_softirq, sum(usage_softirq) AS sum_softirq, count(usage_softirq) AS count_softirq,
       max(usage_steal) AS max_steal, sum(usage_steal) AS sum_steal, count(usage_steal) AS count_steal,
       max(usage_guest) AS max_guest, sum(usage_guest) AS sum_guest, count(usage_guest) AS count_guest,
       max(usage_guest_nice) AS max_guest_nice, sum(usage_guest_nice) AS sum_guest_nice, count(usage_guest_nice) AS count_guest_nice
FROM cpu
GROUP BY hostname, bucket;
