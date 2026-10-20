-- ClickHouse's tuned TSBS DevOps table (D23): an hourly aggregate per host of each CPU metric's maximum and mean, as
-- its docs build them: an AggregatingMergeTree target filled by a materialized view on every insert. The tuned
-- queries (`[clickhouse tuned]`) merge the states.
CREATE TABLE {database}.cpu_1h (
    hostname LowCardinality(String), bucket DateTime64(6, 'UTC'),
    max_user AggregateFunction(max, Nullable(Float64)), avg_user AggregateFunction(avg, Nullable(Float64)),
    max_system AggregateFunction(max, Nullable(Float64)), avg_system AggregateFunction(avg, Nullable(Float64)),
    max_idle AggregateFunction(max, Nullable(Float64)), avg_idle AggregateFunction(avg, Nullable(Float64)),
    max_nice AggregateFunction(max, Nullable(Float64)), avg_nice AggregateFunction(avg, Nullable(Float64)),
    max_iowait AggregateFunction(max, Nullable(Float64)), avg_iowait AggregateFunction(avg, Nullable(Float64)),
    max_irq AggregateFunction(max, Nullable(Float64)), avg_irq AggregateFunction(avg, Nullable(Float64)),
    max_softirq AggregateFunction(max, Nullable(Float64)), avg_softirq AggregateFunction(avg, Nullable(Float64)),
    max_steal AggregateFunction(max, Nullable(Float64)), avg_steal AggregateFunction(avg, Nullable(Float64)),
    max_guest AggregateFunction(max, Nullable(Float64)), avg_guest AggregateFunction(avg, Nullable(Float64)),
    max_guest_nice AggregateFunction(max, Nullable(Float64)), avg_guest_nice AggregateFunction(avg, Nullable(Float64))
) ENGINE = AggregatingMergeTree PARTITION BY toYYYYMM(bucket) ORDER BY (hostname, bucket);

CREATE MATERIALIZED VIEW {database}.cpu_1h_mv TO {database}.cpu_1h AS
SELECT hostname, toStartOfHour(time) AS bucket,
       maxState(usage_user) AS max_user, avgState(usage_user) AS avg_user,
       maxState(usage_system) AS max_system, avgState(usage_system) AS avg_system,
       maxState(usage_idle) AS max_idle, avgState(usage_idle) AS avg_idle,
       maxState(usage_nice) AS max_nice, avgState(usage_nice) AS avg_nice,
       maxState(usage_iowait) AS max_iowait, avgState(usage_iowait) AS avg_iowait,
       maxState(usage_irq) AS max_irq, avgState(usage_irq) AS avg_irq,
       maxState(usage_softirq) AS max_softirq, avgState(usage_softirq) AS avg_softirq,
       maxState(usage_steal) AS max_steal, avgState(usage_steal) AS avg_steal,
       maxState(usage_guest) AS max_guest, avgState(usage_guest) AS avg_guest,
       maxState(usage_guest_nice) AS max_guest_nice, avgState(usage_guest_nice) AS avg_guest_nice
FROM {database}.cpu GROUP BY hostname, bucket;
