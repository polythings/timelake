-- ClickHouse's tuned TSBS IoT tables (D23): 10-minute aggregates per truck as its docs build them, an
-- AggregatingMergeTree target filled by a materialized view on every insert (exact here: TSBS rows never repeat).
-- The tuned queries (`[clickhouse tuned]`) merge the states.
CREATE TABLE {database}.readings_10m (
    tags_id Int32, bucket DateTime64(6, 'UTC'),
    velocity_sum AggregateFunction(sum, Nullable(Float64)), velocity_n AggregateFunction(count, Nullable(Float64))
) ENGINE = AggregatingMergeTree PARTITION BY toYYYYMM(bucket) ORDER BY (tags_id, bucket);

CREATE MATERIALIZED VIEW {database}.readings_10m_mv TO {database}.readings_10m AS
SELECT tags_id, toStartOfInterval(time, INTERVAL 10 MINUTE) AS bucket,
       sumState(velocity) AS velocity_sum, countState(velocity) AS velocity_n
FROM {database}.readings GROUP BY tags_id, bucket;

CREATE TABLE {database}.diagnostics_10m (
    tags_id Int32, bucket DateTime64(6, 'UTC'),
    status_sum AggregateFunction(sum, Nullable(Float64)), status_n AggregateFunction(count, Nullable(Float64)),
    load_sum AggregateFunction(sum, Nullable(Float64)), load_n AggregateFunction(count, Nullable(Float64)), n AggregateFunction(count)
) ENGINE = AggregatingMergeTree PARTITION BY toYYYYMM(bucket) ORDER BY (tags_id, bucket);

CREATE MATERIALIZED VIEW {database}.diagnostics_10m_mv TO {database}.diagnostics_10m AS
SELECT tags_id, toStartOfInterval(time, INTERVAL 10 MINUTE) AS bucket,
       sumState(status) AS status_sum, countState(status) AS status_n,
       sumState(current_load) AS load_sum, countState(current_load) AS load_n, countState() AS n
FROM {database}.diagnostics GROUP BY tags_id, bucket;
